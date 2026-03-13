# -*- coding: utf-8 -*-

"""
Gazetteer Excel/CSV → SQL INSERT/ Update generator

Reads a source sheet (with x number of header rows (chage in column header row)) of place names submitted by other nations,
normalizes and validates key fields, looks up supporting codes
from the database, and writes an INSERT script for the 'gazetteer.place_names' table.

"""

import os
import math
import logging
import argparse
from pathlib import Path
import pandas as pd
import psycopg2

# ------------------ CONFIG ------------------
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
DB_NAME = os.getenv("DB_NAME")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
SCHEMA  = os.getenv("DB_SCHEMA", "gazetteer")

# ------------------ LOGGING ------------------
#prints 
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)
log = logging.getLogger(__name__)

# ------------------ Database connect ------------------
def connect():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=DB_NAME,
        user=PG_USER,
        password=PG_PASS,
        options=f"-c search_path={SCHEMA}"
    )

# ------------------ Helpers ------------------
#converts python values to SQL-safe strings (escapes quotes by ' to '', none = NULL)
def sql_literal(val):
    if val is None:
        return "NULL"
    s = str(val)
    s = s.replace("'", "''")   # escape single quotes for SQL
    return f"'{s}'"

#boolean conversion (converts postive to true and else False)
def to_bool_yes(v):
    if v is None:
        return False
    return str(v).strip().casefold() in ("yes", "y", "true", "1", "t")

#converts numeric altitude to a float
def normalise_altitude(v):
    if v is None:
        return None
    try:
        f = float(v)
        if math.isnan(f):
            return None
        return f
    except Exception:
        return None

#converts dates to iso formats
def normalise_date(v):
    if v is None:
        return None
    dt = pd.to_datetime(v, errors="coerce")
    if pd.isna(dt):
        log.warning("Invalid date found: '%s' → setting NULL", v)
        return None
    return dt.date().isoformat()

#clean strings - strips white space, empty strings to None
def clean(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    return s if s else None    

#cleans numeric fields as valid floats
def clean_float(val):
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f):
            return None
        return f
    except Exception:
        return None

#valiates lat longs
def validate_coords(lat, lon):
    if lat is None or lon is None:
        return False
    return -90 <= lat <= 90 and -180 <= lon <= 180

# ------------------------------------------------
# FILE LOADING
# ------------------------------------------------

# load CSV or Excel then clean column names
def load_table(path: Path, sheet: str | None, header_row: int):
    if not path.exists():
        raise FileNotFoundError(path)
    suffix = path.suffix.lower()
    if suffix in (".csv", ".tsv"):
        sep = "\t" if suffix == ".tsv" else ","
        df = pd.read_csv(path, sep=sep, header=header_row)
    elif suffix in (".xlsx", ".xlsm", ".xls"):
        xls = pd.ExcelFile(path)
        if sheet is None: #dont have to put sheet in
            sheet = xls.sheet_names[0]
            log.info("Using first sheet: %s", sheet)

        df = xls.parse(sheet_name=sheet, header=header_row)
    else:
        raise ValueError("Unsupported file type")
    df.columns = [str(c).strip().casefold() for c in df.columns]
    return df

# ------------------------------------------------
# LOOKUPS
# ------------------------------------------------
#loads feautre type name and maps to code from the db
def load_feature_types(cnxn):

    df = pd.read_sql(
        f"SELECT feature_type_name, feature_type_code FROM {SCHEMA}.feature_types",
        cnxn
    )
    return {
        str(n).strip().casefold(): int(c)
        for n, c in zip(df.feature_type_name, df.feature_type_code)
        if pd.notna(n) and pd.notna(c)
    }

#loads gazetteers and maps to country id
def load_gazetteers(cnxn):

    df = pd.read_sql(
        f"SELECT gazetteer_code, gazetteer_name, country_id FROM {SCHEMA}.gazetteers",
        cnxn
    )

    code_to_country = {}

    for code, name, cid in zip(
        df.gazetteer_code,
        df.gazetteer_name,
        df.country_id
    ):
        if pd.notna(code) and pd.notna(cid):
            code_to_country[str(code).strip().casefold()] = int(cid)

    return code_to_country

# ------------------------------------------------
# INSERT GENERATION
# ------------------------------------------------
#loops through rows, validates required fields, normalises alititude, date, relic flag, narrative, comments, named_for. 
#Builds geometry for SQL, writes insert statements and escapes strings with sql literal
def build_insert(df, ft_map, gaz_map, out_path):

    written = 0
    skipped = 0

    with open(out_path, "w", encoding="utf-8") as f:

        f.write(f"-- INSERT script for {SCHEMA}.place_names\n")
        f.write("BEGIN;\n")

        for idx, row in df.iterrows():

            mapping = clean(row.get("place_name_mapping"))
            gazname = clean(row.get("place_name_gazetteer"))

            if not mapping or not gazname:
                log.warning("Row %s skipped: missing place name fields", idx + 1)
                skipped += 1
                continue

            lat = clean_float(row.get("latitude"))
            lon = clean_float(row.get("longitude"))

            if not validate_coords(lat, lon):
                log.warning("Row %s skipped: invalid coordinates (%s, %s)", idx + 1, lat, lon)
                skipped += 1
                continue

            ft = clean(row.get("feature_type_name"))
            key_ft = (ft or "").casefold()

            if key_ft in ft_map:
                feature_code = ft_map[key_ft]
            else:
                log.warning("Row %s warning: feature type not found '%s'", idx + 1, ft)
                feature_code = None
                continue

            

            gaz_code = clean(row.get("gazetteer"))
            key_gaz = (gaz_code or "").casefold()

            if key_gaz not in gaz_map:
                log.warning("Row %s skipped: gazetteer not found '%s'", idx + 1, gaz_code)
                skipped += 1
                continue

            country_id = gaz_map[key_gaz]

            altitude = normalise_altitude(row.get("altitude"))

            named_for = clean(row.get("named_for"))
            comments = clean(row.get("comments"))
            narrative = clean(row.get("narrative"))

            relic_flag = to_bool_yes(row.get("relic_flag"))

            date_named = normalise_date(row.get("date_approved"))

            geometry = f"ST_SetSRID(ST_MakePoint({lon},{lat}),4326)"

            columns = [
                "country_id",
                "place_name_mapping",
                "place_name_gazetteer",
                "altitude",
                "date_named",
                "feature_type_code",
                "gazetteer",
                "named_for",
                "comments",
                "narrative",
                "relic_flag",
                "geometry"
            ]

            values = [
                sql_literal(country_id),
                sql_literal(mapping),
                sql_literal(gazname),
                sql_literal(altitude),
                sql_literal(date_named),
                sql_literal(feature_code),
                sql_literal(gaz_code),
                sql_literal(named_for),
                sql_literal(comments),
                sql_literal(narrative),
                "TRUE" if relic_flag else "FALSE",
                geometry
            ]

            f.write(
                f"INSERT INTO {SCHEMA}.place_names "
                f"({', '.join(columns)}) "
                f"VALUES ({', '.join(values)});\n"
            )

            written += 1

        f.write("COMMIT;\n")

    log.info("Rows inserted: %s", written)
    log.info("Rows skipped: %s", skipped)


# ------------------------------------------------
# UPDATE GENERATION
# ------------------------------------------------

#checks the gaz_id, updates all fields, writes null for empty cells, writes Update...Set...where name_id =

def build_update(df, ft_map, out_path, nullify_blanks, id_col="name_id"):
    """
    Generate SQL UPDATE statements for all relevant columns.
    """
    written = 0
    skipped = 0
    no_feature = 0

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"-- UPDATE script for {SCHEMA}.place_names\n")
        f.write("BEGIN;\n")

        for idx, row in df.iterrows():
            # Get the target row ID
            gaz_id = clean(row.get("gaz_id"))
            if gaz_id is None:
                skipped += 1
                continue

            updates = {}

            # Text / string fields
            for col in [
                "place_name_mapping",
                "place_name_gazetteer",
                "comments",
                "named_for",
                "narrative",
                "source_name",
                "source_publisher",
                "source_identifier"
            ]:
                val = clean(row.get(col))
                if val is not None:
                    updates[col] = sql_literal(val)
                elif nullify_blanks:
                    updates[col] = "NULL"

            # Numeric / date fields
            altitude = normalise_altitude(clean_float(row.get("altitude")))
            if altitude is not None:
                updates["altitude"] = sql_literal(altitude)
            elif nullify_blanks:
                updates["altitude"] = "NULL"

            date_named = normalise_date(clean(row.get("date_approved")))
            if date_named is not None:
                updates["date_named"] = sql_literal(date_named)
            elif nullify_blanks:
                updates["date_named"] = "NULL"

            # Boolean field
            relic_flag_val = to_bool_yes(row.get("relic_flag"))
            if relic_flag_val is not None:
                updates["relic_flag"] = "TRUE" if relic_flag_val else "FALSE"
            elif nullify_blanks:
                updates["relic_flag"] = "NULL"

            # Feature type
            ft = clean(row.get("feature_type_name"))
            key_ft = (ft or "").casefold()
            if key_ft in ft_map:
                updates["feature_type_code"] = sql_literal(ft_map[key_ft])
            else :
                log.warning("Row %s: feature type not found '%s' → setting NULL", idx + 1, ft)
                updates["feature_type_code"] = "NULL"
                no_feature += 1

            # Geometry from latitude / longitude
            lat = clean_float(row.get("latitude"))
            lon = clean_float(row.get("longitude"))
            if lat is not None and lon is not None:
                updates["geometry"] = f"ST_SetSRID(ST_MakePoint({lon},{lat}),4326)"
            elif nullify_blanks:
                updates["geometry"] = "NULL"

            # Skip rows with nothing to update
            if not updates:
                log.warning("Row %s skipped: nothing to update", idx + 1)
                skipped += 1
                continue

            # Build the SET clause
            set_sql = ", ".join(f"{k} = {v}" for k, v in updates.items())

            # Write UPDATE statement
            f.write(
                f"UPDATE {SCHEMA}.place_names "
                f"SET {set_sql} "
                f"WHERE {id_col} = {sql_literal(int(float(gaz_id)))};\n"
            )

            written += 1

        f.write("COMMIT;\n")

    log.info("Rows updated: %s", written)
    log.info("Rows skipped: %s", skipped)
    if no_feature:
        log.info("Rows with missing feature type set to NULL: %s", no_feature)

# ------------------------------------------------
# MAIN
# ------------------------------------------------
#parses comadnline arguments.
#mode = insert or update, file path, optional sheet name, optional sql outputfile, numm blanks for update mode, column match ub DB
def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--mode", choices=["insert", "update"], default="insert")

    parser.add_argument("--file", required=True)

    parser.add_argument("--sheet")

    parser.add_argument("--header-row", type=int, default=0)

    parser.add_argument("--output")

    parser.add_argument("--nullify-blanks", action="store_true")

    parser.add_argument("--target-id-col", default="name_id")

    args = parser.parse_args()

    path = Path(args.file)

    df = load_table(path, args.sheet, args.header_row)

    out_path = (
        Path(args.output)
        if args.output
        else path.with_suffix(f".{args.mode}.sql")
    )

    cnxn = connect()

    ft_map = load_feature_types(cnxn)

    if args.mode == "insert":

        gaz_map = load_gazetteers(cnxn)

        build_insert(df, ft_map, gaz_map, out_path)

    else:

        build_update(
            df,
            ft_map,
            out_path,
            args.nullify_blanks,
            args.target_id_col
        )

    cnxn.close()


if __name__ == "__main__":
    main()