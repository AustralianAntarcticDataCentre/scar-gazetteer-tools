"""
Gazetteer Excel/CSV → SQL INSERT/UPDATE generator

Reads a source sheet (with x number of header rows (change in column header row)) of place names submitted,
normalizes and validates key fields, looks up supporting codes from the database, 
and writes an INSERT/UPDATE script for the 'gazetteer.place_names' table.

"""

import os
import logging
import argparse
from pathlib import Path
import pandas as pd
from psycopg2 import sql, connect
from dotenv import load_dotenv
import functools

load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
DB_NAME = os.getenv("DB_NAME")
SCHEMA  = os.getenv("DB_SCHEMA", "gazetteer")

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)

log = logging.getLogger(__name__)

@functools.cache
def db():
    return connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=DB_NAME,
        user=PG_USER,
        password=PG_PASS,
        options=f"-c search_path={SCHEMA}"
    )

# boolean conversion (converts 'truthy' strings to boolean)
def to_bool(v):
    if pd.isna(v):
        return False
    
    return str(v).casefold() in ("yes", "y", "true", "1", "t")

# converts dates to iso format
def to_date(v):
    if pd.isna(v):
        return None

    dt = pd.to_datetime(v)
    
    return dt.date().isoformat()

# clean strings - strips white space, empty strings to None
def clean_str(val):
    if pd.isna(val):
        return None

    return str(val).strip() or None

# cleans numeric fields as valid floats
def clean_float(val):
    if pd.isna(val):
        return None
    
    return float(val)

# validates lat/lngs
def validate_coords(lat, lon):
    if lat is None or lon is None:
        return False
    return -90 <= lat <= 90 and -180 <= lon <= 180

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

# loads feature type names and maps to codes from the db
@functools.cache
def get_feature_types():
    with db().cursor() as cur:
        cur.execute(sql.SQL("SELECT feature_type_name, feature_type_code FROM {schema}.feature_types").format(schema=sql.Identifier(SCHEMA)))
        feature_types = cur.fetchall()

    return {
        str(name).strip().casefold(): int(code)
        for name, code in feature_types
        if pd.notna(name) and pd.notna(code)
    }

# loads gazetteers and maps to country id
@functools.cache
def get_gazetteers():
    with db().cursor() as cur:
        cur.execute(sql.SQL("SELECT gazetteer_code, gazetteer_name, country_id FROM {schema}.gazetteers").format(schema=sql.Identifier(SCHEMA)))
        gazetteers = cur.fetchall()

    return {
        str(code).strip(): int(country_id)
        for code, name, country_id in gazetteers
        if pd.notna(code) and pd.notna(country_id)
    }

def validate_row(row, row_id, warn_missing=True):
    result = {}
    row_keys = row.keys()
    feature_types = get_feature_types()
    gazetteers = get_gazetteers()

    # place_name_gazetteer
    if "place_name_gazetteer" in row_keys:
        result["place_name_gazetteer"] = clean_str(row.get("place_name_gazetteer"))
    
    if not result.get("place_name_gazetteer") and warn_missing:
        log.warning("Row %s: missing 'place_name_gazetteer'", row_id)

    # place_name_mapping
    if "place_name_mapping" in row_keys:
        result["place_name_mapping"] = clean_str(row.get("place_name_mapping"))
    
    if not result.get("place_name_mapping") and warn_missing:
        log.warning("Row %s: missing 'place_name_mapping'", row_id)

    # place_id
    if "place_id" in row_keys:
        place_id = clean_str(row.get("place_id"))

        if place_id:
            try:
                result["place_id"] = int(place_id)
            except Exception:
                log.error("Row %s: string to int conversion error on 'place_id'", row_id)
                return None
        else:
            result["place_id"] = None

    # coordinates
    if "latitude" in row_keys and "longitude" in row_keys:
        try:
            lat = clean_float(row.get("latitude"))
            lon = clean_float(row.get("longitude"))
        except Exception:
            log.error("Row %s: string to float conversion error on 'latitude' or 'longitude'", row_id)
            return None

        if lat is not None or lon is not None:
            if validate_coords(lat, lon):
                result["geometry"] = sql.SQL("ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)").format(lon=sql.Literal(lon), lat=sql.Literal(lat))
            else:
                log.error("Row %s: invalid coordinates", row_id)
                return None
        else:
            result["geometry"] = None
    
    if not result.get('geometry') and warn_missing:
        log.warning("Row %s: missing 'geometry'", row_id)

    # feature_type_code
    # converts a feature type name to a feature type code
    if "feature_type_name" in row_keys:
        feature_type_name = (clean_str(row.get("feature_type_name")) or "").casefold()

        if feature_type_name:
            if feature_type_name in feature_types:
                result["feature_type_code"] = feature_types[feature_type_name]
            else:
                log.error("Row %s: feature type not found '%s'", row_id, feature_type_name)
                return None
        else:
            result["feature_type_code"] = None

    if not result.get('feature_type_code') and warn_missing:
        log.warning("Row %s: missing 'feature_type_code'", row_id)

    # gazetteer and country_id
    if "gazetteer" in row_keys:
        gazetteer = clean_str(row.get("gazetteer"))

        if gazetteer:
            if gazetteer in gazetteers:
                result["gazetteer"] = gazetteer
                result["country_id"] = gazetteers[gazetteer]
            else:
                log.error("Row %s: gazetteer not found '%s'", row_id, gazetteer)
                return None
        else:
            result["gazetteer"] = None
            result["country_id"] = None
    
    if not result.get('gazetteer') and warn_missing:
        log.warning("Row %s: missing 'gazetteer'", row_id)

    # altitude
    if "altitude" in row_keys:
        try:
            result["altitude"] = clean_float(row.get("altitude"))
        except Exception:
            log.error("Row %s: string to float conversion error in 'altitude'", row_id)
            return None

    # altitude_accuracy
    if "altitude_accuracy" in row_keys:
        try:
            result["altitude_accuracy"] = clean_float(row.get("altitude_accuracy"))
        except Exception:
            log.error("Row %s: string to float conversion error in 'altitude_accuracy'", row_id)
            return None

    # named_for
    if "named_for" in row_keys:
        result["named_for"] = clean_str(row.get("named_for"))

    # comments
    if "comments" in row_keys:
        result["comments"] = clean_str(row.get("comments"))

    # narrative
    if "narrative" in row_keys:
        result["narrative"] = clean_str(row.get("narrative"))
    
    # relic_flag
    if "is_relic" in row_keys:
        # Ignore if column is simply empty
        if clean_str(row.get("is_relic")):
            result["is_relic"] = to_bool(clean_str(row.get("is_relic")))

    # date_approved
    if "date_approved" in row_keys:
        try:
            result["date_approved"] = to_date(clean_str(row.get("date_approved")))
        except Exception:
            log.error("Row %s: string to date conversion error in 'date_approved'", row_id)
            return None

    return result

# Build insert SQL transaction
def build_insert(df, out_path):
    written, skipped = 0, 0

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"-- INSERT script for {SCHEMA}.place_names\n")
        f.write("BEGIN;\n")

        for idx, row in df.iterrows():
            validated = validate_row(row, idx + 1, True)

            if not validated:
                log.warning("Row %s skipped: nothing to insert", idx + 1)
                skipped += 1
                continue

            query = sql.SQL("INSERT INTO {schema}.place_names ({columns}) VALUE ({values})").format(
                schema=sql.Identifier(SCHEMA),
                columns=sql.SQL(', ').join(map(lambda col: sql.Identifier(col), validated.keys())),
                values=sql.SQL(', ').join(map(lambda val: val if isinstance(val, sql.Composed) else sql.Literal(val), validated.values()))
            )
            
            f.write(f"{query.as_string(db())};\n")

            written += 1

        f.write("COMMIT;\n")

    log.info("Rows inserted: %s", written)
    log.info("Rows skipped: %s", skipped)


# Build update SQL transaction
def build_update(df, out_path, nullify_blanks, id_col="name_id"):
    written, skipped = 0, 0

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"-- UPDATE script for {SCHEMA}.place_names\n")
        f.write("BEGIN;\n")

        for idx, row in df.iterrows():
            try:
                name_id = int(row.get(id_col))
            except Exception:
                name_id = None

            if not name_id:
                log.error("Row %s skipped: missing '%s'", idx + 1, id_col)
                skipped += 1
                continue

            validated = validate_row(row, idx + 1, False)

            # Avoid overwriting data by removing keys with null values, unless 
            # explicitly desired with the `--nullify-blanks` option.
            if validated and not nullify_blanks:
                validated = { k: v for k, v in validated.items() if v is not None }

            if not validated:
                log.warning("Row %s skipped: nothing to update", idx + 1)
                skipped += 1
                continue

            query = sql.SQL("UPDATE {schema}.place_names SET {data} WHERE id = {id}").format(
                schema=sql.Identifier(SCHEMA),
                data=sql.SQL(', ').join(
                    sql.Composed([sql.Identifier(col), sql.SQL(" = "), val if isinstance(val, sql.Composed) else sql.Literal(val)]) for col, val in validated.items()
                ),
                id=sql.Literal(name_id)
            )
            
            f.write(f"{query.as_string(db())};\n")

            written += 1

        f.write("COMMIT;\n")

    log.info("Rows updated: %s", written)
    log.info("Rows skipped: %s", skipped)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["insert", "update"], default="insert")
    parser.add_argument("--input", required=True)
    parser.add_argument("--sheet")
    parser.add_argument("--header-row", type=int, default=0)
    parser.add_argument("--output")
    parser.add_argument("--nullify-blanks", action="store_true")
    parser.add_argument("--target-id-col", default="name_id")
    args = parser.parse_args()

    path = Path(args.input)

    df = load_table(path, args.sheet, args.header_row)

    out_path = (
        Path(args.output)
        if args.output
        else path.with_suffix(f".{args.mode}.sql")
    )

    if args.mode == "insert":
        build_insert(df, out_path)
    else:
        build_update(
            df,
            out_path,
            args.nullify_blanks,
            args.target_id_col
        )

    db().close()

if __name__ == "__main__":
    main()