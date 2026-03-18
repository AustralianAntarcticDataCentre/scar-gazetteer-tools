# scar-gazetteer-tools
Collection of tools and templates to assist with administering the [SCAR Composite Gazetteer or Antarctica (CGA)](https://placenames.aq).

## csv2sql
Creates SQL files containing insert/update transactions based on provided CSV files. Applies basic formatting (trimming strings, etc), type casting, and validation (coordinates, originating gazetteer, feature type). Useful for preparing bulk inserts and updates to the SCAR CGA.

### Installation
```bash
# Create a new Python virtual environment
python -m venv .venv

# Activate the environment
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Usage
```bash
# Generate an insert transaction
python csv2sql.py --input place_names.csv --output place_names_insert.sql

# Generate an update transaction
python csv2sql.py --input place_names.csv --output place_names_update.sql --mode update

# Generate an update transaction, including clearing empty fields
python csv2sql.py --input place_names.csv --output place_names_update.sql --mode update --nullify-blanks

# Generate an update transaction, where the source CSV has the headers on row 1 (default is row 0)
python csv2sql.py --input place_names.csv --output place_names_update.sql --mode update --header-row 1
```
