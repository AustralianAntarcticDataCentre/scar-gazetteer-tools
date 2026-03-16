# scar-gazetteer-tools

## csv2sql
Creates SQL insert/update transactions based on provided CSV files. Applies basic formatting (trimming strings, etc), type casting, and some validation.

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
python csv2sql.py --file place_names.csv --output place_names_insert.sql
```
