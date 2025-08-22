# Running

## Set up the virtual environment (optional)

```bash
# create a virtual environment
python3 -m venv .venv

# activate the virtual environment
source .venv/bin/activate

# or on Windows:
# .\.venv\Scripts\activate

# install dependencies
pip install -r requirements.txt
```

## Run the scripts

### Create Staging Database

Use default options of no filtering and reading CSV files from `sample_data` directory:

```bash
python3 nppes_to_staging.py 
```

Optionally specify input directory if different than sample_data and/or specify a state filter

```bash
python3 nppes_to_staging.py extra_data --states CA TX
```

### Convert Staged Data to JSON

Each type combined into a single NDJSON file:

```bash
python3 ndh_staging_to_json.py
```

Individual resource files into corresponding type directories:

```bash
python3 ndh_staging_to_json.py -i
```

### Upload Resources to FHIR Server

Process and upload all JSON and NDJSON files in the specified directory:

```bash
python3 ndjson_upload.py -u -s http://localhost:8080/fhir -d output
```

Upload resource(s) from an individual file (JSON file for a single resource, NDJSON for multiple).

```bash
python3 ndjson_upload.py -u -s http://localhost:8080/fhir -f <path_to_file>
```
