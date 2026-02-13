## About

This repository contains a reproducible experimental setup for ETL data validation in a PostgreSQL-based pipeline using football API payloads. The main goal is to compare practical trade-offs between validation approaches while keeping checks logically comparable across ETL stages.

The project is completed during the preparation of Maksim A. Metelkin's bachelor thesis at SPbPU Institute of Computer Science and Cybersecurity (SPbPU ICSC).

## Authors and contributors

- **Maksim A. Metelkin** — main author, Student at SPbPU ICSC
- **Vladimir A. Parkhomenko** — advisor and minor contributor, Senior Lecturer at SPbPU ICSC

## Data License and Terms of Use

This project uses football statistics obtained via the Football-Data.org API.

The data is provided by Football-Data.org and is subject to the Football-Data.org General Terms and Conditions (effective June 1, 2018):
https://www.football-data.org/

Use of the data requires compliance with the Service Provider's Terms & Conditions, including but not limited to:

- Registration and valid API key usage
- Fair Use Policy compliance
- Subscription tier limitations
- Attribution requirement:
  "Football data provided by the Football-Data.org API"
- Restrictions after subscription cancellation (no further referencing of obtained data)
- Intellectual property limitations regarding graphics (team logos, profile images, etc.)

All rights to the data remain with the respective rights holders and the Service Provider.

The MIT License included in this repository applies **only to the source code of this project** and does not apply to the football data obtained through the Football-Data.org API.

## Data

Football statistics of the top championships since 2023 from the resource https://football-data.org/

Inputs:

- `input/raw_football_api/<run_id>/manifest.json` — metadata of the input dataset.
- `input/raw_football_api/<run_id>/payloads/*.json` — local JSON payloads (competitions, areas, teams, scorers, matches, standings).

Outputs:

- `output/validation_summary_YYYYMMDD_HHMMSS.csv` — aggregated validation metrics by stages and tools (`E/T/L`, `gx/soda/dbt/sql`, `baseline/experiment`).
- Each new run creates a new timestamped CSV file; previous files are kept.

## Warranty

The contributors provide no warranty for the use of this software. Use it at your own risk.

## License

This project is open for use in educational purposes and is licensed under the MIT License. See `LICENSE`.

## Manual Run (`scripts/run_manual_experiments.py`)

Prerequisites:

- Python 3.12+.
- Docker Engine / Docker Desktop is running if you use `--start-temp-db`.
- Install dependencies:

```bash
pip install -r requirements.experiments.txt
```

Database connection defaults (used if `.env` is missing):

- `POSTGRES_DB=vkr_data`
- `POSTGRES_USER=admin`
- `POSTGRES_PASSWORD=pass`
- `POSTGRES_HOST=localhost`
- `POSTGRES_PORT=55432`

Run:

```bash
python scripts/run_manual_experiments.py [flags]
```

### Available flags

| Flag | Default | Description | When to use |
|---|---|---|---|
| `--input-dir` | `input/raw_football_api` | Root folder with exported input runs (`<run_id>` subfolders). | If your input root is not the default path. |
| `--input-run-dir` | auto (latest subfolder in `--input-dir`) | Explicit input run folder to process. | For reproducible runs on a fixed dataset. |
| `--tools-config` | `config/tools_experiment.yml` | YAML config for stage tools (`gx/soda/dbt/sql`) at `E/T/L`. | To switch between main/test tool profiles. |
| `--mutation-config` | `config/mutation_experiment.yml` | YAML config for mutation experiment iterations. | To switch between main/test mutation profiles. |
| `--output-dir` | `output` | Directory for exported summary CSV (`validation_summary_*.csv`). | If you want artifacts in a custom location. |
| `--logs-dir` | `logs` | Directory for runtime logs (`manual_experiment_*.log`, `db_export_*.log`). | If you want logs in a custom location. |
| `--start-temp-db` | `false` | Starts/resets temporary Postgres via Docker before run. | Recommended for isolated local runs. |
| `--keep-temp-db` | `false` | Keeps temporary DB container after run (instead of `down -v`). | If you want to inspect DB state after completion. |
| `--skip-mutations` | `false` | Skips mutation experiment block. | Fast smoke run focused on load + stage tools. |
| `--skip-tools` | `false` | Skips stage tools block and summary CSV export. | If you only need STG/DDS load and baseline refresh. |
| `--persist-mutation-report` | `false` | Saves mutation HTML report to `logs/mutation_reports`. | If you need persistent mutation report artifacts. |
| `--persist-tool-reports` | `false` | Saves tool reports to `logs/etl_stage_reports`. | If you need persistent per-tool report artifacts. |

Notes:

- `--keep-temp-db` has effect only with `--start-temp-db`.
- Without `--persist-mutation-report` and `--persist-tool-reports`, intermediate reports are written to temporary folders and removed after run.
- The script updates baseline run IDs inside files passed by `--tools-config` and `--mutation-config`.
