## About

This repository contains a reproducible experimental setup for ETL data validation in a PostgreSQL-based pipeline using football API payloads. The main goal is to compare practical trade-offs between validation approaches while keeping checks logically comparable across ETL stages.

## Authors and contributors

- **Maksim A. Metelkin** — main author, Student at SPbPU ICSC
- **Vladimir A. Parkhomenko** — advisor and minor contributor, Senior Lecturer at SPbPU ICSC

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
