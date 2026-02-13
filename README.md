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
