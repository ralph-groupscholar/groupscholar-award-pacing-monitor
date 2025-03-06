# Group Scholar Award Pacing Monitor

A Swift CLI for tracking scholarship award disbursement pacing against an annual budget. It summarizes monthly or quarterly spend, highlights variance from linear pacing, and projects future periods based on recent averages.

## Features
- Parses award CSVs (date, amount, category, cohort)
- Monthly or quarterly pacing views
- Variance vs expected linear spend
- Simple forward projection based on the last three periods
- Pacing alerts for periods outside the target range
- Missing period detection across the reporting window
- Largest period-over-period swing highlights
- Current-year snapshot with projected year-end burn vs budget
- Budget runway guidance on remaining-period targets vs recent pace
- Cumulative pacing view to see running variance
- Period award counts plus average/median award size
- Top category and cohort spend mix
- Optional date, category, and cohort filters to focus specific slices
- Optional JSON export for downstream reporting
- Optional database snapshot sync for reporting dashboards

## Usage

```sh
swift run groupscholar-award-pacing-monitor --file sample/awards.csv --budget 240000 --period month --projection-periods 4
```

```sh
swift run groupscholar-award-pacing-monitor --file sample/awards.csv --budget 240000 --period quarter --start-date 2025-01-01 --end-date 2025-12-31 --category Tuition,Stipend
```

```sh
swift run groupscholar-award-pacing-monitor --file sample/awards.csv --budget 240000 --period month --projection-periods 4 --export-json out/report.json
```

```sh
GS_DB_HOST=your_host GS_DB_PORT=5432 GS_DB_USER=your_user GS_DB_PASSWORD=your_password GS_DB_NAME=your_db \
swift run groupscholar-award-pacing-monitor --file sample/awards.csv --budget 240000 --period month --db-sync
```

### CSV format

```csv
date,amount,category,cohort
2025-09-15,12000,Tuition,Fall 2025
```

## Notes
- Dates must be `YYYY-MM-DD`.
- The annual budget is treated as evenly distributed across the selected period type.
- Database sync uses env vars `GS_DB_HOST`, `GS_DB_PORT`, `GS_DB_USER`, `GS_DB_PASSWORD`, `GS_DB_NAME`, and optional `GS_DB_SCHEMA`.
