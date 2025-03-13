# Group Scholar Award Pacing Monitor

A Swift CLI for tracking scholarship award disbursement pacing against an annual budget. It summarizes monthly or quarterly spend, highlights variance from linear pacing, and projects future periods based on recent averages.

## Features
- Parses award CSVs (date, amount, category, cohort)
- Monthly or quarterly pacing views
- Variance vs expected linear spend
- Simple forward projection based on the last three periods
- Pacing alerts for periods outside the target range
- Missing period detection across the reporting window
- Inactive period streak detection to highlight consecutive no-spend windows
- Largest period-over-period swing highlights
- Current-year snapshot with projected year-end burn vs budget
- Budget runway guidance on remaining-period targets vs recent pace
- Cumulative pacing view to see running variance
- Period award counts plus average/median award size
- Award size volatility (std dev and coefficient of variation)
- Award concentration risk (top award and top 5 share)
- Award cadence metrics (average/median gap between award days)
- Award size band distribution and top award highlights
- Period weights for seasonality-adjusted expectations and alerts
- Top category and cohort spend mix
- Optional category/cohort target mix variance tracking
- Optional seasonality-weighted expectations for non-linear pacing
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
swift run groupscholar-award-pacing-monitor --file sample/awards.csv --budget 240000 --period month --period-weights 12,8,7,7,8,9,9,8,8,7,8,9
```

```sh
swift run groupscholar-award-pacing-monitor --file sample/awards.csv --budget 240000 --period month --category-targets Tuition=70,Stipend=20,Travel=10 --cohort-targets "Fall 2025"=60,"Spring 2026"=40
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
- The annual budget is treated as evenly distributed across the selected period type unless `--period-weights` is supplied.
- Database sync uses env vars `GS_DB_HOST`, `GS_DB_PORT`, `GS_DB_USER`, `GS_DB_PASSWORD`, `GS_DB_NAME`, and optional `GS_DB_SCHEMA`.
