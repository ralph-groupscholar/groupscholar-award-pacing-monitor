# Group Scholar Award Pacing Monitor Progress

## Iteration 1
- Bootstrapped a Swift CLI with CSV parsing, pacing summaries, and projection logic.
- Added a sample awards CSV and documented usage in the README.

## Iteration 2
- Added pacing alerts for out-of-range periods, plus top category and cohort mix breakdowns.
- Expanded the report output to surface the most material deviations and mixes.

## Iteration 3
- Added missing period detection across the reporting window.
- Added period-over-period swing highlights for the largest increases and decreases.

## Iteration 3
- Added optional date, category, and cohort filters to focus pacing analysis on specific slices.
- Included filter summaries in CLI output and updated README with new usage examples.

## Iteration 4
- Added cumulative pacing breakdowns to surface running variance against expected spend.
- Added JSON export for downstream reporting and updated CLI usage/README.

## Iteration 5
- Added budget runway guidance comparing required remaining-period averages vs recent pace.
- Included runway metrics in the JSON export and updated README feature list.

## Iteration 6
- Added award size statistics (average and median) plus per-period award counts/averages in the CLI and JSON export.
- Extended database sync schema and inserts to persist award size and count metrics.

## Iteration 6
- Added category/cohort target mix support with variance tracking in the CLI report and JSON export.
- Persisted target variance snapshots to the database and documented new usage examples.
