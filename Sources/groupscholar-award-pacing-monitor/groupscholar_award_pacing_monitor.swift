import Foundation

struct Config {
    let filePath: String
    let annualBudget: Double
    let period: PeriodType
    let projectionPeriods: Int
}

enum PeriodType: String {
    case month
    case quarter
}

struct Record {
    let year: Int
    let month: Int
    let day: Int
    let amount: Double
    let category: String
    let cohort: String
}

struct PeriodEntry: Hashable {
    let key: String
    let date: Date
    let year: Int
}

@main
struct groupscholar_award_pacing_monitor {
    static func main() {
        do {
            let config = try parseArgs(Array(CommandLine.arguments.dropFirst()))
            let records = try loadRecords(from: config.filePath)
            if records.isEmpty {
                print("No award records found.")
                return
            }
            let summary = buildSummary(records: records, config: config)
            printReport(summary: summary, config: config)
        } catch {
            fputs("Error: \(error)\n", stderr)
            printUsage()
            exit(1)
        }
    }
}

func parseArgs(_ args: [String]) throws -> Config {
    var filePath: String?
    var budget: Double?
    var period: PeriodType = .month
    var projectionPeriods = 0

    var index = 0
    while index < args.count {
        let arg = args[index]
        switch arg {
        case "--file":
            index += 1
            guard index < args.count else { throw ArgError.missingValue("--file") }
            filePath = args[index]
        case "--budget":
            index += 1
            guard index < args.count else { throw ArgError.missingValue("--budget") }
            guard let value = Double(args[index]) else { throw ArgError.invalidValue("--budget") }
            budget = value
        case "--period":
            index += 1
            guard index < args.count else { throw ArgError.missingValue("--period") }
            guard let selected = PeriodType(rawValue: args[index].lowercased()) else {
                throw ArgError.invalidValue("--period")
            }
            period = selected
        case "--projection-periods":
            index += 1
            guard index < args.count else { throw ArgError.missingValue("--projection-periods") }
            guard let value = Int(args[index]) else { throw ArgError.invalidValue("--projection-periods") }
            projectionPeriods = max(0, value)
        case "--help", "-h":
            printUsage()
            exit(0)
        default:
            throw ArgError.unknownFlag(arg)
        }
        index += 1
    }

    guard let filePathUnwrapped = filePath else { throw ArgError.missingRequired("--file") }
    guard let budgetUnwrapped = budget else { throw ArgError.missingRequired("--budget") }

    return Config(filePath: filePathUnwrapped, annualBudget: budgetUnwrapped, period: period, projectionPeriods: projectionPeriods)
}

enum ArgError: Error, CustomStringConvertible {
    case missingRequired(String)
    case missingValue(String)
    case invalidValue(String)
    case unknownFlag(String)

    var description: String {
        switch self {
        case .missingRequired(let flag):
            return "Missing required flag \(flag)"
        case .missingValue(let flag):
            return "Missing value for \(flag)"
        case .invalidValue(let flag):
            return "Invalid value for \(flag)"
        case .unknownFlag(let flag):
            return "Unknown flag \(flag)"
        }
    }
}

func printUsage() {
    let usage = """
    Group Scholar Award Pacing Monitor

    Usage:
      award-pacing --file <csv> --budget <annual_budget> [--period month|quarter] [--projection-periods N]

    Example:
      swift run award-pacing --file sample/awards.csv --budget 240000 --period month --projection-periods 4
    """
    print(usage)
}

func loadRecords(from path: String) throws -> [Record] {
    let url = URL(fileURLWithPath: path)
    let raw = try String(contentsOf: url, encoding: .utf8)
    let lines = raw.split(whereSeparator: \.isNewline).map(String.init)
    if lines.isEmpty { return [] }

    var records: [Record] = []
    for (index, line) in lines.enumerated() {
        if index == 0 && line.lowercased().contains("date") {
            continue
        }
        let fields = splitCSV(line)
        guard fields.count >= 2 else { continue }
        guard let date = parseDate(fields[0]) else { continue }
        guard let amount = Double(fields[1].trimmingCharacters(in: .whitespacesAndNewlines)) else { continue }
        let category = fields.count > 2 ? fields[2].trimmingCharacters(in: .whitespacesAndNewlines) : "Uncategorized"
        let cohort = fields.count > 3 ? fields[3].trimmingCharacters(in: .whitespacesAndNewlines) : "Unassigned"
        records.append(Record(year: date.year, month: date.month, day: date.day, amount: amount, category: category, cohort: cohort))
    }
    return records
}

func splitCSV(_ line: String) -> [String] {
    var fields: [String] = []
    var current = ""
    var inQuotes = false

    for char in line {
        if char == "\"" {
            inQuotes.toggle()
            continue
        }
        if char == "," && !inQuotes {
            fields.append(current)
            current = ""
        } else {
            current.append(char)
        }
    }
    fields.append(current)
    return fields
}

func parseDate(_ value: String) -> (year: Int, month: Int, day: Int)? {
    let parts = value.trimmingCharacters(in: .whitespacesAndNewlines).split(separator: "-")
    guard parts.count == 3,
          let year = Int(parts[0]),
          let month = Int(parts[1]),
          let day = Int(parts[2]) else {
        return nil
    }
    return (year, month, day)
}

struct Summary {
    let totalRecords: Int
    let totalAmount: Double
    let periodTotals: [String: Double]
    let periodEntries: [PeriodEntry]
    let expectedPerPeriod: Double
    let periodType: PeriodType
    let startDate: Date
    let endDate: Date
    let projection: [PeriodEntry: Double]
    let categoryTotals: [String: Double]
    let cohortTotals: [String: Double]
    let paceFlags: [PaceFlag]
    let yearTotals: [Int: Double]
    let yearPeriods: [Int: Set<String>]
}

struct PaceFlag {
    let period: String
    let actual: Double
    let expected: Double
    let variance: Double
    let pace: Double
}

func buildSummary(records: [Record], config: Config) -> Summary {
    let calendar = Calendar(identifier: .gregorian)
    var periodTotals: [String: Double] = [:]
    var entries: [String: PeriodEntry] = [:]
    var categoryTotals: [String: Double] = [:]
    var cohortTotals: [String: Double] = [:]
    var yearTotals: [Int: Double] = [:]
    var yearPeriods: [Int: Set<String>] = [:]
    var minDate = Date.distantFuture
    var maxDate = Date.distantPast

    for record in records {
        guard let date = calendar.date(from: DateComponents(year: record.year, month: record.month, day: record.day)) else { continue }
        minDate = min(minDate, date)
        maxDate = max(maxDate, date)

        let keyInfo = periodKey(for: record, period: config.period, calendar: calendar)
        periodTotals[keyInfo.key, default: 0] += record.amount
        entries[keyInfo.key] = keyInfo
        categoryTotals[record.category, default: 0] += record.amount
        cohortTotals[record.cohort, default: 0] += record.amount
        yearTotals[keyInfo.year, default: 0] += record.amount
        var yearSet = yearPeriods[keyInfo.year, default: Set<String>()]
        yearSet.insert(keyInfo.key)
        yearPeriods[keyInfo.year] = yearSet
    }

    let expectedPerPeriod: Double = config.period == .month ? config.annualBudget / 12.0 : config.annualBudget / 4.0

    let orderedEntries = entries.values.sorted { $0.date < $1.date }
    let projection = buildProjection(entries: orderedEntries, totals: periodTotals, config: config)
    let paceFlags = buildPaceFlags(entries: orderedEntries, totals: periodTotals, expectedPerPeriod: expectedPerPeriod)

    return Summary(
        totalRecords: records.count,
        totalAmount: periodTotals.values.reduce(0, +),
        periodTotals: periodTotals,
        periodEntries: orderedEntries,
        expectedPerPeriod: expectedPerPeriod,
        periodType: config.period,
        startDate: minDate,
        endDate: maxDate,
        projection: projection,
        categoryTotals: categoryTotals,
        cohortTotals: cohortTotals,
        paceFlags: paceFlags,
        yearTotals: yearTotals,
        yearPeriods: yearPeriods
    )
}

func periodKey(for record: Record, period: PeriodType, calendar: Calendar) -> PeriodEntry {
    switch period {
    case .month:
        let key = String(format: "%04d-%02d", record.year, record.month)
        let date = calendar.date(from: DateComponents(year: record.year, month: record.month, day: 1)) ?? Date()
        return PeriodEntry(key: key, date: date, year: record.year)
    case .quarter:
        let quarter = (record.month - 1) / 3 + 1
        let key = "\(record.year)-Q\(quarter)"
        let quarterMonth = (quarter - 1) * 3 + 1
        let date = calendar.date(from: DateComponents(year: record.year, month: quarterMonth, day: 1)) ?? Date()
        return PeriodEntry(key: key, date: date, year: record.year)
    }
}

func buildProjection(entries: [PeriodEntry], totals: [String: Double], config: Config) -> [PeriodEntry: Double] {
    guard config.projectionPeriods > 0 else { return [:] }
    let recentCount = min(3, entries.count)
    guard recentCount > 0 else { return [:] }
    let recentEntries = entries.suffix(recentCount)
    let recentTotal = recentEntries.reduce(0.0) { $0 + (totals[$1.key] ?? 0) }
    let average = recentTotal / Double(recentCount)

    guard let last = entries.last else { return [:] }
    var projected: [PeriodEntry: Double] = [:]
    var cursor = last
    for _ in 0..<config.projectionPeriods {
        cursor = nextPeriod(from: cursor, period: config.period)
        projected[cursor] = average
    }
    return projected
}

func nextPeriod(from entry: PeriodEntry, period: PeriodType) -> PeriodEntry {
    let calendar = Calendar(identifier: .gregorian)
    let monthIncrement = period == .month ? 1 : 3
    let nextDate = calendar.date(byAdding: .month, value: monthIncrement, to: entry.date) ?? entry.date
    let components = calendar.dateComponents([.year, .month], from: nextDate)
    let year = components.year ?? entry.year
    let month = components.month ?? 1

    let record = Record(year: year, month: month, day: 1, amount: 0, category: "", cohort: "")
    return periodKey(for: record, period: period, calendar: calendar)
}

func buildPaceFlags(entries: [PeriodEntry], totals: [String: Double], expectedPerPeriod: Double) -> [PaceFlag] {
    let lowerBound = 0.8
    let upperBound = 1.2
    var flags: [PaceFlag] = []

    for entry in entries {
        let actual = totals[entry.key] ?? 0
        let pace = expectedPerPeriod > 0 ? actual / expectedPerPeriod : 0
        if pace < lowerBound || pace > upperBound {
            let variance = actual - expectedPerPeriod
            flags.append(PaceFlag(period: entry.key, actual: actual, expected: expectedPerPeriod, variance: variance, pace: pace))
        }
    }

    return flags.sorted { abs($0.variance) > abs($1.variance) }
}

func printReport(summary: Summary, config: Config) {
    let formatter = DateFormatter()
    formatter.dateStyle = .medium

    let totalPeriods = summary.periodEntries.count
    let expectedTotal = summary.expectedPerPeriod * Double(totalPeriods)
    let variance = summary.totalAmount - expectedTotal

    print("Award Pacing Summary")
    print("Records: \(summary.totalRecords)")
    print("Date range: \(formatter.string(from: summary.startDate)) - \(formatter.string(from: summary.endDate))")
    print(String(format: "Total spent: $%.2f", summary.totalAmount))
    print(String(format: "Expected (%.0f periods): $%.2f", Double(totalPeriods), expectedTotal))
    print(String(format: "Variance: $%.2f", variance))
    print("")

    print("Period Breakdown")
    let header = String(format: "%-10@ | %-12@ | %-12@ | %-8@", "Period" as NSString, "Actual" as NSString, "Expected" as NSString, "Pace" as NSString)
    print(header)
    print(String(repeating: "-", count: 52))

    for entry in summary.periodEntries {
        let actual = summary.periodTotals[entry.key] ?? 0
        let pace = summary.expectedPerPeriod > 0 ? actual / summary.expectedPerPeriod : 0
        let row = String(format: "%-10@ | $%-11.2f | $%-11.2f | %-7.0f%%", entry.key as NSString, actual, summary.expectedPerPeriod, pace * 100)
        print(row)
    }

    if !summary.projection.isEmpty {
        print("")
        print("Projection (Avg of last 3 periods)")
        for entry in summary.projection.keys.sorted(by: { $0.date < $1.date }) {
            if let amount = summary.projection[entry] {
                let row = String(format: "%-10@ | $%-11.2f", entry.key as NSString, amount)
                print(row)
            }
        }
    }

    if !summary.paceFlags.isEmpty {
        print("")
        print("Pacing Alerts (outside 80%-120%)")
        let header = String(format: "%-10@ | %-12@ | %-12@ | %-8@", "Period" as NSString, "Actual" as NSString, "Expected" as NSString, "Pace" as NSString)
        print(header)
        print(String(repeating: "-", count: 52))
        for flag in summary.paceFlags.prefix(6) {
            let row = String(format: "%-10@ | $%-11.2f | $%-11.2f | %-7.0f%%", flag.period as NSString, flag.actual, flag.expected, flag.pace * 100)
            print(row)
        }
    }

    if let latestYear = summary.yearTotals.keys.max(),
       let yearTotal = summary.yearTotals[latestYear],
       let periodCount = summary.yearPeriods[latestYear]?.count {
        let periodsPerYear = summary.periodType == .month ? 12 : 4
        let expectedToDate = summary.expectedPerPeriod * Double(periodCount)
        let paceToDate = expectedToDate > 0 ? yearTotal / expectedToDate : 0
        let averagePerPeriod = periodCount > 0 ? yearTotal / Double(periodCount) : 0
        let projectedYearEnd = averagePerPeriod * Double(periodsPerYear)
        let varianceVsBudget = projectedYearEnd - config.annualBudget
        let remainingBudget = config.annualBudget - yearTotal

        print("")
        print("Current Year Snapshot (\(latestYear))")
        print(String(format: "Periods reported: %d of %d", periodCount, periodsPerYear))
        print(String(format: "Year-to-date spend: $%.2f", yearTotal))
        print(String(format: "Expected to date: $%.2f (%.0f%% pace)", expectedToDate, paceToDate * 100))
        print(String(format: "Projected year-end: $%.2f", projectedYearEnd))
        print(String(format: "Projected vs budget: $%.2f", varianceVsBudget))
        print(String(format: "Remaining budget: $%.2f", remainingBudget))
    }

    let topCategory = topBreakdown(title: "Top Categories", totals: summary.categoryTotals, totalAmount: summary.totalAmount)
    if !topCategory.isEmpty {
        print("")
        print(topCategory)
    }

    let topCohort = topBreakdown(title: "Top Cohorts", totals: summary.cohortTotals, totalAmount: summary.totalAmount)
    if !topCohort.isEmpty {
        print("")
        print(topCohort)
    }
}

func topBreakdown(title: String, totals: [String: Double], totalAmount: Double) -> String {
    guard totalAmount > 0, !totals.isEmpty else { return "" }
    let sorted = totals.sorted { $0.value > $1.value }
    var lines: [String] = [title]
    for (index, entry) in sorted.prefix(5).enumerated() {
        let share = (entry.value / totalAmount) * 100
        lines.append(String(format: "%2d. %-20@ $%-10.2f (%.1f%%)", index + 1, entry.key as NSString, entry.value, share))
    }
    return lines.joined(separator: "\n")
}
