import Foundation
import NIOPosix
import PostgresNIO

struct Config {
    let filePath: String
    let annualBudget: Double
    let period: PeriodType
    let projectionPeriods: Int
    let startDate: Date?
    let endDate: Date?
    let categoryFilters: [String]
    let cohortFilters: [String]
    let categoryTargets: [TargetConfig]
    let cohortTargets: [TargetConfig]
    let exportPath: String?
    let dbSync: Bool
    let dbSchema: String?
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

struct TargetConfig {
    let name: String
    let share: Double
    let normalized: String
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
            let filtered = applyFilters(records: records, config: config)
            if filtered.isEmpty {
                print("No award records found after applying filters.")
                return
            }
            let summary = buildSummary(records: filtered, config: config)
            printReport(summary: summary, config: config)
            if let exportPath = config.exportPath {
                try exportReport(summary: summary, config: config, to: exportPath)
                print("")
                print("Exported JSON report to \(exportPath)")
            }
            if config.dbSync {
                try syncToDatabase(summary: summary, config: config)
                print("")
                print("Synced report snapshot to database.")
            }
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
    var startDate: Date?
    var endDate: Date?
    var categoryFilters: [String] = []
    var cohortFilters: [String] = []
    var categoryTargets: [TargetConfig] = []
    var cohortTargets: [TargetConfig] = []
    var exportPath: String?
    var dbSync = false
    var dbSchema: String?

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
        case "--start-date":
            index += 1
            guard index < args.count else { throw ArgError.missingValue("--start-date") }
            guard let parsed = parseDate(args[index]) else { throw ArgError.invalidValue("--start-date") }
            startDate = buildDate(year: parsed.year, month: parsed.month, day: parsed.day)
        case "--end-date":
            index += 1
            guard index < args.count else { throw ArgError.missingValue("--end-date") }
            guard let parsed = parseDate(args[index]) else { throw ArgError.invalidValue("--end-date") }
            endDate = buildDate(year: parsed.year, month: parsed.month, day: parsed.day)
        case "--category":
            index += 1
            guard index < args.count else { throw ArgError.missingValue("--category") }
            categoryFilters = parseFilterList(args[index])
        case "--cohort":
            index += 1
            guard index < args.count else { throw ArgError.missingValue("--cohort") }
            cohortFilters = parseFilterList(args[index])
        case "--category-targets":
            index += 1
            guard index < args.count else { throw ArgError.missingValue("--category-targets") }
            categoryTargets = try parseTargetList(args[index], flag: "--category-targets")
        case "--cohort-targets":
            index += 1
            guard index < args.count else { throw ArgError.missingValue("--cohort-targets") }
            cohortTargets = try parseTargetList(args[index], flag: "--cohort-targets")
        case "--export-json":
            index += 1
            guard index < args.count else { throw ArgError.missingValue("--export-json") }
            exportPath = args[index]
        case "--db-sync":
            dbSync = true
        case "--db-schema":
            index += 1
            guard index < args.count else { throw ArgError.missingValue("--db-schema") }
            dbSchema = args[index]
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

    return Config(
        filePath: filePathUnwrapped,
        annualBudget: budgetUnwrapped,
        period: period,
        projectionPeriods: projectionPeriods,
        startDate: startDate,
        endDate: endDate,
        categoryFilters: categoryFilters,
        cohortFilters: cohortFilters,
        categoryTargets: categoryTargets,
        cohortTargets: cohortTargets,
        exportPath: exportPath,
        dbSync: dbSync,
        dbSchema: dbSchema
    )
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

enum DbError: Error, CustomStringConvertible {
    case missingEnv(String)
    case invalidEnv(String)

    var description: String {
        switch self {
        case .missingEnv(let name):
            return "Missing required environment variable \(name)"
        case .invalidEnv(let name):
            return "Invalid environment variable value for \(name)"
        }
    }
}

func printUsage() {
    let usage = """
    Group Scholar Award Pacing Monitor

    Usage:
      groupscholar-award-pacing-monitor --file <csv> --budget <annual_budget> [--period month|quarter] [--projection-periods N]
                   [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--category list] [--cohort list]
                   [--category-targets list] [--cohort-targets list] [--export-json path] [--db-sync] [--db-schema name]

    Example:
      swift run groupscholar-award-pacing-monitor --file sample/awards.csv --budget 240000 --period month --projection-periods 4 --start-date 2025-01-01 --category Tuition,Stipend --export-json out/report.json

    Database sync env vars (required when --db-sync is set):
      GS_DB_HOST, GS_DB_PORT, GS_DB_USER, GS_DB_PASSWORD, GS_DB_NAME
      GS_DB_SCHEMA (optional, defaults to award_pacing_monitor or --db-schema)
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

func buildDate(year: Int, month: Int, day: Int) -> Date? {
    let calendar = Calendar(identifier: .gregorian)
    return calendar.date(from: DateComponents(year: year, month: month, day: day))
}

func parseFilterList(_ raw: String) -> [String] {
    raw.split(separator: ",")
        .map { $0.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() }
        .filter { !$0.isEmpty }
}

func parseTargetList(_ raw: String, flag: String) throws -> [TargetConfig] {
    let entries = raw.split(separator: ",").map { String($0) }
    var results: [TargetConfig] = []
    var totalShare = 0.0

    for entry in entries {
        let parts = entry.split(separator: "=", maxSplits: 1).map { String($0) }
        guard parts.count == 2 else { throw ArgError.invalidValue(flag) }
        let name = parts[0].trimmingCharacters(in: .whitespacesAndNewlines)
        let rawValue = parts[1].trimmingCharacters(in: .whitespacesAndNewlines)
        guard !name.isEmpty, let parsed = Double(rawValue) else { throw ArgError.invalidValue(flag) }
        let share = parsed > 1.0 ? parsed / 100.0 : parsed
        guard share >= 0 else { throw ArgError.invalidValue(flag) }
        totalShare += share
        results.append(TargetConfig(name: name, share: share, normalized: name.lowercased()))
    }

    if totalShare > 1.001 {
        throw ArgError.invalidValue(flag)
    }

    return results
}

func recordDate(_ record: Record) -> Date? {
    buildDate(year: record.year, month: record.month, day: record.day)
}

func applyFilters(records: [Record], config: Config) -> [Record] {
    let normalizedCategoryFilters = Set(config.categoryFilters)
    let normalizedCohortFilters = Set(config.cohortFilters)

    return records.filter { record in
        guard let date = recordDate(record) else { return false }
        if let start = config.startDate, date < start { return false }
        if let end = config.endDate, date > end { return false }
        if !normalizedCategoryFilters.isEmpty {
            let category = record.category.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            if !normalizedCategoryFilters.contains(category) { return false }
        }
        if !normalizedCohortFilters.isEmpty {
            let cohort = record.cohort.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            if !normalizedCohortFilters.contains(cohort) { return false }
        }
        return true
    }
}

struct Summary {
    let totalRecords: Int
    let totalAmount: Double
    let averageAward: Double
    let medianAward: Double
    let periodTotals: [String: Double]
    let periodCounts: [String: Int]
    let periodEntries: [PeriodEntry]
    let missingPeriods: [PeriodEntry]
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
    let periodDeltas: [PeriodDelta]
}

struct PaceFlag {
    let period: String
    let actual: Double
    let expected: Double
    let variance: Double
    let pace: Double
}

struct PeriodDelta {
    let from: PeriodEntry
    let to: PeriodEntry
    let delta: Double
    let percent: Double?
}

struct CumulativeEntry {
    let entry: PeriodEntry
    let actual: Double
    let expected: Double
    let variance: Double
}

struct ExportPayload: Encodable {
    let generatedAt: String
    let periodType: String
    let annualBudget: Double
    let dateRange: ExportDateRange
    let filters: ExportFilters
    let totals: ExportTotals
    let periods: [ExportPeriod]
    let missingPeriods: [String]
    let paceAlerts: [ExportPaceAlert]
    let projection: [ExportProjection]
    let topCategories: [ExportBreakdown]
    let topCohorts: [ExportBreakdown]
    let categoryTargets: [ExportTargetVariance]?
    let cohortTargets: [ExportTargetVariance]?
    let runway: ExportRunway?
}

struct ExportDateRange: Encodable {
    let start: String
    let end: String
}

struct ExportFilters: Encodable {
    let startDate: String?
    let endDate: String?
    let categories: [String]
    let cohorts: [String]
}

struct ExportTotals: Encodable {
    let records: Int
    let actual: Double
    let expected: Double
    let variance: Double
    let averageAward: Double
    let medianAward: Double
}

struct ExportPeriod: Encodable {
    let key: String
    let actual: Double
    let expected: Double
    let pace: Double
    let recordCount: Int
    let averageAward: Double
    let cumulativeActual: Double
    let cumulativeExpected: Double
    let cumulativeVariance: Double
}

struct ExportPaceAlert: Encodable {
    let period: String
    let actual: Double
    let expected: Double
    let variance: Double
    let pace: Double
}

struct ExportProjection: Encodable {
    let period: String
    let amount: Double
}

struct ExportBreakdown: Encodable {
    let name: String
    let amount: Double
    let share: Double
}

struct ExportTargetVariance: Encodable {
    let name: String
    let targetShare: Double
    let actualAmount: Double
    let expectedAmount: Double
    let variance: Double
    let actualShare: Double
}

struct Runway {
    let year: Int
    let periodsReported: Int
    let periodsPerYear: Int
    let remainingPeriods: Int
    let yearToDate: Double
    let remainingBudget: Double
    let requiredAverage: Double?
    let recentAverage: Double?
    let recentPeriods: Int
    let deltaVsRecent: Double?
}

struct ExportRunway: Encodable {
    let year: Int
    let periodsReported: Int
    let periodsPerYear: Int
    let remainingPeriods: Int
    let yearToDate: Double
    let remainingBudget: Double
    let requiredAverage: Double?
    let recentAverage: Double?
    let recentPeriods: Int
    let deltaVsRecent: Double?
}

struct TargetVariance {
    let name: String
    let targetShare: Double
    let actualAmount: Double
    let expectedAmount: Double
    let variance: Double
    let actualShare: Double
}

func buildSummary(records: [Record], config: Config) -> Summary {
    let calendar = Calendar(identifier: .gregorian)
    var periodTotals: [String: Double] = [:]
    var periodCounts: [String: Int] = [:]
    var entries: [String: PeriodEntry] = [:]
    var categoryTotals: [String: Double] = [:]
    var cohortTotals: [String: Double] = [:]
    var yearTotals: [Int: Double] = [:]
    var yearPeriods: [Int: Set<String>] = [:]
    var minDate = Date.distantFuture
    var maxDate = Date.distantPast
    var amounts: [Double] = []

    for record in records {
        guard let date = calendar.date(from: DateComponents(year: record.year, month: record.month, day: record.day)) else { continue }
        minDate = min(minDate, date)
        maxDate = max(maxDate, date)

        let keyInfo = periodKey(for: record, period: config.period, calendar: calendar)
        periodTotals[keyInfo.key, default: 0] += record.amount
        periodCounts[keyInfo.key, default: 0] += 1
        entries[keyInfo.key] = keyInfo
        categoryTotals[record.category, default: 0] += record.amount
        cohortTotals[record.cohort, default: 0] += record.amount
        yearTotals[keyInfo.year, default: 0] += record.amount
        var yearSet = yearPeriods[keyInfo.year, default: Set<String>()]
        yearSet.insert(keyInfo.key)
        yearPeriods[keyInfo.year] = yearSet
        amounts.append(record.amount)
    }

    let expectedPerPeriod: Double = config.period == .month ? config.annualBudget / 12.0 : config.annualBudget / 4.0

    let orderedEntries = entries.values.sorted { $0.date < $1.date }
    let missingPeriods = buildMissingPeriods(entries: orderedEntries, totals: periodTotals, period: config.period)
    let periodDeltas = buildPeriodDeltas(entries: orderedEntries, totals: periodTotals)
    let projection = buildProjection(entries: orderedEntries, totals: periodTotals, config: config)
    let paceFlags = buildPaceFlags(entries: orderedEntries, totals: periodTotals, expectedPerPeriod: expectedPerPeriod)
    let totalAmount = periodTotals.values.reduce(0, +)
    let averageAward = records.isEmpty ? 0 : totalAmount / Double(records.count)
    let medianAward = computeMedian(values: amounts)

    return Summary(
        totalRecords: records.count,
        totalAmount: totalAmount,
        averageAward: averageAward,
        medianAward: medianAward,
        periodTotals: periodTotals,
        periodCounts: periodCounts,
        periodEntries: orderedEntries,
        missingPeriods: missingPeriods,
        expectedPerPeriod: expectedPerPeriod,
        periodType: config.period,
        startDate: minDate,
        endDate: maxDate,
        projection: projection,
        categoryTotals: categoryTotals,
        cohortTotals: cohortTotals,
        paceFlags: paceFlags,
        yearTotals: yearTotals,
        yearPeriods: yearPeriods,
        periodDeltas: periodDeltas
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

func buildRunway(summary: Summary, config: Config) -> Runway? {
    guard let latestYear = summary.yearTotals.keys.max(),
          let yearTotal = summary.yearTotals[latestYear] else {
        return nil
    }

    let periodsPerYear = summary.periodType == .month ? 12 : 4
    let periodCount = summary.yearPeriods[latestYear]?.count ?? 0
    let remainingPeriods = max(0, periodsPerYear - periodCount)
    let remainingBudget = config.annualBudget - yearTotal
    let requiredAverage = remainingPeriods > 0 ? remainingBudget / Double(remainingPeriods) : nil

    let yearEntries = summary.periodEntries.filter { $0.year == latestYear }
    let recentCount = min(3, yearEntries.count)
    let recentAverage: Double?
    if recentCount > 0 {
        let recentEntries = yearEntries.suffix(recentCount)
        let recentTotal = recentEntries.reduce(0.0) { $0 + (summary.periodTotals[$1.key] ?? 0) }
        recentAverage = recentTotal / Double(recentCount)
    } else {
        recentAverage = nil
    }

    let deltaVsRecent: Double?
    if let required = requiredAverage, let recent = recentAverage {
        deltaVsRecent = required - recent
    } else {
        deltaVsRecent = nil
    }

    return Runway(
        year: latestYear,
        periodsReported: periodCount,
        periodsPerYear: periodsPerYear,
        remainingPeriods: remainingPeriods,
        yearToDate: yearTotal,
        remainingBudget: remainingBudget,
        requiredAverage: requiredAverage,
        recentAverage: recentAverage,
        recentPeriods: recentCount,
        deltaVsRecent: deltaVsRecent
    )
}

func buildMissingPeriods(entries: [PeriodEntry], totals: [String: Double], period: PeriodType) -> [PeriodEntry] {
    guard let first = entries.first, let last = entries.last else { return [] }
    var missing: [PeriodEntry] = []
    var cursor = first
    while cursor.date <= last.date {
        if totals[cursor.key] == nil {
            missing.append(cursor)
        }
        cursor = nextPeriod(from: cursor, period: period)
    }
    return missing
}

func buildPeriodDeltas(entries: [PeriodEntry], totals: [String: Double]) -> [PeriodDelta] {
    guard entries.count > 1 else { return [] }
    var deltas: [PeriodDelta] = []
    for index in 1..<entries.count {
        let previous = entries[index - 1]
        let current = entries[index]
        let previousAmount = totals[previous.key] ?? 0
        let currentAmount = totals[current.key] ?? 0
        let delta = currentAmount - previousAmount
        let percent = previousAmount != 0 ? delta / previousAmount : nil
        deltas.append(PeriodDelta(from: previous, to: current, delta: delta, percent: percent))
    }
    return deltas
}

func buildCumulative(entries: [PeriodEntry], totals: [String: Double], expectedPerPeriod: Double) -> [CumulativeEntry] {
    var cumulativeActual = 0.0
    var results: [CumulativeEntry] = []

    for entry in entries {
        let actual = totals[entry.key] ?? 0
        cumulativeActual += actual
        let cumulativeExpected = expectedPerPeriod * Double(results.count + 1)
        let variance = cumulativeActual - cumulativeExpected
        results.append(CumulativeEntry(entry: entry, actual: cumulativeActual, expected: cumulativeExpected, variance: variance))
    }

    return results
}

func computeMedian(values: [Double]) -> Double {
    guard !values.isEmpty else { return 0 }
    let sorted = values.sorted()
    let mid = sorted.count / 2
    if sorted.count % 2 == 0 {
        return (sorted[mid - 1] + sorted[mid]) / 2
    }
    return sorted[mid]
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

func exportReport(summary: Summary, config: Config, to path: String) throws {
    let formatter = DateFormatter()
    formatter.dateFormat = "yyyy-MM-dd"
    formatter.timeZone = TimeZone(secondsFromGMT: 0)

    let totalPeriods = summary.periodEntries.count
    let expectedTotal = summary.expectedPerPeriod * Double(totalPeriods)
    let variance = summary.totalAmount - expectedTotal

    let cumulative = buildCumulative(entries: summary.periodEntries, totals: summary.periodTotals, expectedPerPeriod: summary.expectedPerPeriod)
    let periods: [ExportPeriod] = summary.periodEntries.enumerated().map { index, entry in
        let actual = summary.periodTotals[entry.key] ?? 0
        let pace = summary.expectedPerPeriod > 0 ? actual / summary.expectedPerPeriod : 0
        let recordCount = summary.periodCounts[entry.key] ?? 0
        let averageAward = recordCount > 0 ? actual / Double(recordCount) : 0
        let cumulativeEntry = cumulative[index]
        return ExportPeriod(
            key: entry.key,
            actual: actual,
            expected: summary.expectedPerPeriod,
            pace: pace,
            recordCount: recordCount,
            averageAward: averageAward,
            cumulativeActual: cumulativeEntry.actual,
            cumulativeExpected: cumulativeEntry.expected,
            cumulativeVariance: cumulativeEntry.variance
        )
    }

    let projections = summary.projection.keys.sorted(by: { $0.date < $1.date }).compactMap { entry -> ExportProjection? in
        guard let amount = summary.projection[entry] else { return nil }
        return ExportProjection(period: entry.key, amount: amount)
    }

    let categoryTargetVariances = buildTargetVariances(targets: config.categoryTargets, totals: summary.categoryTotals, totalAmount: summary.totalAmount)
    let cohortTargetVariances = buildTargetVariances(targets: config.cohortTargets, totals: summary.cohortTotals, totalAmount: summary.totalAmount)

    let payload = ExportPayload(
        generatedAt: formatter.string(from: Date()),
        periodType: summary.periodType.rawValue,
        annualBudget: config.annualBudget,
        dateRange: ExportDateRange(
            start: formatter.string(from: summary.startDate),
            end: formatter.string(from: summary.endDate)
        ),
        filters: ExportFilters(
            startDate: config.startDate.map { formatter.string(from: $0) },
            endDate: config.endDate.map { formatter.string(from: $0) },
            categories: config.categoryFilters,
            cohorts: config.cohortFilters
        ),
        totals: ExportTotals(
            records: summary.totalRecords,
            actual: summary.totalAmount,
            expected: expectedTotal,
            variance: variance,
            averageAward: summary.averageAward,
            medianAward: summary.medianAward
        ),
        periods: periods,
        missingPeriods: summary.missingPeriods.map { $0.key },
        paceAlerts: summary.paceFlags.map {
            ExportPaceAlert(period: $0.period, actual: $0.actual, expected: $0.expected, variance: $0.variance, pace: $0.pace)
        },
        projection: projections,
        topCategories: buildBreakdownEntries(totals: summary.categoryTotals, totalAmount: summary.totalAmount),
        topCohorts: buildBreakdownEntries(totals: summary.cohortTotals, totalAmount: summary.totalAmount),
        categoryTargets: categoryTargetVariances.isEmpty ? nil : categoryTargetVariances.map {
            ExportTargetVariance(
                name: $0.name,
                targetShare: $0.targetShare,
                actualAmount: $0.actualAmount,
                expectedAmount: $0.expectedAmount,
                variance: $0.variance,
                actualShare: $0.actualShare
            )
        },
        cohortTargets: cohortTargetVariances.isEmpty ? nil : cohortTargetVariances.map {
            ExportTargetVariance(
                name: $0.name,
                targetShare: $0.targetShare,
                actualAmount: $0.actualAmount,
                expectedAmount: $0.expectedAmount,
                variance: $0.variance,
                actualShare: $0.actualShare
            )
        },
        runway: buildRunway(summary: summary, config: config).map {
            ExportRunway(
                year: $0.year,
                periodsReported: $0.periodsReported,
                periodsPerYear: $0.periodsPerYear,
                remainingPeriods: $0.remainingPeriods,
                yearToDate: $0.yearToDate,
                remainingBudget: $0.remainingBudget,
                requiredAverage: $0.requiredAverage,
                recentAverage: $0.recentAverage,
                recentPeriods: $0.recentPeriods,
                deltaVsRecent: $0.deltaVsRecent
            )
        }
    )

    let encoder = JSONEncoder()
    encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
    let data = try encoder.encode(payload)
    let url = URL(fileURLWithPath: path)
    try data.write(to: url)
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
    print(String(format: "Average award: $%.2f", summary.averageAward))
    print(String(format: "Median award: $%.2f", summary.medianAward))

    if config.startDate != nil || config.endDate != nil || !config.categoryFilters.isEmpty || !config.cohortFilters.isEmpty {
        print("")
        print("Filters")
        if let start = config.startDate {
            print("Start date: \(formatter.string(from: start))")
        }
        if let end = config.endDate {
            print("End date: \(formatter.string(from: end))")
        }
        if !config.categoryFilters.isEmpty {
            print("Categories: \(config.categoryFilters.joined(separator: ", "))")
        }
        if !config.cohortFilters.isEmpty {
            print("Cohorts: \(config.cohortFilters.joined(separator: ", "))")
        }
    }
    print("")

    print("Period Breakdown")
    let header = String(format: "%-10@ | %-6@ | %-12@ | %-12@ | %-8@ | %-12@", "Period" as NSString, "Count" as NSString, "Actual" as NSString, "Expected" as NSString, "Pace" as NSString, "Avg Award" as NSString)
    print(header)
    print(String(repeating: "-", count: 78))

    for entry in summary.periodEntries {
        let actual = summary.periodTotals[entry.key] ?? 0
        let pace = summary.expectedPerPeriod > 0 ? actual / summary.expectedPerPeriod : 0
        let count = summary.periodCounts[entry.key] ?? 0
        let averageAward = count > 0 ? actual / Double(count) : 0
        let row = String(format: "%-10@ | %-6d | $%-11.2f | $%-11.2f | %-7.0f%% | $%-11.2f", entry.key as NSString, count, actual, summary.expectedPerPeriod, pace * 100, averageAward)
        print(row)
    }

    let cumulative = buildCumulative(entries: summary.periodEntries, totals: summary.periodTotals, expectedPerPeriod: summary.expectedPerPeriod)
    if !cumulative.isEmpty {
        print("")
        print("Cumulative Pace")
        let header = String(format: "%-10@ | %-12@ | %-12@ | %-12@", "Period" as NSString, "Actual" as NSString, "Expected" as NSString, "Variance" as NSString)
        print(header)
        print(String(repeating: "-", count: 56))
        for entry in cumulative {
            let row = String(format: "%-10@ | $%-11.2f | $%-11.2f | $%-11.2f", entry.entry.key as NSString, entry.actual, entry.expected, entry.variance)
            print(row)
        }
    }

    if !summary.missingPeriods.isEmpty {
        print("")
        print("Missing Periods")
        print("Count: \(summary.missingPeriods.count)")
        let list = summary.missingPeriods.prefix(6).map { $0.key }.joined(separator: ", ")
        print(list)
        if summary.missingPeriods.count > 6 {
            print("...and \(summary.missingPeriods.count - 6) more")
        }
    }

    let increases = summary.periodDeltas.filter { $0.delta > 0 }.sorted { $0.delta > $1.delta }
    let decreases = summary.periodDeltas.filter { $0.delta < 0 }.sorted { $0.delta < $1.delta }
    if !increases.isEmpty || !decreases.isEmpty {
        print("")
        print("Largest Period Swings")
        for delta in increases.prefix(3) {
            let percent = delta.percent.map { String(format: " (%.0f%%)", $0 * 100) } ?? ""
            let row = String(format: "+ $%-10.2f | %@ -> %@%@", delta.delta, delta.from.key, delta.to.key, percent)
            print(row)
        }
        for delta in decreases.prefix(3) {
            let percent = delta.percent.map { String(format: " (%.0f%%)", $0 * 100) } ?? ""
            let row = String(format: "- $%-10.2f | %@ -> %@%@", abs(delta.delta), delta.from.key, delta.to.key, percent)
            print(row)
        }
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

    if let runway = buildRunway(summary: summary, config: config) {
        print("")
        print("Budget Runway")
        print(String(format: "Remaining periods: %d of %d", runway.remainingPeriods, runway.periodsPerYear))
        print(String(format: "Remaining budget: $%.2f", runway.remainingBudget))
        if let requiredAverage = runway.requiredAverage {
            print(String(format: "Required avg per remaining period: $%.2f", requiredAverage))
        } else {
            print("Required avg per remaining period: n/a")
        }
        if let recentAverage = runway.recentAverage, runway.recentPeriods > 0 {
            print(String(format: "Recent avg (last %d periods): $%.2f", runway.recentPeriods, recentAverage))
        }
        if let delta = runway.deltaVsRecent {
            if delta > 0 {
                print(String(format: "Need to increase by $%.2f per period to hit budget.", delta))
            } else if delta < 0 {
                print(String(format: "Need to decrease by $%.2f per period to hit budget.", abs(delta)))
            } else {
                print("On track with recent pace.")
            }
        }
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

    let categoryTargets = buildTargetVariances(targets: config.categoryTargets, totals: summary.categoryTotals, totalAmount: summary.totalAmount)
    if !categoryTargets.isEmpty {
        print("")
        print("Category Targets")
        for target in categoryTargets {
            let varianceLabel = String(format: "%+.2f", target.variance)
            let row = String(
                format: "%-20@ | Target %.1f%% | Actual $%.2f (%.1f%%) | Variance $%@",
                target.name as NSString,
                target.targetShare * 100,
                target.actualAmount,
                target.actualShare * 100,
                varianceLabel
            )
            print(row)
        }
    }

    let cohortTargets = buildTargetVariances(targets: config.cohortTargets, totals: summary.cohortTotals, totalAmount: summary.totalAmount)
    if !cohortTargets.isEmpty {
        print("")
        print("Cohort Targets")
        for target in cohortTargets {
            let varianceLabel = String(format: "%+.2f", target.variance)
            let row = String(
                format: "%-20@ | Target %.1f%% | Actual $%.2f (%.1f%%) | Variance $%@",
                target.name as NSString,
                target.targetShare * 100,
                target.actualAmount,
                target.actualShare * 100,
                varianceLabel
            )
            print(row)
        }
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

func buildBreakdownEntries(totals: [String: Double], totalAmount: Double) -> [ExportBreakdown] {
    guard totalAmount > 0, !totals.isEmpty else { return [] }
    let sorted = totals.sorted { $0.value > $1.value }
    return sorted.prefix(5).map { entry in
        let share = (entry.value / totalAmount) * 100
        return ExportBreakdown(name: entry.key, amount: entry.value, share: share)
    }
}

func buildTargetVariances(targets: [TargetConfig], totals: [String: Double], totalAmount: Double) -> [TargetVariance] {
    guard totalAmount > 0, !targets.isEmpty else { return [] }
    var normalizedTotals: [String: Double] = [:]
    for (key, value) in totals {
        let normalized = key.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        normalizedTotals[normalized, default: 0] += value
    }

    return targets.map { target in
        let actual = normalizedTotals[target.normalized] ?? 0
        let expected = totalAmount * target.share
        let variance = actual - expected
        let actualShare = actual / totalAmount
        return TargetVariance(
            name: target.name,
            targetShare: target.share,
            actualAmount: actual,
            expectedAmount: expected,
            variance: variance,
            actualShare: actualShare
        )
    }
}

struct DBConfig {
    let host: String
    let port: Int
    let username: String
    let password: String
    let database: String
    let schema: String
}

func loadDbConfig(config: Config) throws -> DBConfig {
    let env = ProcessInfo.processInfo.environment
    guard let host = env["GS_DB_HOST"], !host.isEmpty else { throw DbError.missingEnv("GS_DB_HOST") }
    guard let portRaw = env["GS_DB_PORT"], let port = Int(portRaw) else { throw DbError.invalidEnv("GS_DB_PORT") }
    guard let user = env["GS_DB_USER"], !user.isEmpty else { throw DbError.missingEnv("GS_DB_USER") }
    guard let password = env["GS_DB_PASSWORD"], !password.isEmpty else { throw DbError.missingEnv("GS_DB_PASSWORD") }
    guard let database = env["GS_DB_NAME"], !database.isEmpty else { throw DbError.missingEnv("GS_DB_NAME") }
    let schema = config.dbSchema ?? env["GS_DB_SCHEMA"] ?? "award_pacing_monitor"

    return DBConfig(host: host, port: port, username: user, password: password, database: database, schema: schema)
}

func sqlLiteral(_ value: String) -> String {
    value.replacingOccurrences(of: "'", with: "''")
}

func sqlDecimal(_ value: Double) -> String {
    guard value.isFinite else { return "0" }
    return String(format: "%.2f", value)
}

func sqlDate(_ date: Date) -> String {
    let formatter = DateFormatter()
    formatter.dateFormat = "yyyy-MM-dd"
    formatter.timeZone = TimeZone(secondsFromGMT: 0)
    return formatter.string(from: date)
}

func sqlTimestamp(_ date: Date) -> String {
    let formatter = ISO8601DateFormatter()
    formatter.timeZone = TimeZone(secondsFromGMT: 0)
    return formatter.string(from: date)
}

func encodeFiltersJson(config: Config) throws -> String {
    let formatter = DateFormatter()
    formatter.dateFormat = "yyyy-MM-dd"
    formatter.timeZone = TimeZone(secondsFromGMT: 0)
    let filters = ExportFilters(
        startDate: config.startDate.map { formatter.string(from: $0) },
        endDate: config.endDate.map { formatter.string(from: $0) },
        categories: config.categoryFilters,
        cohorts: config.cohortFilters
    )
    let encoder = JSONEncoder()
    let data = try encoder.encode(filters)
    return String(data: data, encoding: .utf8) ?? "{}"
}

func syncToDatabase(summary: Summary, config: Config) throws {
    let dbConfig = try loadDbConfig(config: config)
    let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    defer { try? group.syncShutdownGracefully() }

    let pgConfig = PostgresConfiguration(
        host: dbConfig.host,
        port: dbConfig.port,
        username: dbConfig.username,
        password: dbConfig.password,
        database: dbConfig.database,
        tls: .disable
    )
    let connection = try PostgresConnection.connect(on: group.next(), configuration: pgConfig).wait()
    defer { try? connection.close().wait() }

    let schema = dbConfig.schema
    try connection.simpleQuery("CREATE SCHEMA IF NOT EXISTS \(schema);").wait()

    let createSnapshots = """
    CREATE TABLE IF NOT EXISTS \(schema).snapshots (
        snapshot_id UUID PRIMARY KEY,
        generated_at TIMESTAMPTZ NOT NULL,
        period_type TEXT NOT NULL,
        annual_budget NUMERIC NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL,
        total_records INTEGER NOT NULL,
        total_amount NUMERIC NOT NULL,
        expected_total NUMERIC NOT NULL,
        variance NUMERIC NOT NULL,
        average_award NUMERIC NOT NULL,
        median_award NUMERIC NOT NULL,
        filters_json TEXT NOT NULL
    );
    """
    let createPeriods = """
    CREATE TABLE IF NOT EXISTS \(schema).periods (
        snapshot_id UUID NOT NULL,
        period_key TEXT NOT NULL,
        actual NUMERIC NOT NULL,
        expected NUMERIC NOT NULL,
        pace NUMERIC NOT NULL,
        record_count INTEGER NOT NULL,
        average_award NUMERIC NOT NULL,
        cumulative_actual NUMERIC NOT NULL,
        cumulative_expected NUMERIC NOT NULL,
        cumulative_variance NUMERIC NOT NULL
    );
    """
    let createPaceAlerts = """
    CREATE TABLE IF NOT EXISTS \(schema).pace_alerts (
        snapshot_id UUID NOT NULL,
        period_key TEXT NOT NULL,
        actual NUMERIC NOT NULL,
        expected NUMERIC NOT NULL,
        variance NUMERIC NOT NULL,
        pace NUMERIC NOT NULL
    );
    """
    let createProjections = """
    CREATE TABLE IF NOT EXISTS \(schema).projections (
        snapshot_id UUID NOT NULL,
        period_key TEXT NOT NULL,
        amount NUMERIC NOT NULL
    );
    """
    let createBreakdowns = """
    CREATE TABLE IF NOT EXISTS \(schema).breakdowns (
        snapshot_id UUID NOT NULL,
        breakdown_type TEXT NOT NULL,
        name TEXT NOT NULL,
        amount NUMERIC NOT NULL,
        share NUMERIC NOT NULL
    );
    """
    let createTargets = """
    CREATE TABLE IF NOT EXISTS \(schema).targets (
        snapshot_id UUID NOT NULL,
        target_type TEXT NOT NULL,
        name TEXT NOT NULL,
        target_share NUMERIC NOT NULL,
        actual_amount NUMERIC NOT NULL,
        expected_amount NUMERIC NOT NULL,
        variance NUMERIC NOT NULL,
        actual_share NUMERIC NOT NULL
    );
    """
    let createMissing = """
    CREATE TABLE IF NOT EXISTS \(schema).missing_periods (
        snapshot_id UUID NOT NULL,
        period_key TEXT NOT NULL
    );
    """

    try connection.simpleQuery(createSnapshots).wait()
    try connection.simpleQuery(createPeriods).wait()
    try connection.simpleQuery(createPaceAlerts).wait()
    try connection.simpleQuery(createProjections).wait()
    try connection.simpleQuery(createBreakdowns).wait()
    try connection.simpleQuery(createTargets).wait()
    try connection.simpleQuery(createMissing).wait()

    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS average_award NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS median_award NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).periods ADD COLUMN IF NOT EXISTS record_count INTEGER NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).periods ADD COLUMN IF NOT EXISTS average_award NUMERIC NOT NULL DEFAULT 0;").wait()

    let snapshotId = UUID().uuidString
    let generatedAt = Date()
    let totalPeriods = summary.periodEntries.count
    let expectedTotal = summary.expectedPerPeriod * Double(totalPeriods)
    let variance = summary.totalAmount - expectedTotal
    let filtersJson = try encodeFiltersJson(config: config)

    let snapshotInsert = """
    INSERT INTO \(schema).snapshots
        (snapshot_id, generated_at, period_type, annual_budget, start_date, end_date, total_records, total_amount, expected_total, variance, average_award, median_award, filters_json)
    VALUES
        ('\(snapshotId)'::uuid, '\(sqlTimestamp(generatedAt))', '\(sqlLiteral(summary.periodType.rawValue))', \(sqlDecimal(config.annualBudget)),
         '\(sqlDate(summary.startDate))', '\(sqlDate(summary.endDate))', \(summary.totalRecords), \(sqlDecimal(summary.totalAmount)),
         \(sqlDecimal(expectedTotal)), \(sqlDecimal(variance)), \(sqlDecimal(summary.averageAward)), \(sqlDecimal(summary.medianAward)), '\(sqlLiteral(filtersJson))');
    """

    do {
        try connection.simpleQuery("BEGIN;").wait()
        try connection.simpleQuery(snapshotInsert).wait()

        let cumulative = buildCumulative(entries: summary.periodEntries, totals: summary.periodTotals, expectedPerPeriod: summary.expectedPerPeriod)
        for (index, entry) in summary.periodEntries.enumerated() {
            let actual = summary.periodTotals[entry.key] ?? 0
            let expected = summary.expectedPerPeriod
            let pace = expected > 0 ? actual / expected : 0
            let recordCount = summary.periodCounts[entry.key] ?? 0
            let averageAward = recordCount > 0 ? actual / Double(recordCount) : 0
            let cumulativeEntry = cumulative[index]
            let insert = """
            INSERT INTO \(schema).periods
                (snapshot_id, period_key, actual, expected, pace, record_count, average_award, cumulative_actual, cumulative_expected, cumulative_variance)
            VALUES
                ('\(snapshotId)'::uuid, '\(sqlLiteral(entry.key))', \(sqlDecimal(actual)), \(sqlDecimal(expected)),
                 \(sqlDecimal(pace)), \(recordCount), \(sqlDecimal(averageAward)), \(sqlDecimal(cumulativeEntry.actual)), \(sqlDecimal(cumulativeEntry.expected)), \(sqlDecimal(cumulativeEntry.variance)));
            """
            try connection.simpleQuery(insert).wait()
        }

        for missing in summary.missingPeriods {
            let insert = """
            INSERT INTO \(schema).missing_periods (snapshot_id, period_key)
            VALUES ('\(snapshotId)'::uuid, '\(sqlLiteral(missing.key))');
            """
            try connection.simpleQuery(insert).wait()
        }

        for alert in summary.paceFlags {
            let insert = """
            INSERT INTO \(schema).pace_alerts (snapshot_id, period_key, actual, expected, variance, pace)
            VALUES ('\(snapshotId)'::uuid, '\(sqlLiteral(alert.period))', \(sqlDecimal(alert.actual)), \(sqlDecimal(alert.expected)),
                    \(sqlDecimal(alert.variance)), \(sqlDecimal(alert.pace)));
            """
            try connection.simpleQuery(insert).wait()
        }

        for entry in summary.projection.keys.sorted(by: { $0.date < $1.date }) {
            if let amount = summary.projection[entry] {
                let insert = """
                INSERT INTO \(schema).projections (snapshot_id, period_key, amount)
                VALUES ('\(snapshotId)'::uuid, '\(sqlLiteral(entry.key))', \(sqlDecimal(amount)));
                """
                try connection.simpleQuery(insert).wait()
            }
        }

        let categoryBreakdowns = buildBreakdownEntries(totals: summary.categoryTotals, totalAmount: summary.totalAmount)
        let cohortBreakdowns = buildBreakdownEntries(totals: summary.cohortTotals, totalAmount: summary.totalAmount)
        for entry in categoryBreakdowns {
            let insert = """
            INSERT INTO \(schema).breakdowns (snapshot_id, breakdown_type, name, amount, share)
            VALUES ('\(snapshotId)'::uuid, 'category', '\(sqlLiteral(entry.name))', \(sqlDecimal(entry.amount)), \(sqlDecimal(entry.share)));
            """
            try connection.simpleQuery(insert).wait()
        }
        for entry in cohortBreakdowns {
            let insert = """
            INSERT INTO \(schema).breakdowns (snapshot_id, breakdown_type, name, amount, share)
            VALUES ('\(snapshotId)'::uuid, 'cohort', '\(sqlLiteral(entry.name))', \(sqlDecimal(entry.amount)), \(sqlDecimal(entry.share)));
            """
            try connection.simpleQuery(insert).wait()
        }

        let categoryTargets = buildTargetVariances(targets: config.categoryTargets, totals: summary.categoryTotals, totalAmount: summary.totalAmount)
        let cohortTargets = buildTargetVariances(targets: config.cohortTargets, totals: summary.cohortTotals, totalAmount: summary.totalAmount)
        for entry in categoryTargets {
            let insert = """
            INSERT INTO \(schema).targets (snapshot_id, target_type, name, target_share, actual_amount, expected_amount, variance, actual_share)
            VALUES ('\(snapshotId)'::uuid, 'category', '\(sqlLiteral(entry.name))', \(sqlDecimal(entry.targetShare)),
                    \(sqlDecimal(entry.actualAmount)), \(sqlDecimal(entry.expectedAmount)), \(sqlDecimal(entry.variance)), \(sqlDecimal(entry.actualShare)));
            """
            try connection.simpleQuery(insert).wait()
        }
        for entry in cohortTargets {
            let insert = """
            INSERT INTO \(schema).targets (snapshot_id, target_type, name, target_share, actual_amount, expected_amount, variance, actual_share)
            VALUES ('\(snapshotId)'::uuid, 'cohort', '\(sqlLiteral(entry.name))', \(sqlDecimal(entry.targetShare)),
                    \(sqlDecimal(entry.actualAmount)), \(sqlDecimal(entry.expectedAmount)), \(sqlDecimal(entry.variance)), \(sqlDecimal(entry.actualShare)));
            """
            try connection.simpleQuery(insert).wait()
        }

        try connection.simpleQuery("COMMIT;").wait()
    } catch {
        try? connection.simpleQuery("ROLLBACK;").wait()
        throw error
    }
}
