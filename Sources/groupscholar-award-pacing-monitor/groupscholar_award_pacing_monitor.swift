import Foundation
import NIOPosix
import PostgresNIO
import Logging

struct Config {
    let filePath: String
    let annualBudget: Double
    let period: PeriodType
    let periodWeights: [Double]?
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
    var periodWeights: [Double]?
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
        case "--period-weights":
            index += 1
            guard index < args.count else { throw ArgError.missingValue("--period-weights") }
            periodWeights = try parsePeriodWeights(args[index], flag: "--period-weights")
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
    if let weights = periodWeights {
        periodWeights = try normalizePeriodWeights(weights, period: period, flag: "--period-weights")
    }

    return Config(
        filePath: filePathUnwrapped,
        annualBudget: budgetUnwrapped,
        period: period,
        periodWeights: periodWeights,
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
                   [--period-weights list]
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

func parsePeriodWeights(_ raw: String, flag: String) throws -> [Double] {
    let entries = raw.split(separator: ",").map { String($0) }
    guard !entries.isEmpty else { throw ArgError.invalidValue(flag) }
    var results: [Double] = []
    for entry in entries {
        let trimmed = entry.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let parsed = Double(trimmed) else { throw ArgError.invalidValue(flag) }
        let weight = parsed > 1.0 ? parsed / 100.0 : parsed
        guard weight >= 0 else { throw ArgError.invalidValue(flag) }
        results.append(weight)
    }
    return results
}

func normalizePeriodWeights(_ raw: [Double], period: PeriodType, flag: String) throws -> [Double] {
    let expectedCount = period == .month ? 12 : 4
    guard raw.count == expectedCount else { throw ArgError.invalidValue(flag) }
    let total = raw.reduce(0, +)
    guard total > 0 else { throw ArgError.invalidValue(flag) }
    return raw.map { $0 / total }
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
    let awardStdDev: Double
    let awardCoeffVar: Double
    let topAwardShare: Double
    let topFiveShare: Double
    let awardBands: [AwardBandResult]
    let topAwards: [TopAward]
    let cadence: CadenceMetrics
    let periodTotals: [String: Double]
    let periodCounts: [String: Int]
    let periodEntries: [PeriodEntry]
    let missingPeriods: [PeriodEntry]
    let inactiveStreaks: [InactiveStreak]
    let expectedPerPeriod: Double
    let weightedExpectations: [String: Double]?
    let weightedExpectedTotal: Double?
    let weightedVariance: Double?
    let weightedPace: Double?
    let periodType: PeriodType
    let startDate: Date
    let endDate: Date
    let projection: [PeriodEntry: Double]
    let categoryTotals: [String: Double]
    let cohortTotals: [String: Double]
    let paceFlags: [PaceFlag]
    let weightedPaceFlags: [PaceFlag]
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

struct InactiveStreak {
    let start: PeriodEntry
    let end: PeriodEntry
    let length: Int
    let totalExpected: Double
    let totalActual: Double
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
    let periodWeights: [Double]?
    let dateRange: ExportDateRange
    let filters: ExportFilters
    let totals: ExportTotals
    let cadence: ExportCadence?
    let awardBands: [ExportAwardBand]
    let topAwards: [ExportTopAward]
    let periods: [ExportPeriod]
    let missingPeriods: [String]
    let inactiveStreaks: [ExportInactiveStreak]
    let paceAlerts: [ExportPaceAlert]
    let seasonalityAlerts: [ExportPaceAlert]?
    let projection: [ExportProjection]
    let topCategories: [ExportBreakdown]
    let topCohorts: [ExportBreakdown]
    let categoryTargets: [ExportTargetVariance]?
    let cohortTargets: [ExportTargetVariance]?
    let runway: ExportRunway?
    let concentration: ExportConcentration
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
    let weightedExpected: Double?
    let weightedVariance: Double?
    let weightedPace: Double?
    let averageAward: Double
    let medianAward: Double
    let awardStdDev: Double
    let awardCoeffVar: Double
}

struct ExportConcentration: Encodable {
    let topAwardShare: Double
    let topFiveShare: Double
}

struct ExportCadence: Encodable {
    let gapCount: Int
    let averageGapDays: Double?
    let medianGapDays: Double?
    let maxGapDays: Int?
    let recentGapDays: Int?
}

struct ExportAwardBand: Encodable {
    let label: String
    let minAmount: Double
    let maxAmount: Double?
    let recordCount: Int
    let totalAmount: Double
    let averageAward: Double
    let share: Double
}

struct ExportTopAward: Encodable {
    let date: String
    let amount: Double
    let category: String
    let cohort: String
}

struct ExportPeriod: Encodable {
    let key: String
    let actual: Double
    let expected: Double
    let weightedExpected: Double?
    let weightedVariance: Double?
    let pace: Double
    let weightedPace: Double?
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

struct ExportInactiveStreak: Encodable {
    let startPeriod: String
    let endPeriod: String
    let length: Int
    let expectedTotal: Double
    let actualTotal: Double
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

struct AwardBand {
    let label: String
    let minAmount: Double
    let maxAmount: Double?
}

struct AwardBandResult {
    let label: String
    let minAmount: Double
    let maxAmount: Double?
    let recordCount: Int
    let totalAmount: Double
    let averageAward: Double
    let share: Double
}

struct TopAward {
    let date: Date
    let amount: Double
    let category: String
    let cohort: String
}

struct CadenceMetrics {
    let gapCount: Int
    let averageGapDays: Double?
    let medianGapDays: Double?
    let maxGapDays: Int?
    let recentGapDays: Int?
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
    let weightedExpectations = buildWeightedExpectations(entries: orderedEntries, config: config)
    let weightedPaceFlags = buildWeightedPaceFlags(entries: orderedEntries, totals: periodTotals, expectedByPeriod: weightedExpectations)
    let weightedExpectedTotal = weightedExpectations?.values.reduce(0, +)
    let weightedVariance = weightedExpectedTotal.map { totalAmount - $0 }
    let weightedPace = weightedExpectedTotal.map { $0 > 0 ? totalAmount / $0 : 0 }
    let inactiveStreaks = buildInactiveStreaks(entries: orderedEntries, totals: periodTotals, period: config.period, expectedPerPeriod: expectedPerPeriod)
    let averageAward = records.isEmpty ? 0 : totalAmount / Double(records.count)
    let medianAward = computeMedian(values: amounts)
    let awardStdDev = computeStdDev(values: amounts)
    let awardCoeffVar = averageAward > 0 ? awardStdDev / averageAward : 0
    let topAwardShare = computeConcentration(values: amounts, topCount: 1)
    let topFiveShare = computeConcentration(values: amounts, topCount: 5)
    let awardBands = buildAwardBands(records: records, totalAmount: totalAmount)
    let topAwards = buildTopAwards(records: records, limit: 5)
    let cadence = buildCadence(records: records)

    return Summary(
        totalRecords: records.count,
        totalAmount: totalAmount,
        averageAward: averageAward,
        medianAward: medianAward,
        awardStdDev: awardStdDev,
        awardCoeffVar: awardCoeffVar,
        topAwardShare: topAwardShare,
        topFiveShare: topFiveShare,
        awardBands: awardBands,
        topAwards: topAwards,
        cadence: cadence,
        periodTotals: periodTotals,
        periodCounts: periodCounts,
        periodEntries: orderedEntries,
        missingPeriods: missingPeriods,
        inactiveStreaks: inactiveStreaks,
        expectedPerPeriod: expectedPerPeriod,
        weightedExpectations: weightedExpectations,
        weightedExpectedTotal: weightedExpectedTotal,
        weightedVariance: weightedVariance,
        weightedPace: weightedPace,
        periodType: config.period,
        startDate: minDate,
        endDate: maxDate,
        projection: projection,
        categoryTotals: categoryTotals,
        cohortTotals: cohortTotals,
        paceFlags: paceFlags,
        weightedPaceFlags: weightedPaceFlags,
        yearTotals: yearTotals,
        yearPeriods: yearPeriods,
        periodDeltas: periodDeltas
    )
}

func periodWeightIndex(for entry: PeriodEntry, period: PeriodType) -> Int? {
    switch period {
    case .month:
        let parts = entry.key.split(separator: "-")
        guard parts.count == 2, let month = Int(parts[1]), month >= 1 else { return nil }
        return month - 1
    case .quarter:
        guard let range = entry.key.range(of: "-Q") else { return nil }
        let quarterValue = entry.key[range.upperBound...]
        guard let quarter = Int(quarterValue), quarter >= 1 else { return nil }
        return quarter - 1
    }
}

func buildWeightedExpectations(entries: [PeriodEntry], config: Config) -> [String: Double]? {
    guard let weights = config.periodWeights else { return nil }
    var expectations: [String: Double] = [:]
    for entry in entries {
        guard let index = periodWeightIndex(for: entry, period: config.period),
              index < weights.count else { continue }
        expectations[entry.key] = config.annualBudget * weights[index]
    }
    return expectations
}

func buildWeightedPaceFlags(entries: [PeriodEntry], totals: [String: Double], expectedByPeriod: [String: Double]?) -> [PaceFlag] {
    guard let expectedByPeriod = expectedByPeriod else { return [] }
    let lowerBound = 0.8
    let upperBound = 1.2
    var flags: [PaceFlag] = []

    for entry in entries {
        guard let expected = expectedByPeriod[entry.key], expected > 0 else { continue }
        let actual = totals[entry.key] ?? 0
        let pace = actual / expected
        if pace < lowerBound || pace > upperBound {
            let variance = actual - expected
            flags.append(PaceFlag(period: entry.key, actual: actual, expected: expected, variance: variance, pace: pace))
        }
    }

    return flags.sorted { abs($0.variance) > abs($1.variance) }
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

func buildInactiveStreaks(entries: [PeriodEntry], totals: [String: Double], period: PeriodType, expectedPerPeriod: Double) -> [InactiveStreak] {
    guard let first = entries.first, let last = entries.last else { return [] }
    var streaks: [InactiveStreak] = []
    var cursor = first
    var currentStart: PeriodEntry?
    var currentLength = 0
    var currentActual = 0.0
    var lastInactive: PeriodEntry?

    while cursor.date <= last.date {
        let actual = totals[cursor.key] ?? 0
        let isInactive = actual == 0

        if isInactive {
            if currentStart == nil {
                currentStart = cursor
                currentLength = 0
                currentActual = 0
            }
            currentLength += 1
            currentActual += actual
            lastInactive = cursor
        } else if let start = currentStart, let end = lastInactive {
            streaks.append(
                InactiveStreak(
                    start: start,
                    end: end,
                    length: currentLength,
                    totalExpected: expectedPerPeriod * Double(currentLength),
                    totalActual: currentActual
                )
            )
            currentStart = nil
            currentLength = 0
            currentActual = 0
            lastInactive = nil
        }

        cursor = nextPeriod(from: cursor, period: period)
    }

    if let start = currentStart, let end = lastInactive {
        streaks.append(
            InactiveStreak(
                start: start,
                end: end,
                length: currentLength,
                totalExpected: expectedPerPeriod * Double(currentLength),
                totalActual: currentActual
            )
        )
    }

    return streaks.sorted {
        if $0.length == $1.length {
            return $0.start.date < $1.start.date
        }
        return $0.length > $1.length
    }
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

func computeStdDev(values: [Double]) -> Double {
    guard !values.isEmpty else { return 0 }
    let mean = values.reduce(0, +) / Double(values.count)
    let variance = values.reduce(0) { $0 + pow($1 - mean, 2) } / Double(values.count)
    return sqrt(variance)
}

func computeConcentration(values: [Double], topCount: Int) -> Double {
    guard topCount > 0 else { return 0 }
    let total = values.reduce(0, +)
    guard total > 0 else { return 0 }
    let top = values.sorted(by: >).prefix(topCount).reduce(0, +)
    return top / total
}

func buildAwardBands(records: [Record], totalAmount: Double) -> [AwardBandResult] {
    let bands: [AwardBand] = [
        AwardBand(label: "<$1k", minAmount: 0, maxAmount: 1000),
        AwardBand(label: "$1k-$5k", minAmount: 1000, maxAmount: 5000),
        AwardBand(label: "$5k-$10k", minAmount: 5000, maxAmount: 10000),
        AwardBand(label: "$10k-$25k", minAmount: 10000, maxAmount: 25000),
        AwardBand(label: "$25k-$50k", minAmount: 25000, maxAmount: 50000),
        AwardBand(label: ">$50k", minAmount: 50000, maxAmount: nil)
    ]

    return bands.map { band in
        let matching = records.filter { record in
            record.amount >= band.minAmount && (band.maxAmount == nil || record.amount < band.maxAmount!)
        }
        let count = matching.count
        let total = matching.reduce(0.0) { $0 + $1.amount }
        let average = count > 0 ? total / Double(count) : 0
        let share = totalAmount > 0 ? total / totalAmount : 0
        return AwardBandResult(
            label: band.label,
            minAmount: band.minAmount,
            maxAmount: band.maxAmount,
            recordCount: count,
            totalAmount: total,
            averageAward: average,
            share: share
        )
    }
}

func buildTopAwards(records: [Record], limit: Int) -> [TopAward] {
    let calendar = Calendar(identifier: .gregorian)
    let sorted = records.sorted { $0.amount > $1.amount }
    var results: [TopAward] = []
    for record in sorted.prefix(limit) {
        guard let date = calendar.date(from: DateComponents(year: record.year, month: record.month, day: record.day)) else { continue }
        results.append(TopAward(date: date, amount: record.amount, category: record.category, cohort: record.cohort))
    }
    return results
}

func buildCadence(records: [Record]) -> CadenceMetrics {
    let calendar = Calendar(identifier: .gregorian)
    var uniqueDays = Set<Date>()

    for record in records {
        guard let date = calendar.date(from: DateComponents(year: record.year, month: record.month, day: record.day)) else { continue }
        uniqueDays.insert(calendar.startOfDay(for: date))
    }

    let orderedDays = uniqueDays.sorted()
    guard orderedDays.count > 1 else {
        return CadenceMetrics(gapCount: 0, averageGapDays: nil, medianGapDays: nil, maxGapDays: nil, recentGapDays: nil)
    }

    var gaps: [Int] = []
    for index in 1..<orderedDays.count {
        let gap = calendar.dateComponents([.day], from: orderedDays[index - 1], to: orderedDays[index]).day ?? 0
        gaps.append(max(0, gap))
    }

    let average = Double(gaps.reduce(0, +)) / Double(gaps.count)
    let median = computeMedian(values: gaps.map { Double($0) })
    let maxGap = gaps.max()
    let recentGap = gaps.last

    return CadenceMetrics(
        gapCount: gaps.count,
        averageGapDays: average,
        medianGapDays: median,
        maxGapDays: maxGap,
        recentGapDays: recentGap
    )
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
        let weightedExpected = summary.weightedExpectations?[entry.key]
        let weightedVariance = weightedExpected.map { actual - $0 }
        let weightedPace = weightedExpected.flatMap { $0 > 0 ? actual / $0 : nil }
        let recordCount = summary.periodCounts[entry.key] ?? 0
        let averageAward = recordCount > 0 ? actual / Double(recordCount) : 0
        let cumulativeEntry = cumulative[index]
        return ExportPeriod(
            key: entry.key,
            actual: actual,
            expected: summary.expectedPerPeriod,
            weightedExpected: weightedExpected,
            weightedVariance: weightedVariance,
            pace: pace,
            weightedPace: weightedPace,
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
        periodWeights: config.periodWeights,
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
            weightedExpected: summary.weightedExpectedTotal,
            weightedVariance: summary.weightedVariance,
            weightedPace: summary.weightedPace,
            averageAward: summary.averageAward,
            medianAward: summary.medianAward,
            awardStdDev: summary.awardStdDev,
            awardCoeffVar: summary.awardCoeffVar
        ),
        cadence: summary.cadence.gapCount > 0 ? ExportCadence(
            gapCount: summary.cadence.gapCount,
            averageGapDays: summary.cadence.averageGapDays,
            medianGapDays: summary.cadence.medianGapDays,
            maxGapDays: summary.cadence.maxGapDays,
            recentGapDays: summary.cadence.recentGapDays
        ) : nil,
        awardBands: summary.awardBands.map {
            ExportAwardBand(
                label: $0.label,
                minAmount: $0.minAmount,
                maxAmount: $0.maxAmount,
                recordCount: $0.recordCount,
                totalAmount: $0.totalAmount,
                averageAward: $0.averageAward,
                share: $0.share
            )
        },
        topAwards: summary.topAwards.map {
            ExportTopAward(
                date: formatter.string(from: $0.date),
                amount: $0.amount,
                category: $0.category,
                cohort: $0.cohort
            )
        },
        periods: periods,
        missingPeriods: summary.missingPeriods.map { $0.key },
        inactiveStreaks: summary.inactiveStreaks.map {
            ExportInactiveStreak(
                startPeriod: $0.start.key,
                endPeriod: $0.end.key,
                length: $0.length,
                expectedTotal: $0.totalExpected,
                actualTotal: $0.totalActual
            )
        },
        paceAlerts: summary.paceFlags.map {
            ExportPaceAlert(period: $0.period, actual: $0.actual, expected: $0.expected, variance: $0.variance, pace: $0.pace)
        },
        seasonalityAlerts: summary.weightedPaceFlags.isEmpty ? nil : summary.weightedPaceFlags.map {
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
        },
        concentration: ExportConcentration(
            topAwardShare: summary.topAwardShare,
            topFiveShare: summary.topFiveShare
        )
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
    print(String(format: "Award std dev: $%.2f", summary.awardStdDev))
    print(String(format: "Award coeff var: %.2f", summary.awardCoeffVar))
    if summary.cadence.gapCount > 0 {
        print(String(format: "Award cadence avg gap: %.1f days", summary.cadence.averageGapDays ?? 0))
        print(String(format: "Award cadence median gap: %.1f days", summary.cadence.medianGapDays ?? 0))
    }

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

    if !summary.inactiveStreaks.isEmpty {
        print("")
        print("Inactive Period Streaks")
        for streak in summary.inactiveStreaks.prefix(3) {
            let range = "\(streak.start.key) -> \(streak.end.key)"
            let row = String(format: "%2d periods | %-19@ | Expected $%-10.2f | Actual $%-10.2f",
                             streak.length, range as NSString, streak.totalExpected, streak.totalActual)
            print(row)
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

    if summary.weightedExpectations != nil {
        print("")
        print("Seasonality Pacing (weighted expectations)")
        if let configWeights = config.periodWeights, !configWeights.isEmpty {
            let weightLabel = configWeights.map { String(format: "%.1f%%", $0 * 100) }.joined(separator: ", ")
            print("Weights: \(weightLabel)")
        }
        if let weightedExpectedTotal = summary.weightedExpectedTotal {
            print(String(format: "Weighted expected total: $%.2f", weightedExpectedTotal))
        }
        if let weightedVariance = summary.weightedVariance {
            print(String(format: "Weighted variance: $%.2f", weightedVariance))
        }
        if let weightedPace = summary.weightedPace {
            print(String(format: "Weighted pace: %.0f%%", weightedPace * 100))
        }
        if summary.weightedPaceFlags.isEmpty {
            print("No seasonality alerts.")
        } else {
            let header = String(format: "%-10@ | %-12@ | %-12@ | %-8@", "Period" as NSString, "Actual" as NSString, "Expected" as NSString, "Pace" as NSString)
            print(header)
            print(String(repeating: "-", count: 52))
            for flag in summary.weightedPaceFlags.prefix(6) {
                let row = String(format: "%-10@ | $%-11.2f | $%-11.2f | %-7.0f%%", flag.period as NSString, flag.actual, flag.expected, flag.pace * 100)
                print(row)
            }
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

    if summary.cadence.gapCount > 0 {
        print("")
        print("Award Cadence")
        print("Award days tracked: \(summary.cadence.gapCount + 1)")
        if let average = summary.cadence.averageGapDays {
            print(String(format: "Average gap: %.1f days", average))
        }
        if let median = summary.cadence.medianGapDays {
            print(String(format: "Median gap: %.1f days", median))
        }
        if let maxGap = summary.cadence.maxGapDays {
            print(String(format: "Longest gap: %d days", maxGap))
        }
        if let recentGap = summary.cadence.recentGapDays {
            print(String(format: "Most recent gap: %d days", recentGap))
        }
    }

    if !summary.awardBands.isEmpty {
        print("")
        print("Award Size Bands")
        let header = String(format: "%-12@ | %-6@ | %-12@ | %-12@ | %-8@", "Band" as NSString, "Count" as NSString, "Total" as NSString, "Avg Award" as NSString, "Share" as NSString)
        print(header)
        print(String(repeating: "-", count: 60))
        for band in summary.awardBands {
            let row = String(format: "%-12@ | %-6d | $%-11.2f | $%-11.2f | %-7.1f%%", band.label as NSString, band.recordCount, band.totalAmount, band.averageAward, band.share * 100)
            print(row)
        }
    }

    if !summary.topAwards.isEmpty {
        print("")
        print("Top Awards")
        let header = String(format: "%-12@ | %-12@ | %-16@ | %-12@", "Date" as NSString, "Amount" as NSString, "Category" as NSString, "Cohort" as NSString)
        print(header)
        print(String(repeating: "-", count: 70))
        for award in summary.topAwards {
            let date = formatter.string(from: award.date)
            let row = String(format: "%-12@ | $%-11.2f | %-16@ | %-12@", date as NSString, award.amount, award.category as NSString, award.cohort as NSString)
            print(row)
        }
    }

    print("")
    print("Award Concentration")
    print(String(format: "Top award share: %.1f%%", summary.topAwardShare * 100))
    print(String(format: "Top 5 awards share: %.1f%%", summary.topFiveShare * 100))

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
        weighted_expected_total NUMERIC NOT NULL,
        weighted_variance NUMERIC NOT NULL,
        weighted_pace NUMERIC NOT NULL,
        average_award NUMERIC NOT NULL,
        median_award NUMERIC NOT NULL,
        award_std_dev NUMERIC NOT NULL,
        award_coeff_var NUMERIC NOT NULL,
        top_award_share NUMERIC NOT NULL,
        top_five_share NUMERIC NOT NULL,
        gap_count INTEGER NOT NULL,
        average_gap_days NUMERIC NOT NULL,
        median_gap_days NUMERIC NOT NULL,
        max_gap_days INTEGER NOT NULL,
        recent_gap_days INTEGER NOT NULL,
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
        weighted_expected NUMERIC NOT NULL,
        weighted_variance NUMERIC NOT NULL,
        weighted_pace NUMERIC NOT NULL,
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
    let createSeasonalityAlerts = """
    CREATE TABLE IF NOT EXISTS \(schema).seasonality_alerts (
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
    let createInactiveStreaks = """
    CREATE TABLE IF NOT EXISTS \(schema).inactive_streaks (
        snapshot_id UUID NOT NULL,
        start_period TEXT NOT NULL,
        end_period TEXT NOT NULL,
        length INTEGER NOT NULL,
        expected_total NUMERIC NOT NULL,
        actual_total NUMERIC NOT NULL
    );
    """
    let createBands = """
    CREATE TABLE IF NOT EXISTS \(schema).size_bands (
        snapshot_id UUID NOT NULL,
        band_label TEXT NOT NULL,
        min_amount NUMERIC NOT NULL,
        max_amount NUMERIC,
        record_count INTEGER NOT NULL,
        total_amount NUMERIC NOT NULL,
        average_award NUMERIC NOT NULL,
        share NUMERIC NOT NULL
    );
    """
    let createTopAwards = """
    CREATE TABLE IF NOT EXISTS \(schema).top_awards (
        snapshot_id UUID NOT NULL,
        award_date DATE NOT NULL,
        amount NUMERIC NOT NULL,
        category TEXT NOT NULL,
        cohort TEXT NOT NULL
    );
    """

    try connection.simpleQuery(createSnapshots).wait()
    try connection.simpleQuery(createPeriods).wait()
    try connection.simpleQuery(createPaceAlerts).wait()
    try connection.simpleQuery(createSeasonalityAlerts).wait()
    try connection.simpleQuery(createProjections).wait()
    try connection.simpleQuery(createBreakdowns).wait()
    try connection.simpleQuery(createTargets).wait()
    try connection.simpleQuery(createMissing).wait()
    try connection.simpleQuery(createInactiveStreaks).wait()
    try connection.simpleQuery(createBands).wait()
    try connection.simpleQuery(createTopAwards).wait()

    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS average_award NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS median_award NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS award_std_dev NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS award_coeff_var NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS top_award_share NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS top_five_share NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS weighted_expected_total NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS weighted_variance NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS weighted_pace NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS gap_count INTEGER NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS average_gap_days NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS median_gap_days NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS max_gap_days INTEGER NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).snapshots ADD COLUMN IF NOT EXISTS recent_gap_days INTEGER NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).periods ADD COLUMN IF NOT EXISTS record_count INTEGER NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).periods ADD COLUMN IF NOT EXISTS average_award NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).periods ADD COLUMN IF NOT EXISTS weighted_expected NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).periods ADD COLUMN IF NOT EXISTS weighted_variance NUMERIC NOT NULL DEFAULT 0;").wait()
    try connection.simpleQuery("ALTER TABLE \(schema).periods ADD COLUMN IF NOT EXISTS weighted_pace NUMERIC NOT NULL DEFAULT 0;").wait()

    let snapshotId = UUID().uuidString
    let generatedAt = Date()
    let totalPeriods = summary.periodEntries.count
    let expectedTotal = summary.expectedPerPeriod * Double(totalPeriods)
    let variance = summary.totalAmount - expectedTotal
    let weightedExpectedTotal = summary.weightedExpectedTotal ?? 0
    let weightedVariance = summary.weightedVariance ?? 0
    let weightedPace = summary.weightedPace ?? 0
    let filtersJson = try encodeFiltersJson(config: config)

    let snapshotInsert = """
    INSERT INTO \(schema).snapshots
        (snapshot_id, generated_at, period_type, annual_budget, start_date, end_date, total_records, total_amount, expected_total, variance,
         weighted_expected_total, weighted_variance, weighted_pace,
         average_award, median_award, award_std_dev, award_coeff_var, top_award_share, top_five_share, gap_count, average_gap_days,
         median_gap_days, max_gap_days, recent_gap_days, filters_json)
    VALUES
        ('\(snapshotId)'::uuid, '\(sqlTimestamp(generatedAt))', '\(sqlLiteral(summary.periodType.rawValue))', \(sqlDecimal(config.annualBudget)),
         '\(sqlDate(summary.startDate))', '\(sqlDate(summary.endDate))', \(summary.totalRecords), \(sqlDecimal(summary.totalAmount)),
         \(sqlDecimal(expectedTotal)), \(sqlDecimal(variance)), \(sqlDecimal(weightedExpectedTotal)), \(sqlDecimal(weightedVariance)),
         \(sqlDecimal(weightedPace)), \(sqlDecimal(summary.averageAward)), \(sqlDecimal(summary.medianAward)),
         \(sqlDecimal(summary.awardStdDev)), \(sqlDecimal(summary.awardCoeffVar)), \(sqlDecimal(summary.topAwardShare)), \(sqlDecimal(summary.topFiveShare)),
         \(summary.cadence.gapCount), \(sqlDecimal(summary.cadence.averageGapDays ?? 0)), \(sqlDecimal(summary.cadence.medianGapDays ?? 0)),
         \(summary.cadence.maxGapDays ?? 0), \(summary.cadence.recentGapDays ?? 0),
         '\(sqlLiteral(filtersJson))');
    """

    do {
        try connection.simpleQuery("BEGIN;").wait()
        try connection.simpleQuery(snapshotInsert).wait()

        let cumulative = buildCumulative(entries: summary.periodEntries, totals: summary.periodTotals, expectedPerPeriod: summary.expectedPerPeriod)
        for (index, entry) in summary.periodEntries.enumerated() {
            let actual = summary.periodTotals[entry.key] ?? 0
            let expected = summary.expectedPerPeriod
            let pace = expected > 0 ? actual / expected : 0
            let weightedExpected = summary.weightedExpectations?[entry.key] ?? 0
            let weightedVariance = actual - weightedExpected
            let weightedPace = weightedExpected > 0 ? actual / weightedExpected : 0
            let recordCount = summary.periodCounts[entry.key] ?? 0
            let averageAward = recordCount > 0 ? actual / Double(recordCount) : 0
            let cumulativeEntry = cumulative[index]
            let insert = """
            INSERT INTO \(schema).periods
                (snapshot_id, period_key, actual, expected, pace, weighted_expected, weighted_variance, weighted_pace, record_count, average_award, cumulative_actual, cumulative_expected, cumulative_variance)
            VALUES
                ('\(snapshotId)'::uuid, '\(sqlLiteral(entry.key))', \(sqlDecimal(actual)), \(sqlDecimal(expected)),
                 \(sqlDecimal(pace)), \(sqlDecimal(weightedExpected)), \(sqlDecimal(weightedVariance)), \(sqlDecimal(weightedPace)),
                 \(recordCount), \(sqlDecimal(averageAward)), \(sqlDecimal(cumulativeEntry.actual)), \(sqlDecimal(cumulativeEntry.expected)), \(sqlDecimal(cumulativeEntry.variance)));
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

        for streak in summary.inactiveStreaks {
            let insert = """
            INSERT INTO \(schema).inactive_streaks (snapshot_id, start_period, end_period, length, expected_total, actual_total)
            VALUES ('\(snapshotId)'::uuid, '\(sqlLiteral(streak.start.key))', '\(sqlLiteral(streak.end.key))', \(streak.length),
                    \(sqlDecimal(streak.totalExpected)), \(sqlDecimal(streak.totalActual)));
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

        for alert in summary.weightedPaceFlags {
            let insert = """
            INSERT INTO \(schema).seasonality_alerts (snapshot_id, period_key, actual, expected, variance, pace)
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

        for band in summary.awardBands {
            let maxValue = band.maxAmount.map { sqlDecimal($0) } ?? "NULL"
            let insert = """
            INSERT INTO \(schema).size_bands (snapshot_id, band_label, min_amount, max_amount, record_count, total_amount, average_award, share)
            VALUES ('\(snapshotId)'::uuid, '\(sqlLiteral(band.label))', \(sqlDecimal(band.minAmount)), \(maxValue), \(band.recordCount),
                    \(sqlDecimal(band.totalAmount)), \(sqlDecimal(band.averageAward)), \(sqlDecimal(band.share)));
            """
            try connection.simpleQuery(insert).wait()
        }

        for award in summary.topAwards {
            let insert = """
            INSERT INTO \(schema).top_awards (snapshot_id, award_date, amount, category, cohort)
            VALUES ('\(snapshotId)'::uuid, '\(sqlDate(award.date))', \(sqlDecimal(award.amount)),
                    '\(sqlLiteral(award.category))', '\(sqlLiteral(award.cohort))');
            """
            try connection.simpleQuery(insert).wait()
        }

        try connection.simpleQuery("COMMIT;").wait()
    } catch {
        try? connection.simpleQuery("ROLLBACK;").wait()
        throw error
    }
}
