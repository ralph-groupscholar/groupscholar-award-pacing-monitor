import XCTest
@testable import groupscholar_award_pacing_monitor

final class StatsTests: XCTestCase {
    func testComputeStdDev() {
        let values: [Double] = [100, 200, 300]
        let stdDev = computeStdDev(values: values)
        XCTAssertEqual(stdDev, 81.65, accuracy: 0.01)
    }

    func testComputeConcentrationTopOne() {
        let values: [Double] = [50, 50, 100]
        let share = computeConcentration(values: values, topCount: 1)
        XCTAssertEqual(share, 0.5, accuracy: 0.0001)
    }

    func testComputeConcentrationTopFiveCapsAtTotal() {
        let values: [Double] = [25, 25, 50]
        let share = computeConcentration(values: values, topCount: 5)
        XCTAssertEqual(share, 1.0, accuracy: 0.0001)
    }

    func testComputeStdDevEmpty() {
        let stdDev = computeStdDev(values: [])
        XCTAssertEqual(stdDev, 0)
    }

    func testBuildCadenceUsesUniqueAwardDays() {
        let records = [
            Record(year: 2025, month: 1, day: 1, amount: 100, category: "Tuition", cohort: "Fall"),
            Record(year: 2025, month: 1, day: 1, amount: 200, category: "Tuition", cohort: "Fall"),
            Record(year: 2025, month: 1, day: 6, amount: 300, category: "Stipend", cohort: "Fall")
        ]

        let cadence = buildCadence(records: records)
        XCTAssertEqual(cadence.gapCount, 1)
        XCTAssertEqual(cadence.averageGapDays, 5, accuracy: 0.01)
        XCTAssertEqual(cadence.medianGapDays, 5, accuracy: 0.01)
        XCTAssertEqual(cadence.maxGapDays, 5)
        XCTAssertEqual(cadence.recentGapDays, 5)
    }

    func testBuildPeriodStatsRecentMomentum() {
        let date1 = buildDate(year: 2025, month: 1, day: 1)!
        let date2 = buildDate(year: 2025, month: 2, day: 1)!
        let date3 = buildDate(year: 2025, month: 3, day: 1)!
        let date4 = buildDate(year: 2025, month: 4, day: 1)!
        let entries = [
            PeriodEntry(key: "2025-01", date: date1, year: 2025),
            PeriodEntry(key: "2025-02", date: date2, year: 2025),
            PeriodEntry(key: "2025-03", date: date3, year: 2025),
            PeriodEntry(key: "2025-04", date: date4, year: 2025)
        ]
        let totals: [String: Double] = [
            "2025-01": 100,
            "2025-02": 200,
            "2025-03": 300,
            "2025-04": 400
        ]

        let stats = buildPeriodStats(entries: entries, totals: totals)
        XCTAssertEqual(stats.average, 250, accuracy: 0.001)
        XCTAssertEqual(stats.median, 250, accuracy: 0.001)
        XCTAssertEqual(stats.min, 100, accuracy: 0.001)
        XCTAssertEqual(stats.max, 400, accuracy: 0.001)
        XCTAssertEqual(stats.recentAverage, 300, accuracy: 0.001)
        XCTAssertEqual(stats.recentMomentum, 1.2, accuracy: 0.001)
        XCTAssertEqual(stats.recentPeriods, ["2025-02", "2025-03", "2025-04"])
    }

    func testWeightedSeasonalityTotals() {
        let weights = [0.1, 0.2] + Array(repeating: 0.07, count: 10)
        let config = Config(
            filePath: "sample/awards.csv",
            annualBudget: 1000,
            period: .month,
            periodWeights: weights,
            projectionPeriods: 0,
            startDate: nil,
            endDate: nil,
            categoryFilters: [],
            cohortFilters: [],
            categoryTargets: [],
            cohortTargets: [],
            exportPath: nil,
            dbSync: false,
            dbSchema: nil
        )
        let records = [
            Record(year: 2025, month: 1, day: 5, amount: 100, category: "Tuition", cohort: "Fall"),
            Record(year: 2025, month: 2, day: 10, amount: 200, category: "Tuition", cohort: "Fall")
        ]

        let summary = buildSummary(records: records, config: config)
        XCTAssertEqual(summary.weightedExpectedTotal, 300, accuracy: 0.001)
        XCTAssertEqual(summary.weightedVariance, 0, accuracy: 0.001)
        XCTAssertEqual(summary.weightedPace, 1.0, accuracy: 0.001)
    }
}
