import XCTest
@testable import groupscholar_award_pacing_monitor

final class SeasonalityTests: XCTestCase {
    func testPeriodWeightIndexForMonth() {
        let calendar = Calendar(identifier: .gregorian)
        let entry = PeriodEntry(
            key: "2025-03",
            date: calendar.date(from: DateComponents(year: 2025, month: 3, day: 1))!,
            year: 2025
        )

        let index = periodWeightIndex(for: entry, period: .month)
        XCTAssertEqual(index, 2)
    }

    func testPeriodWeightIndexForQuarter() {
        let calendar = Calendar(identifier: .gregorian)
        let entry = PeriodEntry(
            key: "2025-Q4",
            date: calendar.date(from: DateComponents(year: 2025, month: 10, day: 1))!,
            year: 2025
        )

        let index = periodWeightIndex(for: entry, period: .quarter)
        XCTAssertEqual(index, 3)
    }

    func testBuildWeightedExpectationsUsesWeights() {
        let calendar = Calendar(identifier: .gregorian)
        let jan = PeriodEntry(
            key: "2025-01",
            date: calendar.date(from: DateComponents(year: 2025, month: 1, day: 1))!,
            year: 2025
        )
        let feb = PeriodEntry(
            key: "2025-02",
            date: calendar.date(from: DateComponents(year: 2025, month: 2, day: 1))!,
            year: 2025
        )

        let weights: [Double] = [0.5, 0.25, 0.25] + Array(repeating: 0, count: 9)
        let config = Config(
            filePath: "sample.csv",
            annualBudget: 1200,
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

        let expectations = buildWeightedExpectations(entries: [jan, feb], config: config)
        XCTAssertEqual(expectations?["2025-01"], 600, accuracy: 0.01)
        XCTAssertEqual(expectations?["2025-02"], 300, accuracy: 0.01)
    }

    func testWeightedPaceFlagsCaptureSeasonalityVariance() {
        let calendar = Calendar(identifier: .gregorian)
        let jan = PeriodEntry(
            key: "2025-01",
            date: calendar.date(from: DateComponents(year: 2025, month: 1, day: 1))!,
            year: 2025
        )

        let weights: [Double] = [1.0] + Array(repeating: 0, count: 11)
        let config = Config(
            filePath: "sample.csv",
            annualBudget: 1200,
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

        let expectations = buildWeightedExpectations(entries: [jan], config: config)
        let totals: [String: Double] = ["2025-01": 900]
        let flags = buildWeightedPaceFlags(entries: [jan], totals: totals, expectedByPeriod: expectations)
        XCTAssertEqual(flags.count, 1)
        XCTAssertEqual(flags.first?.period, "2025-01")
        XCTAssertEqual(flags.first?.expected ?? 0, 1200, accuracy: 0.01)
    }
}
