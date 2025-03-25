import XCTest
@testable import groupscholar_award_pacing_monitor

final class InactiveStreakTests: XCTestCase {
    func testInactiveStreaksCaptureMissingAndZeroPeriods() {
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
        let apr = PeriodEntry(
            key: "2025-04",
            date: calendar.date(from: DateComponents(year: 2025, month: 4, day: 1))!,
            year: 2025
        )

        let entries = [jan, feb, apr]
        let totals: [String: Double] = [
            "2025-01": 100,
            "2025-02": 0,
            "2025-04": 50
        ]

        let streaks = buildInactiveStreaks(entries: entries, totals: totals, period: .month, expectedPerPeriod: 100)
        XCTAssertEqual(streaks.count, 1)
        let streak = streaks[0]
        XCTAssertEqual(streak.start.key, "2025-02")
        XCTAssertEqual(streak.end.key, "2025-03")
        XCTAssertEqual(streak.length, 2)
        XCTAssertEqual(streak.totalExpected, 200, accuracy: 0.01)
        XCTAssertEqual(streak.totalActual, 0, accuracy: 0.01)
    }
}
