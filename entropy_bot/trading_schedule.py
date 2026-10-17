from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class TradingScheduleState:
    paused: bool
    reason: str | None
    eastern_now: datetime
    session_open: datetime | None = None
    session_close: datetime | None = None


# NYSE's published 2026-2028 exceptional early closes. Ordinary full-day
# holidays are calculated below so the gate keeps working after 2028 too.
_PUBLISHED_EARLY_CLOSES = {
    date(2026, 11, 27),
    date(2026, 12, 24),
    date(2027, 11, 26),
    date(2028, 7, 3),
    date(2028, 11, 24),
}


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        first_next = date(year + 1, 1, 1)
    else:
        first_next = date(year, month + 1, 1)
    value = first_next - timedelta(days=1)
    return value - timedelta(days=(value.weekday() - weekday) % 7)


def _observed(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _easter_sunday(year: int) -> date:
    # Anonymous Gregorian computus.
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = (h + l - 7 * m + 114) % 31 + 1
    return date(year, month, day)


def nyse_full_day_holidays(year: int) -> set[date]:
    holidays = {
        _nth_weekday(year, 1, 0, 3),  # Martin Luther King Jr. Day
        _nth_weekday(year, 2, 0, 3),  # Washington's Birthday
        _easter_sunday(year) - timedelta(days=2),  # Good Friday
        _last_weekday(year, 5, 0),  # Memorial Day
        _observed(date(year, 6, 19)),
        _observed(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),  # Labor Day
        _nth_weekday(year, 11, 3, 4),  # Thanksgiving
        _observed(date(year, 12, 25)),
    }
    new_year = date(year, 1, 1)
    if new_year.weekday() == 6:
        holidays.add(new_year + timedelta(days=1))
    elif new_year.weekday() != 5:
        holidays.add(new_year)
    return holidays


def us_regular_session_state(
    *,
    enabled: bool,
    now: datetime | None = None,
    pre_open_buffer_min: int = 5,
    post_close_buffer_min: int = 5,
) -> TradingScheduleState:
    if now is None:
        eastern_now = datetime.now(timezone.utc).astimezone(NEW_YORK)
    elif now.tzinfo is None:
        raise ValueError("now 必须带时区")
    else:
        eastern_now = now.astimezone(NEW_YORK)

    if not enabled:
        return TradingScheduleState(False, None, eastern_now)

    today = eastern_now.date()
    if today.weekday() >= 5 or today in nyse_full_day_holidays(today.year):
        return TradingScheduleState(False, None, eastern_now)

    close_time = time(13, 0) if today in _PUBLISHED_EARLY_CLOSES else time(16, 0)
    core_open = datetime.combine(today, time(9, 30), NEW_YORK)
    core_close = datetime.combine(today, close_time, NEW_YORK)
    pause_from = core_open - timedelta(minutes=pre_open_buffer_min)
    resume_from = core_close + timedelta(minutes=post_close_buffer_min)
    paused = pause_from <= eastern_now < resume_from
    reason = None
    if paused:
        reason = (
            "美股常规交易时段保护：策略挂单已暂停；"
            f"预计美东 {resume_from:%H:%M} 后自动恢复"
        )
    return TradingScheduleState(
        paused,
        reason,
        eastern_now,
        session_open=core_open,
        session_close=core_close,
    )
