"""SimClock: central sim-time owner. Ticks in 1 sim-minute steps.

Speed (sim-min per wall-sec) only matters for live/demo modes; headless
replay runs as fast as possible. pause/step API is for the M4 dashboard.
"""

from __future__ import annotations

OPEN_MIN = 7 * 60
CLOSE_MIN = 22 * 60
DAY_MINUTES = 24 * 60
SIM_DATE = "2026-01-05"  # fixed demo date for sim_ts (doc/01)


def fmt(minute: int) -> str:
    if not 0 <= minute < DAY_MINUTES:
        raise ValueError(f"minute {minute} out of [0, 1440)")
    return f"{minute // 60:02d}:{minute % 60:02d}"


def to_iso(minute: int) -> str:
    """sim_ts for event contracts: 2026-01-05T09:14:00."""
    return f"{SIM_DATE}T{fmt(minute)}:00"


class SimClock:
    def __init__(self, start_min: int = OPEN_MIN, end_min: int = CLOSE_MIN) -> None:
        if not (0 <= start_min < DAY_MINUTES and 0 <= end_min <= DAY_MINUTES):
            raise ValueError(f"clock range [{start_min}, {end_min}] out of day")
        self.now_min = start_min
        self.end_min = end_min
        self.paused = False

    @property
    def done(self) -> bool:
        return self.now_min >= self.end_min

    def pause(self) -> None:
        self.paused = True

    def resume(self) -> None:
        self.paused = False

    def tick(self) -> int:
        if not self.paused and not self.done:
            self.now_min += 1
        return self.now_min

    def step(self, n: int = 15) -> int:
        """Manual advance (dashboard Step button). Never backwards, never
        past close, never while paused — time travel breaks velocity deques."""
        if n < 0:
            raise ValueError(f"step n must be >= 0, got {n}")
        if self.paused:
            return self.now_min
        self.now_min = min(self.end_min, self.now_min + n)
        return self.now_min
