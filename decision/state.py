"""Mutable shelf evolution: sale / receipt / confirmation transitions.

Stateful counterpart to the pure rules in decision/rules.py. Used by the
sim loop and the M3 decision service alike so both evolve state identically.

Physical invariant, enforced on every transition: 0 <= shelf_est <= boh.
BOH is the building total (floor + backroom), so the shelf can never hold
more than BOH. The min() caps below also heal phantom shelves left by
older over-confirmations on the next receipt.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass
class MutableShelf:
    boh: int
    shelf_est: int
    sales_since_task: int = 0
    zero_flag: bool = False
    zero_since_min: int | None = None
    last_task_min: int | None = None
    effective_cap: int = 0
    case_size: int = 1

    def apply_boh_update(
        self, new_boh: int, now_min: int = 0
    ) -> Literal["sale", "receipt", "noop"]:
        """Apply one boh_updates event. Returns sale | receipt | noop.

        Receipts land in the backroom: BOH rises, shelf unchanged (doc/02).
        """
        if new_boh < 0:
            raise ValueError("new_boh must be >= 0")
        if new_boh == self.boh:
            return "noop"
        if new_boh > self.boh:
            self.boh = new_boh  # backroom, NOT shelf
            self.shelf_est = min(self.shelf_est, self.boh)
            return "receipt"
        sold = self.boh - new_boh
        self.boh = new_boh
        self.shelf_est = max(0, self.shelf_est - sold)
        self.sales_since_task += sold
        if self.shelf_est <= 0 and not self.zero_flag:
            self.zero_flag = True
            self.zero_since_min = now_min
        return "sale"

    def apply_confirmation(self, cases_fetched: int, now_min: int) -> None:
        if cases_fetched < 0:
            raise ValueError("cases_fetched must be >= 0")
        self.shelf_est = min(
            self.effective_cap, self.shelf_est + cases_fetched * self.case_size, self.boh
        )
        self.sales_since_task = 0
        self.last_task_min = now_min
        if self.shelf_est > 0:
            self.zero_flag = False
            self.zero_since_min = None
