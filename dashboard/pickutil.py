"""Pure picker helpers (no Streamlit imports — unit-testable)."""


def merge_newcomers(current_pick, prev_seen, zeros, all_skus):
    """Fold newly-zeroed SKUs into the truck picker selection.

    Streamlit multiselect defaults apply only on first render, so without
    this a SKU hitting zero later never appears. Returns (pick, seen):
    pick None means leave the widget alone (absent key → default handles
    it). Manual picks/deselects are preserved — only genuinely new zeros
    are appended.
    """
    newcomers = [z for z in zeros if z not in set(prev_seen) and z in all_skus]
    seen = list(zeros)
    if current_pick is None:  # key absent: widget default handles it
        return None, seen
    extra = [z for z in newcomers if z not in current_pick]
    if extra:
        return list(current_pick) + extra, seen
    return None, seen
