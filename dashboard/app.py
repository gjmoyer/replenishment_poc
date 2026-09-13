"""Replenishment monitor: per-store shelf grid, task queue, inject panel.

Single-page ops console. Reads Postgres (written by decision/sim services),
writes sim_control for time/scenario commands, publishes confirmations to
Kafka. Every fragment owns fresh short-lived connections (psycopg conns are
not thread-safe across Streamlit's fragment threads). Clicks render
optimistically from session state first, then publish in try/except —
a broker/DB failure toasts instead of killing the demo.
"""
from __future__ import annotations

import json
import os
import time
import uuid

import psycopg
import streamlit as st
from kafka import KafkaProducer

from sim.catalog import build_catalog
from sim.clock import fmt as fmt_min

PRODUCTS = {s.sku: s for s in build_catalog(45)}
THRESHOLDS = {sku: s.threshold_pct for sku, s in PRODUCTS.items()}

# --- palette: dark workspace, soft slate surfaces (never bright white) --------
RED, AMBER, GREEN = "#F2555A", "#F5A524", "#3FAD6E"
RULE_BLUE, LLM_PURPLE, GRAY = "#7AA2FF", "#A885FF", "#8B8D98"
INK, INK_DIM = "#E9ECF3", "#9AA3B5"

st.set_page_config(page_title="Shelf Replenishment Monitor", layout="wide")

st.markdown(
    """
<style>
#MainMenu, footer, header[data-testid="stHeader"] {display: none;}
.block-container {max-width: 1440px; padding: 0.75rem 1rem 8rem 1rem;}
.stickybar {position: sticky; top: 0; z-index: 999; background: #14161B;
  border-bottom: 1px solid #2E3442; padding: 8px 1rem; margin: 0 -1rem 12px -1rem;}
.clock {font-family: ui-monospace, monospace; font-size: 28px; color: #E9ECF3;
  font-variant-numeric: tabular-nums;}
.num {font-variant-numeric: tabular-nums;}
.card {background: #1E2230; border: 1px solid #2E3442; border-radius: 8px;
  padding: 12px; margin-bottom: 8px; color: #E9ECF3;}
.card .dim {color: #9AA3B5;}
.bar {height: 8px; border-radius: 4px; background: #2A2F3C; overflow: hidden;
  position: relative;}
.bar > div.fill {height: 100%; border-radius: 4px;}
.bar > div.peak {position: absolute; top: 0; bottom: 0; background: rgba(255,255,255,0.10);}
.pill {display: inline-block; font-size: 11px; font-weight: 600; padding: 1px 7px;
  border-radius: 20px; margin-left: 4px; white-space: nowrap;}
.p-rule {background: #3E63DD; color: #fff;}
.p-llm {background: #8E4EC6; color: #fff;}
.p-promo {border: 1px solid #F5A524; color: #F5C86E; background: transparent;}
.p-bulk {border: 1px solid #8B8D98; color: #B9BEC9; background: transparent;}
.p-zero {background: #E5484D; color: #fff;}
.p-open {background: #3E63DD; color: #fff;}
.mono {font-family: ui-monospace, monospace; font-size: 11px;}
.rulebox {font-family: ui-monospace, monospace; font-size: 12px; background: #262B38;
  color: #E9ECF3; border-radius: 6px; padding: 6px 8px; margin: 6px 0;}
.llmbox {border-left: 3px solid #A885FF; padding: 4px 0 4px 10px; margin: 6px 0;
  font-style: italic; font-size: 13px; color: #E9ECF3;}
.skipbox {border-left: 3px solid #F5A524; padding: 4px 0 4px 10px; margin: 6px 0;
  font-size: 13px; color: #9AA3B5;}
.logrow {font-size: 12px; padding: 3px 0; border-bottom: 1px solid #2A2F3C; color: #E9ECF3;}
.dot {display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px;}
.footer {position: fixed; bottom: 0; left: 0; right: 0; background: #14161B;
  border-top: 1px solid #2E3442; z-index: 999; font-size: 13px; color: #E9ECF3;
  font-variant-numeric: tabular-nums;}
.stButton > button[kind="primary"] {background: #F5F6F8; border-color: #F5F6F8; color: #111;}
div[data-testid="stSegmentedControl"] {margin-bottom: 0;}
</style>
""",
    unsafe_allow_html=True,
)

PEAKS = ((12 * 60, 14 * 60), (17 * 60, 19 * 60))
DAY_OPEN, DAY_SPAN = 7 * 60, 15 * 60

# --- io (fresh connection per call; fragments run on threads) ---------------------


def _conn():
    return psycopg.connect(
        os.getenv("DATABASE_URL", "postgresql://poc:poc@localhost:5432/poc"),
        autocommit=True,
        connect_timeout=3,
    )


def q(sql, params=()):
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description] if cur.description else []
            return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
    except Exception as e:
        st.toast(f"db read failed: {type(e).__name__}", icon="⚠")
        return []


def set_fields(**fields):
    try:
        with _conn() as conn, conn.cursor() as cur:
            sets = ", ".join(f"{k}=%s" for k in fields)
            cur.execute(f"UPDATE sim_control SET {sets} WHERE id=1", list(fields.values()))
        return True
    except Exception as e:
        st.toast(f"control write failed: {type(e).__name__}", icon="⚠")
        return False


def claim_cmd(name, arg=None):
    """Issue a runner command only if none is pending (no silent drops)."""
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE sim_control SET cmd=%s, cmd_arg=%s WHERE id=1 AND cmd IS NULL",
                (name, json.dumps(arg) if arg is not None else None),
            )
            if cur.rowcount:
                return True
        st.toast("runner busy with previous command — retry in a second", icon="⚠")
        return False
    except Exception as e:
        st.toast(f"command failed: {type(e).__name__}", icon="⚠")
        return False


@st.cache_resource
def kafka_producer():
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: (k or "").encode(),
        linger_ms=20,
    )


def get_ctl() -> dict:
    rows = q("SELECT sim_min, paused, speed, seed, day_done, flags FROM sim_control WHERE id=1")
    return rows[0] if rows else {"sim_min": 420, "paused": True, "speed": 60,
                                 "seed": 42, "day_done": False, "flags": {}}


# --- helpers ---------------------------------------------------------------------


def aisle_of(sku: str) -> str:
    return f"Aisle {(sum(map(ord, sku)) % 8) + 1}"


def location_of(sku: str, is_promo: bool) -> str:
    loc = aisle_of(sku)
    return f"Endcap-A + {loc}" if is_promo else loc


def pct_color(pct: float, thresh: float) -> str:
    if pct < thresh:
        return RED
    if pct < 0.5:
        return AMBER
    return GREEN


@st.cache_data(max_entries=500)
def sparkline(points: tuple) -> str:
    pts = (list(points) + [0] * 60)[:60]
    mx = max(pts) or 1
    coords = " ".join(f"{i * 80 / 59:.1f},{24 - (v / mx) * 22:.1f}" for i, v in enumerate(pts))
    return (
        '<svg width="80" height="24" style="float:right">'
        f'<polyline points="{coords}" fill="none" stroke="#3E63DD" stroke-width="1.5"/>'
        "</svg>"
    )


def publish_confirmation(store_id, sku, task_id, action, cases):
    ctl = get_ctl()
    sim_ts = f"2026-01-05T{fmt_min(ctl['sim_min'])}:00"
    kafka_producer().send("restock_confirmations", key=f"{store_id}:{sku}", value={
        "task_id": task_id, "store_id": store_id, "sku": sku, "sim_ts": sim_ts,
        "action": action, "cases_fetched": cases, "actor": "dashboard",
        "event_id": uuid.uuid4().hex,
    })
    kafka_producer().flush(2.0)


def prune_session(open_ids: set):
    st.session_state.done_tasks = {t: w for t, w in st.session_state.done_tasks.items()
                                   if t in open_ids or time.time() - w < 120}
    st.session_state.skipped = {t: w for t, w in st.session_state.skipped.items()
                                if t in open_ids}
    st.session_state.adj = {t: c for t, c in st.session_state.adj.items() if t in open_ids}


# --- session + store ---------------------------------------------------------------

params = st.query_params
stores = [r["store_id"] for r in q("SELECT DISTINCT store_id FROM shelf_state ORDER BY 1")]
if not stores:
    stores = ["store-001", "store-002"]
if "store" not in st.session_state:
    st.session_state.store = params.get("store", stores[0])
if st.session_state.store not in stores:
    st.session_state.store = stores[0]
st.query_params["store"] = st.session_state.store
store = st.session_state.store

st.session_state.setdefault("done_tasks", {})
st.session_state.setdefault("skipped", {})
st.session_state.setdefault("adj", {})
st.session_state.setdefault("show_all", False)

# --- header (sticky) ---------------------------------------------------------------


@st.fragment(run_every=1)
def header():
    ctl = get_ctl()
    now_min = ctl["sim_min"]
    day_pct = min(max((now_min - DAY_OPEN) / DAY_SPAN, 0.0), 1.0)
    c1, c2, c3, c4 = st.columns([2.2, 4.5, 2.6, 2.7])
    with c1:
        sel = st.selectbox("Store", stores, index=stores.index(st.session_state.store),
                           key="store_sel", label_visibility="collapsed")
        if sel != st.session_state.store:
            st.session_state.store = sel
            st.query_params["store"] = sel
    with c2:
        st.markdown(
            f'<span class="clock">{fmt_min(now_min)}</span> '
            '<span class="num" style="color:#8B8D98">SIM &middot; day '
            f"{day_pct:.0%} &middot; peak 12-14 / 17-19</span>",
            unsafe_allow_html=True,
        )
        peaks = "".join(
            f'<div class="peak" style="left:{(a - DAY_OPEN) / DAY_SPAN * 100:.1f}%;'
            f"width:{(b - a) / DAY_SPAN * 100:.1f}%\"></div>"
            for a, b in PEAKS
        )
        st.markdown(
            f'<div class="bar"><div class="fill" style="width:{day_pct * 100:.1f}%;'
            f'background:{RULE_BLUE}"></div>{peaks}</div>',
            unsafe_allow_html=True,
        )
    with c3:
        if "speed" not in st.session_state:
            st.session_state.speed = ctl["speed"]
        if st.segmented_control("Speed", [30, 60, 120], key="speed",
                                label_visibility="collapsed") != ctl["speed"]:
            set_fields(speed=st.session_state.speed)
    with c4:
        b1, b2, b3 = st.columns(3)
        if b1.button("Pause" if not ctl["paused"] else "Play", key="pp"):
            set_fields(paused=not ctl["paused"])
        if b2.button("Step +15m", key="step"):
            claim_cmd("step", {"n": 15})
        if b3.button("Restart", key="restart"):
            if claim_cmd("restart"):
                st.session_state.done_tasks = {}
                st.session_state.skipped = {}
                st.session_state.adj = {}
    st.markdown(
        f'<span class="pill p-bulk">seed:{ctl["seed"]}</span>'
        f'<span class="pill p-bulk">{"PAUSED" if ctl["paused"] else f"{ctl["speed"]}x"}</span>'
        + ('<span class="pill p-zero">DAY DONE</span>' if ctl["day_done"] else ""),
        unsafe_allow_html=True,
    )
    if ctl["day_done"]:
        syn = q("""SELECT count(*) FILTER (WHERE status='done') AS done,
                          count(*) AS total,
                          count(*) FILTER (WHERE status='rejected') AS rej
                   FROM tasks WHERE store_id=%s""", (store,))[0]
        llm = q("SELECT count(*) AS n FROM llm_calls")[0]
        z = q("SELECT count(*) AS n FROM shelf_state WHERE store_id=%s AND zero_flag",
              (store,))[0]
        st.success(f"Day complete 07:00–22:00 for {store} — done {syn['done']}/{syn['total']}, "
                   f"rejected {syn['rej']}, LLM calls {llm['n']} (all stores), "
                   f"shelves still zero: {z['n']}.")
        if st.button("Restart day", key="day_restart"):
            if claim_cmd("restart"):
                st.session_state.done_tasks = {}
                st.session_state.skipped = {}
                st.session_state.adj = {}


st.markdown('<div class="stickybar">', unsafe_allow_html=True)
header()
st.markdown("</div>", unsafe_allow_html=True)

# --- body ----------------------------------------------------------------------------

left, right = st.columns([0.6, 0.4], gap="medium")

with left:

    @st.fragment(run_every=2)
    def shelf_grid():
        now_min = get_ctl()["sim_min"]
        t1, t2, t3 = st.columns([2.4, 3.4, 1.6])
        with t1:
            s = st.text_input("Search SKU", key="search", label_visibility="collapsed",
                              placeholder="Search SKU")
        with t2:
            if "filt" not in st.session_state:
                st.session_state.filt = "All"
            f = st.segmented_control("Filter", ["All", "Needs restock", "Promo", "Bulk", "Zero"],
                                     key="filt", label_visibility="collapsed")
        with t3:
            st.toggle("Show all", key="show_all")
        rows = q("""SELECT store_id, sku, boh, shelf_est, effective_cap, case_size,
                           is_promo, is_bulk, velocity_30m, velocity_120m, zero_flag
                    FROM shelf_state WHERE store_id=%s""", (store,))
        open_rows = q("SELECT sku FROM tasks WHERE store_id=%s AND status='open'", (store,))
        open_set = {r["sku"] for r in open_rows}
        sold_rows = q("""SELECT sku, sum(units) AS n FROM sales_hist
                         WHERE store_id=%s GROUP BY 1""", (store,))
        sold_today = {r["sku"]: r["n"] for r in sold_rows}
        fill_rows = q("""SELECT sku, max(emit_sim_min) AS m FROM tasks
                         WHERE store_id=%s AND status='done' GROUP BY 1""", (store,))
        last_fill = {r["sku"]: r["m"] for r in fill_rows}
        hist = q("""SELECT sku, sim_min, units FROM sales_hist
                    WHERE store_id=%s AND sim_min>%s""", (store, now_min - 60))
        by_sku: dict[str, list] = {}
        for h in hist:
            by_sku.setdefault(h["sku"], []).append((h["sim_min"], h["units"]))

        def series(sku):
            pts = [0] * 60
            for m, u in by_sku.get(sku, []):
                i = m - (now_min - 60)
                if 0 <= i < 60:
                    pts[i] += u
            return tuple(pts)

        cards = []
        for r in rows:
            cap = r["effective_cap"] or 1
            pct = r["shelf_est"] / cap
            thresh = THRESHOLDS.get(r["sku"], 0.35)
            urgent = r["zero_flag"] or pct < thresh or r["sku"] in open_set
            if s and s.lower() not in r["sku"].lower():
                continue
            if f == "Needs restock" and not urgent:
                continue
            if f == "Promo" and not r["is_promo"]:
                continue
            if f == "Bulk" and not r["is_bulk"]:
                continue
            if f == "Zero" and not r["zero_flag"]:
                continue
            cards.append((not urgent, pct, r))
        cards.sort(key=lambda t: (t[0], t[1]))
        if not st.session_state.show_all:
            cards = cards[:24]
        st.caption(f"showing {len(cards)}/{len(rows)} — urgent-first")
        if not cards:
            st.info("All shelves stocked — no action needed.")
            return
        for i in range(0, len(cards), 3):
            cols = st.columns(3)
            for j, (_, pct, r) in enumerate(cards[i:i + 3]):
                with cols[j]:
                    thresh = THRESHOLDS.get(r["sku"], 0.35)
                    color = pct_color(pct, thresh)
                    loc = location_of(r["sku"], r["is_promo"])
                    width = min(pct, 1) * 100
                    prod = PRODUCTS.get(r["sku"])
                    name = prod.name if prod else r["sku"]
                    pack = prod.pack if prod else "unit"
                    shelf_cap = prod.shelf_capacity_units if prod else r["effective_cap"]
                    extra = r["effective_cap"] - shelf_cap
                    endcap = f" (+{extra} endcap)" if extra > 0 else ""
                    backroom = max(r["boh"] - r["shelf_est"], 0)
                    if r["shelf_est"] <= 0:
                        cover = "EMPTY"
                    elif r["velocity_30m"] > 0:
                        cover = f"~{r['shelf_est'] / r['velocity_30m']:.0f}m"
                    else:
                        cover = "—"
                    fill = fmt_min(last_fill[r["sku"]]) if r["sku"] in last_fill else "—"
                    flags = ""
                    if r["is_promo"]:
                        flags += '<span class="pill p-promo">PROMO</span>'
                    if r["is_bulk"]:
                        flags += '<span class="pill p-bulk">BULK</span>'
                    if r["zero_flag"]:
                        flags += '<span class="pill p-zero">ZERO</span>'
                    if r["sku"] in open_set:
                        flags += '<span class="pill p-open">OPEN TASK</span>'
                    st.markdown(
                        f'<div class="card"><div><b>{name}</b>{flags}</div>'
                        f'<div class="dim mono">{r["sku"]} &middot; {pack} &middot; {loc}</div>'
                        f'<div class="dim">Case of {r["case_size"]} &middot; '
                        f"Shelf {shelf_cap}{endcap} &middot; refill &lt;{thresh:.0%}</div>"
                        f'<div class="bar" style="margin:6px 0">'
                        f'<div class="fill" style="width:{width:.0f}%;'
                        f"background:{color}\"></div></div>"
                        f'<div class="num">Shelf {r["shelf_est"]}/{r["effective_cap"]}'
                        f" ({pct:.0%}) &middot; BOH {r['boh']} (backroom ~{backroom})</div>"
                        f'<div class="num dim">vel {r["velocity_30m"]:.1f}/'
                        f'{r["velocity_120m"]:.1f} u/m &middot; cover {cover} &middot; '
                        f'sold today {sold_today.get(r["sku"], 0)} &middot; last fill {fill}</div>'
                        f"{sparkline(series(r['sku']))}"
                        '<div style="clear:both"></div></div>',
                        unsafe_allow_html=True,
                    )

    shelf_grid()

with right:

    @st.fragment(run_every=1)
    def task_queue():
        now_min = get_ctl()["sim_min"]
        tasks = q("""SELECT t.task_id, t.sku, t.emit_sim_min, t.action, t.reason, t.cases,
                            t.source, t.rationale, t.confidence, t.status,
                            t.shelf_at_emit, t.boh_at_emit,
                            s.is_promo, s.boh, s.shelf_est, s.effective_cap, s.case_size
                     FROM tasks t LEFT JOIN shelf_state s
                       ON s.store_id=t.store_id AND s.sku=t.sku
                     WHERE t.store_id=%s AND t.status='open'
                     ORDER BY t.emit_sim_min ASC LIMIT 30""", (store,))
        prune_session({t["task_id"] for t in tasks})
        st.subheader(f"Task queue ({len(tasks)} open)")
        if not tasks:
            st.info("Queue clear. Next likely task: bread 09:30 peak.")
            return
        for t in tasks:
            tid = t["task_id"]
            if tid in st.session_state.done_tasks:
                st.markdown(
                    f'<div class="card">Done ✓ {t["sku"]} '
                    f'<span class="mono">confirmed {fmt_min(now_min)} SIM</span></div>',
                    unsafe_allow_html=True,
                )
                continue
            if tid in st.session_state.skipped:
                st.markdown(
                    f'<div class="card"><b>{t["sku"]}</b> — '
                    '<span class="skipbox">Skipped by associate; quiet 30m SIM</span></div>',
                    unsafe_allow_html=True,
                )
                continue
            age = now_min - t["emit_sim_min"]
            badge = ("p-llm", "LLM") if t["source"] == "llm" else (
                ("p-rule", "RULE-FB") if t["source"] == "rule_fallback" else ("p-rule", "RULE"))
            age_html = (f'<b style="color:{RED}">+{age}m sim</b>'
                        if age > 60 else f"+{age}m sim")
            emitted = fmt_min(t["emit_sim_min"])
            prod = PRODUCTS.get(t["sku"])
            name = prod.name if prod else t["sku"]
            loc = location_of(t["sku"], bool(t["is_promo"]))
            thresh = THRESHOLDS.get(t["sku"], 0.35)
            case_size = t["case_size"] or 0
            adj = st.session_state.adj.get(tid, t["cases"])
            units = adj * case_size if case_size else 0
            cap = t["effective_cap"] or 0
            shelf_then = t["shelf_at_emit"]
            boh_then = t["boh_at_emit"]
            drift = ""
            if shelf_then is not None and t["shelf_est"] is not None and cap:
                drift = (f"Shelf {shelf_then}→{t['shelf_est']}/{cap} &middot; "
                         f"BOH {boh_then}→{t['boh']} &middot; refill &lt;{thresh:.0%}")
            conf = (f'<div><span class="pill p-llm">conf {t["confidence"]:.2f}</span></div>'
                    if t["confidence"] is not None else "")
            if t["source"] == "llm":
                rat = f'<div class="llmbox">{t["rationale"]}</div>{conf}'
            else:
                rat = (f'<div class="rulebox">at emit {emitted}: {t["rationale"]}</div>')
            idline = f'<span class="dim mono">{t["sku"]} &middot; dock</span>'
            if t["action"] == "check":
                st.markdown(
                    f'<div class="card"><span class="pill {badge[0]}">{badge[1]}</span> '
                    f'<span class="mono">{t["reason"]}</span> &middot; {age_html}<br>'
                    f"<b>{name}</b> &middot; {idline}<br>"
                    f"<b>FETCH 0 — verify dock</b> &middot; live BOH:{t['boh']} &middot; "
                    f"truck {emitted}<br>{rat}</div>",
                    unsafe_allow_html=True,
                )
                if st.button("Verify dock", key=f"dock_{tid}"):
                    st.session_state.done_tasks[tid] = time.time()
                    try:
                        publish_confirmation(store, t["sku"], tid, "done", adj)
                    except Exception as e:
                        st.toast(f"publish failed ({type(e).__name__}) — will retry", icon="⚠")
                continue
            nameline = (f'<b>{name}</b> &middot; '
                        f'<span class="dim mono">{t["sku"]} &middot; {loc}</span>')
            st.markdown(
                f'<div class="card"><span class="pill {badge[0]}">{badge[1]}</span> '
                f'<span class="mono">{t["reason"]}</span> &middot; {age_html} '
                f'&middot; emitted {emitted}<br>'
                f"{nameline}<br>"
                f'<span style="font-size:18px"><b>FETCH {adj} cases</b></span>'
                f'<span class="dim"> ({units} units)</span><br>'
                f'<div class="dim num">{drift}</div>{rat}</div>',
                unsafe_allow_html=True,
            )
            b1, b2, b3, b4 = st.columns([3, 1, 1, 1.4])
            if b1.button("Restock done", key=f"done_{tid}", type="primary"):
                st.session_state.done_tasks[tid] = time.time()
                try:
                    publish_confirmation(store, t["sku"], tid, "done", adj)
                except Exception as e:
                    st.toast(f"publish failed ({type(e).__name__}) — will retry", icon="⚠")
            if b2.button("−1", key=f"m_{tid}"):
                st.session_state.adj[tid] = max(adj - 1, 0)
            if b3.button("+1", key=f"p_{tid}"):
                st.session_state.adj[tid] = adj + 1
            if b4.button("Skip", key=f"skip_{tid}"):
                st.session_state.skipped[tid] = time.time()
                try:
                    publish_confirmation(store, t["sku"], tid, "reject", 0)
                except Exception as e:
                    st.toast(f"publish failed ({type(e).__name__}) — will retry", icon="⚠")

    task_queue()

    @st.fragment(run_every=2)
    def event_log():
        st.subheader("Event log")
        f1, f2 = st.columns([2, 3])
        with f1:
            fsku = st.text_input("SKU filter", key="logsku", label_visibility="collapsed",
                                 placeholder="SKU filter")
        with f2:
            ftype = st.multiselect("Type", ["task", "check", "suppress", "receipt", "truck",
                                            "confirm", "llm", "abandon"],
                                   key="logtype", label_visibility="collapsed",
                                   placeholder="Type filter")
        sql = ("SELECT sim_min, wall, store_id, sku, type, message, source FROM events"
               " WHERE store_id=%s")
        params: list = [store]
        if fsku:
            sql += " AND sku ILIKE %s"
            params.append(f"%{fsku}%")
        if ftype:
            sql += " AND type = ANY(%s)"
            params.append(ftype)
        sql += " ORDER BY id DESC LIMIT 120"
        rows = q(sql, params)
        colors = {"task": RED, "check": AMBER, "suppress": AMBER, "receipt": GREEN,
                  "truck": RULE_BLUE, "confirm": INK, "llm": LLM_PURPLE, "abandon": GRAY}
        if not rows and (not ftype or "truck" in ftype):
            st.caption("No trucks yet — scheduled 10:30 / 14:00 or Send truck now.")
        html = ['<div style="height:280px;overflow-y:auto">']
        for r in rows:
            strike = "text-decoration:line-through;" if r["type"] == "suppress" else ""
            src = (f' <span class="pill {"p-llm" if r["source"] == "llm" else "p-rule"}>'
                   f'{r["source"]}</span>' if r["source"] else "")
            wall = r["wall"].strftime("%H:%M:%S") if r["wall"] else "??:??:??"
            html.append(
                f'<div class="logrow" style="{strike}">'
                f'<span class="mono">{fmt_min(r["sim_min"])} SIM &middot; {wall} wall</span> '
                f'<span class="dot" style="background:{colors.get(r["type"], GRAY)}"></span>'
                f'<b>{r["type"]}</b> {r["sku"] or ""} — {r["message"]}{src}</div>'
            )
        html.append("</div>")
        st.markdown("".join(html), unsafe_allow_html=True)

    event_log()

    @st.fragment(run_every=5)
    def inject_panel():
        ctl = get_ctl()
        st.subheader("Inject events")
        disabled = ctl["day_done"]
        # Honor a pending clear BEFORE the multiselect below: assigning a
        # widget key pre-instantiation is legal (post-instantiation raises),
        # so the click handler only sets this flag for the next run.
        zeros = [r["sku"] for r in
                 q("SELECT sku FROM shelf_state WHERE store_id=%s AND zero_flag", (store,))]
        all_skus = [r["sku"] for r in
                    q("SELECT sku FROM shelf_state WHERE store_id=%s ORDER BY 1", (store,))]
        if st.session_state.pop("_clear_truck_pick", False):
            # Dispatch acknowledged: empty the picker, remember the zeros
            # so *fresh* zeros later re-arm the preselect (below).
            st.session_state["truck_pick"] = []
            st.session_state["_truck_base_zeros"] = list(zeros)
        elif (st.session_state.get("truck_pick") == []
              and "_truck_base_zeros" in st.session_state
              and set(zeros) != set(st.session_state["_truck_base_zeros"])):
            # Zero set changed since the last dispatch and the user hasn't
            # composed anything: drop the key so the default re-applies.
            st.session_state.pop("truck_pick", None)
            st.session_state.pop("_truck_base_zeros", None)
        pick = st.multiselect("Truck SKUs (zero-flag preselected)", all_skus,
                              default=[z for z in zeros if z in all_skus], key="truck_pick",
                              disabled=disabled)
        if st.button("Send truck now", key="truck_go", disabled=disabled or not pick):
            if claim_cmd("truck_now", {"store_id": store, "skus": pick}):
                st.toast(f"Truck dispatched to {store} ({len(pick)} SKUs)")
                st.session_state["_clear_truck_pick"] = True
        c1, c2, c3 = st.columns([3, 1.4, 1.6])
        bsku = c1.selectbox("Burst SKU", all_skus, key="burst_sku", disabled=disabled,
                            label_visibility="collapsed")
        bunits = c2.number_input("units", 1, 30, 10, key="burst_n", disabled=disabled,
                                 label_visibility="collapsed")
        if c3.button("Burst", key="burst_go", disabled=disabled):
            if claim_cmd("burst", {"store_id": store, "sku": bsku, "units": int(bunits)}):
                st.toast(f"Burst: {bunits} sales on {bsku}")
        st.caption("Scenarios restart the day with new flags.")
        s1, s2, s3, s4 = st.columns(4)
        if s1.button("Promo rush", key="sc_promo", disabled=disabled):
            claim_cmd("scenario", {"flags": {"promo_rush": True}})
        if s2.button("Bulk thrash", key="sc_bulk", disabled=disabled):
            claim_cmd("scenario", {"flags": {"bulk_thrash": True}})
        if s3.button("Silent OOS", key="sc_silent", disabled=disabled):
            claim_cmd("scenario", {"flags": {"silent_oos": True}})
        if s4.button("Receipt spike", key="sc_receipt", disabled=disabled):
            claim_cmd("scenario", {"flags": {"receipt_spike": True}})

    inject_panel()

    @st.fragment(run_every=2)
    def llm_panel():
        with st.expander("LLM calls (eval)", expanded=False):
            calls = q("""SELECT trigger, model, latency_ms, fallback,
                                output->>'rationale' AS rationale,
                                output->>'confidence' AS conf
                         FROM llm_calls ORDER BY id DESC LIMIT 15""")
            if not calls:
                st.caption("No LLM calls yet — exceptions route here.")
            for c in calls:
                pill = "p-rule" if c["fallback"] else "p-llm"
                kind = "FB" if c["fallback"] else "LLM"
                st.markdown(
                    f'<div class="logrow"><span class="pill {pill}>{kind}</span> '
                    f'{"FB" if c["fallback"] else "LLM"}</span> '
                    f'<span class="mono">{c["trigger"]} &middot; {c["latency_ms"]}ms'
                    f' &middot; conf {c["conf"]}</span><br>{c["rationale"]}</div>',
                    unsafe_allow_html=True,
                )

    llm_panel()


@st.fragment(run_every=2)
def footer():
    m = q("""SELECT (SELECT count(*) FROM tasks WHERE store_id=%s) AS tasks,
                    (SELECT count(*) FROM events WHERE store_id=%s AND type='suppress') AS supp,
                    (SELECT count(*) FROM llm_calls) AS llm,
                    (SELECT count(*) FROM tasks WHERE store_id=%s AND status='rejected') AS rej,
                    (SELECT count(*) FROM tasks WHERE store_id=%s AND status='open') AS open""",
            (store, store, store, store))[0]
    st.markdown(
        f'<div class="footer"><div style="max-width:1440px;margin:auto;padding:8px 16px">'
        f"{store} — Tasks fired "
        f'<b class="num">{m["tasks"]}</b> &nbsp;·&nbsp; '
        f'Suppressed <b class="num">{m["supp"]}</b> &nbsp;·&nbsp; '
        f'LLM calls <b class="num">{m["llm"]}</b> (all stores) &nbsp;·&nbsp; '
        f'Rejected <b class="num">{m["rej"]}</b> &nbsp;·&nbsp; '
        f'Open <b class="num">{m["open"]}</b></div></div>',
        unsafe_allow_html=True,
    )


footer()
