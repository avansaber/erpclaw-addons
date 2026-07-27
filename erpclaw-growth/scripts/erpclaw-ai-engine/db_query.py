#!/usr/bin/env python3
"""ERPClaw AI Engine Skill -- db_query.py

AI-powered business analysis: anomaly detection, cash flow forecasting,
business rules, relationship scoring, conversation memory.

Usage: python3 db_query.py --action <action-name> [--flags ...]
Output: JSON to stdout, exit 0 on success, exit 1 on error.
"""
import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

# ---------------------------------------------------------------------------
# Shared library
# ---------------------------------------------------------------------------
try:
    sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
    from erpclaw_lib.db import get_connection, ensure_db_exists, DEFAULT_DB_PATH  # noqa: E402
    from erpclaw_lib.decimal_utils import to_decimal, round_currency  # noqa: E402
    from erpclaw_lib.validation import check_input_lengths  # noqa: E402
    from erpclaw_lib.response import ok, err, row_to_dict
    from erpclaw_lib.audit import audit
    from erpclaw_lib.dependencies import check_required_tables
    from erpclaw_lib.query import Q, P, Table, Field, fn, Case, Order, Criterion, Not, NULL, DecimalSum, DecimalAbs
    from erpclaw_lib.vendor.pypika.terms import LiteralValue, ValueWrapper
    from erpclaw_lib.args import SafeArgumentParser, check_unknown_args
except ImportError:
    import json as _json
    print(_json.dumps({"status": "error", "error": "ERPClaw foundation not installed. Install erpclaw first: clawhub install erpclaw", "suggestion": "clawhub install erpclaw"}))
    sys.exit(1)

REQUIRED_TABLES = ["company", "account"]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_ANOMALY_TYPES = {
    "price_spike", "volume_change", "duplicate_possible", "margin_erosion",
    "unusual_vendor", "pattern_break", "consumption_spike", "late_pattern",
    "round_number", "ghost_employee", "vendor_concentration",
    "sequence_violation", "benford_deviation", "budget_overrun",
    "inventory_shrinkage", "payment_pattern_shift",
    # Wave 1 AI1: asset book-value invariant + dimension-tagging consistency
    "asset_book_value_drift", "dimension_tag_drift",
    # Wave 2 AI1: reservation-vs-stock headroom + subcontract receipt/transfer parity
    "reservation_over_available", "subcontract_receipt_mismatch",
    # Wave F AI1: usage-vs-plan expectations (the spike half of the Wave F pair
    # emits the pre-declared `consumption_spike` type above)
    "rate_plan_mismatch",
}

VALID_SEVERITY = {"info", "warning", "critical"}

VALID_ANOMALY_STATUSES = {
    "new", "acknowledged", "investigated", "dismissed", "resolved",
}

VALID_SCENARIO_TYPES = {
    "price_change", "supplier_loss", "demand_shift", "cost_change",
    "hiring_impact", "expansion", "contraction",
}

VALID_FORECAST_SCENARIOS = {"pessimistic", "expected", "optimistic"}

VALID_RULE_ACTION_TYPES = {"block", "warn", "notify", "auto_execute", "suggest"}

VALID_CONTEXT_TYPES = {
    "active_workflow", "pending_decision", "in_progress_analysis",
}

VALID_DECISION_STATUSES = {"pending", "decided", "expired"}

VALID_STRENGTH = {"weak", "moderate", "strong"}

VALID_CORRELATION_STATUSES = {"new", "validated", "dismissed"}

VALID_PARTY_TYPES = {"customer", "supplier"}

VALID_CREATED_BY = {"user", "ai"}

VALID_SOURCES = {"bank_feed", "ocr_vendor", "email_subject"}

STRENGTH_ORDER = {"weak": 1, "moderate": 2, "strong": 3}

# Severity mapping from plan flags to schema action column
SEVERITY_TO_ACTION = {
    "block": "block",
    "warn": "warn",
    "notify": "notify",
    "critical": "block",
    "warning": "warn",
    "info": "notify",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _parse_json_arg(value, name):
    if not value:
        err(f"--{name} is required")
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        err(f"--{name} must be valid JSON")


def _validate_company(conn, company_id):
    """Validate company exists. Returns company row or calls _err."""
    if not company_id:
        err("--company-id is required")
    t = Table("company")
    q = Q.from_(t).select(t.star).where(t.id == P())
    company = conn.execute(q.get_sql(), (company_id,)).fetchone()
    if not company:
        err(f"Company not found: {company_id}",
             suggestion="Run 'tutorial' to create a demo company, or 'setup company' to create your own.")
    return company


def _insert_anomaly(conn, anomaly_type, severity, entity_type, entity_id,
                    description, evidence, baseline=None, actual=None,
                    deviation_pct=None):
    """Insert an anomaly if not already exists (idempotent). Returns anomaly_id or None."""
    a = Table("anomaly")
    q = (Q.from_(a).select(a.id)
         .where(a.anomaly_type == P())
         .where(a.entity_type == P())
         .where(a.entity_id == P())
         .where(a.status.isin([P(), P()])))
    existing = conn.execute(
        q.get_sql(), (anomaly_type, entity_type, entity_id, 'new', 'acknowledged'),
    ).fetchone()
    if existing:
        return None

    anomaly_id = str(uuid.uuid4())
    q = (Q.into(a)
         .columns("id", "anomaly_type", "severity", "entity_type",
                  "entity_id", "description", "evidence", "baseline", "actual",
                  "deviation_pct", "status")
         .insert(P(), P(), P(), P(), P(), P(), P(), P(), P(), P(), P()))
    conn.execute(
        q.get_sql(),
        (anomaly_id, anomaly_type, severity, entity_type, entity_id,
         description,
         json.dumps(evidence) if isinstance(evidence, dict) else evidence,
         json.dumps(baseline) if isinstance(baseline, dict) else baseline,
         json.dumps(actual) if isinstance(actual, dict) else actual,
         str(deviation_pct) if deviation_pct is not None else None,
         'new'),
    )
    return anomaly_id


def _table_exists(conn, table_name):
    """True if a base table exists (dialect-aware). Guards the Wave 2 inventory
    detectors so detect-anomalies still runs on DBs where the M5/S5 migrations
    (foundation 025/026) have not yet been applied — the detector simply
    contributes zero anomalies instead of failing the whole sweep. table_name is
    an internal literal, never user input."""
    if os.environ.get("ERPCLAW_DB_DIALECT", "sqlite") == "postgresql":
        row = conn.execute("SELECT to_regclass(?)", (table_name,)).fetchone()
        return bool(row) and row[0] is not None
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _detect_reservation_over_available(conn, company_id, from_date, to_date):
    """Wave 2 AI1 detector: reservation_over_available.

    Flags any (item, warehouse) whose SUM of ACTIVE stock_reservation_entry
    reserved_qty exceeds the on-hand balance (SUM of SLE actual_qty). When
    reserved > on-hand the reservations cannot all be fulfilled -> a stock-out is
    predicted. Reads M5's stock_reservation_entry (owned by erpclaw-inventory) +
    the shared stock_ledger_entry; WRITES only growth's anomaly table via
    _insert_anomaly. Point-in-time state check (reservations carry no posting
    window), so it ignores the date range. Returns a list of
    {"id","type","severity"} for each inserted anomaly.

    All quantities are Decimal-as-text; comparison + arithmetic use Decimal.
    """
    emitted = []
    if not _table_exists(conn, "stock_reservation_entry"):
        return emitted

    # Active reservations for this company, grouped in Python (dialect-safe: no
    # GROUP BY / decimal aggregate in SQL). Company scope via the reservation's
    # own company_id column.
    res_rows = conn.execute(
        "SELECT item_id, warehouse_id, reserved_qty "
        "FROM stock_reservation_entry "
        "WHERE company_id = ? AND status = 'active'",
        (company_id,),
    ).fetchall()

    reserved_by_pair = {}
    for r in res_rows:
        d = row_to_dict(r)
        key = (d["item_id"], d["warehouse_id"])
        reserved_by_pair[key] = reserved_by_pair.get(key, Decimal("0")) + to_decimal(
            str(d["reserved_qty"] or "0"))

    for (item_id, warehouse_id), reserved in sorted(reserved_by_pair.items()):
        if reserved <= 0:
            continue
        # On-hand = SUM of non-cancelled SLE actual_qty for the pair. Summed in
        # Python from raw rows (dialect-safe).
        sle_rows = conn.execute(
            "SELECT actual_qty FROM stock_ledger_entry "
            "WHERE item_id = ? AND warehouse_id = ? AND is_cancelled = 0",
            (item_id, warehouse_id),
        ).fetchall()
        on_hand = sum((to_decimal(str(s["actual_qty"] or "0")) for s in sle_rows),
                      Decimal("0"))
        if reserved <= on_hand:
            continue  # enough stock on hand to cover the reservations

        shortfall = round_currency(reserved - on_hand)
        # Fraction of the reserved qty that cannot be covered.
        coverage_gap = (shortfall / reserved * Decimal("100")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP)
        # Nothing on hand at all is the more severe stock-out signal.
        severity = "critical" if on_hand <= 0 else "warning"
        aid = _insert_anomaly(
            conn, "reservation_over_available", severity,
            "item", f"{item_id}:{warehouse_id}",
            f"Active reservations ({reserved}) exceed on-hand stock ({round_currency(on_hand)}) "
            f"for item {item_id} in warehouse {warehouse_id}: short by {shortfall} "
            f"({coverage_gap}% of reservations unfulfillable).",
            {"company_id": company_id, "item_id": item_id,
             "warehouse_id": warehouse_id, "shortfall": str(shortfall)},
            baseline={"on_hand_qty": str(round_currency(on_hand))},
            actual={"reserved_qty": str(round_currency(reserved))},
            deviation_pct=str(coverage_gap),
        )
        if aid:
            emitted.append({"id": aid, "type": "reservation_over_available",
                            "severity": severity})
    return emitted


# Fractional tolerance (percent) for the subcontract receipt/transfer parity check.
SUBCONTRACT_MISMATCH_TOLERANCE_PCT = Decimal("5")


def _detect_subcontract_receipt_mismatch(conn, company_id, from_date, to_date):
    """Wave 2 AI1 detector: subcontract_receipt_mismatch.

    Flags a subcontracting_order whose finished-goods received_qty diverges from
    the materials_transferred (both are FG-unit measures in S5) beyond
    SUBCONTRACT_MISMATCH_TOLERANCE_PCT. The hard validation only caps cumulative
    received at the order qty; it does NOT enforce received <= transferred, so
    receiving more FG than the transferred raw materials could yield (phantom
    production) slips through — that over-receipt is the primary signal here. For
    a 'completed' order the reverse divergence (materials shipped materially
    exceed FG returned = yield loss / missing goods) is also flagged. Reads S5's
    subcontracting_order (owned by erpclaw-manufacturing); WRITES only growth's
    anomaly table via _insert_anomaly. Point-in-time state check (orders carry no
    posting window), so it ignores the date range. Returns a list of
    {"id","type","severity"} for each inserted anomaly.

    All quantities are Decimal-as-text; comparison + arithmetic use Decimal.
    """
    emitted = []
    if not _table_exists(conn, "subcontracting_order"):
        return emitted

    rows = conn.execute(
        "SELECT id, status, materials_transferred, received_qty "
        "FROM subcontracting_order "
        "WHERE company_id = ? AND status IN ('partially_received', 'completed')",
        (company_id,),
    ).fetchall()

    for row in rows:
        o = row_to_dict(row)
        transferred = to_decimal(str(o["materials_transferred"] or "0"))
        received = to_decimal(str(o["received_qty"] or "0"))
        if transferred <= 0 or received <= 0:
            continue  # no comparable activity on both sides yet

        divergence = received - transferred
        divergence_pct = (divergence / transferred * Decimal("100")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP)

        if received > transferred and divergence_pct > SUBCONTRACT_MISMATCH_TOLERANCE_PCT:
            # Over-receipt: more FG received than the transferred materials support.
            aid = _insert_anomaly(
                conn, "subcontract_receipt_mismatch", "critical",
                "subcontracting_order", o["id"],
                f"Subcontract received qty ({received}) exceeds materials "
                f"transferred ({transferred}) by {divergence_pct}% "
                f"(over tolerance {SUBCONTRACT_MISMATCH_TOLERANCE_PCT}%): more finished "
                f"goods received than the transferred materials can yield.",
                {"company_id": company_id, "subcontracting_order_id": o["id"],
                 "direction": "over_receipt"},
                baseline={"materials_transferred": str(round_currency(transferred))},
                actual={"received_qty": str(round_currency(received))},
                deviation_pct=str(divergence_pct),
            )
            if aid:
                emitted.append({"id": aid, "type": "subcontract_receipt_mismatch",
                                "severity": "critical"})
        elif o["status"] == "completed":
            shortfall_pct = (-divergence / transferred * Decimal("100")).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP)
            if shortfall_pct > SUBCONTRACT_MISMATCH_TOLERANCE_PCT:
                # Completed order where transferred materials materially exceed the
                # FG returned: yield loss / unaccounted materials.
                aid = _insert_anomaly(
                    conn, "subcontract_receipt_mismatch", "warning",
                    "subcontracting_order", o["id"],
                    f"Completed subcontract received qty ({received}) is "
                    f"{shortfall_pct}% below materials transferred ({transferred}) "
                    f"(over tolerance {SUBCONTRACT_MISMATCH_TOLERANCE_PCT}%): possible "
                    f"yield loss or unaccounted materials.",
                    {"company_id": company_id, "subcontracting_order_id": o["id"],
                     "direction": "under_receipt"},
                    baseline={"materials_transferred": str(round_currency(transferred))},
                    actual={"received_qty": str(round_currency(received))},
                    deviation_pct=str(shortfall_pct),
                )
                if aid:
                    emitted.append({"id": aid, "type": "subcontract_receipt_mismatch",
                                    "severity": "warning"})
    return emitted


# Wave F AI1 usage-anomaly thresholds (OVERVIEW AI1 table, Wave F row).
USAGE_SPIKE_MULTIPLIER = Decimal("3")   # window avg > N x historical baseline avg
USAGE_MIN_BASELINE_READINGS = 3         # too few pre-window readings = no baseline
USAGE_DEFAULT_WINDOW_READINGS = 3       # default sweep: last N readings = window

# rate_plan types whose rate_tier rows are CUMULATIVE VOLUME BANDS — the only
# tier shape where max(tier_end) is a hard per-billing-period consumption
# ceiling. Exactly the set erpclaw-billing's _calculate_charge walks tier-by-
# tier (2026-07-25 QA bounce #2, DEFECT-B): 'flat' prices ALL volume at one
# rate (a closed tier_end never meant a ceiling), and for
# time_of_use/demand/hybrid the tier rows are time/demand BANDS each with its
# own cap, so max(tier_end) falsely accuses a customer whose usage is within
# every band. Per-band ceilings for TOU/demand/hybrid arrive with S1.1-rated
# data, not this heuristic.
VOLUME_BAND_PLAN_TYPES = ("tiered", "volume_discount")

# A rate_tier.tier_end at or beyond this magnitude is treated as garbage data,
# not a real per-billing-period volume ceiling (no utility bills 10^15 units in
# one period). add-rate-plan stores tier_end unvalidated, so absurd values are
# reachable through shipped actions; the ceiling heuristic skips such plans
# instead of "judging" against a nonsense ceiling (2026-07-26 QA round 3,
# DEFECT-D).
USAGE_CEILING_SANITY_MAX = Decimal("1E15")


def _reading_day(value):
    """Normalize a meter_reading.reading_date to its 10-char date part.

    add-meter-reading stores --reading-date verbatim (no format validation),
    so a value like '2026-03-31 08:00:00' or '2026-03-31T08:00' is reachable
    through the shipped action. Raw string comparison against 'YYYY-MM-DD'
    window bounds would silently drop such readings from BOTH the baseline and
    the window (2026-07-25 QA bounce, MEDIUM defect) — every detector-side
    date comparison goes through this normalization instead.
    """
    return str(value)[:10]


def _sane_consumption(raw):
    """Parse a consumption/money-like stored figure defensively, or None.

    add-meter-reading and legacy rows store these figures unvalidated, so
    non-finite ('NaN') and finite-but-absurd magnitudes (>= 1E26 survives
    is_finite() yet overflows the default 28-digit decimal context in the
    emission arithmetic) are reachable in stored data. Treating such a
    figure as real aborted the WHOLE company sweep in all four emission
    branches (2026-07-27 QA round 4, DEFECT-E). No real consumption or
    charge approaches USAGE_CEILING_SANITY_MAX (1E15) — beyond it the row
    is garbage data and the detector skips exactly that row. Never raises.
    """
    try:
        val = to_decimal(str(raw))
    except (ValueError, TypeError, ArithmeticError):
        return None
    if not val.is_finite() or abs(val) >= USAGE_CEILING_SANITY_MAX:
        return None
    return val


def _plan_volume_ceiling(conn, rate_plan_id, cache):
    """Resolve a plan's hard per-billing-period volume ceiling, or None.

    Returns ``(plan_name, ceiling Decimal)`` only when ALL hold: the plan
    exists; its plan_type is in VOLUME_BAND_PLAN_TYPES (cumulative volume
    bands — see the constant's rationale, 2026-07-25 QA bounce #2 DEFECT-B);
    it has at least one rate_tier and every tier_end is closed AND sane;
    and the resulting max(tier_end) is positive. Otherwise None — the plan
    defines no usable volume ceiling and the mismatch heuristic must stay
    silent. ``cache`` memoizes per sweep (dict keyed by rate_plan_id).

    tier_end containment (2026-07-26 QA round 3, DEFECT-D — add-rate-plan
    stores tier_end unvalidated, and one bad row must NEVER abort the whole
    company sweep):
      - NULL or blank/whitespace-only -> OPEN-ENDED (no ceiling): a blank
        upper limit is the natural NL-authored "no upper limit", and it is
        exactly how erpclaw-billing's _calculate_charge already treats it
        (truthy check on tier_end) — engine-consistent, not invented here.
      - unparseable ('abc'), non-finite ('NaN'/'sNaN'/'Infinity'), or
        absurd-magnitude (>= USAGE_CEILING_SANITY_MAX) -> the plan's tier
        data is garbage; skip the ceiling heuristic for THIS plan and let
        the sweep continue. Never raise out of this helper.
    """
    if rate_plan_id in cache:
        return cache[rate_plan_id]
    result = None
    plan_row = conn.execute(
        "SELECT id, name, plan_type FROM rate_plan WHERE id = ?",
        (rate_plan_id,),
    ).fetchone()
    if plan_row:
        plan = row_to_dict(plan_row)
        if plan["plan_type"] in VOLUME_BAND_PLAN_TYPES:
            tier_rows = conn.execute(
                "SELECT tier_end FROM rate_tier WHERE rate_plan_id = ?",
                (rate_plan_id,),
            ).fetchall()
            raw_ends = [row_to_dict(t)["tier_end"] for t in tier_rows]
            ends = []
            usable = bool(raw_ends)
            for raw in raw_ends:
                if raw is None or not str(raw).strip():
                    usable = False      # open-ended tier -> no ceiling
                    break
                try:
                    val = to_decimal(str(raw).strip())
                except (ValueError, TypeError, ArithmeticError):
                    usable = False      # garbage tier_end -> skip this plan
                    break
                if not val.is_finite() or abs(val) >= USAGE_CEILING_SANITY_MAX:
                    usable = False      # non-finite/absurd -> skip this plan
                    break
                ends.append(val)
            if usable and ends:
                ceiling = max(ends)
                if ceiling > 0:
                    result = (plan["name"], ceiling)
    cache[rate_plan_id] = result
    return result


def _plan_pricing_rows(conn, rate_plan_id, cache):
    """Fetch (plan_type, tier rows) for pricing math, memoized per sweep.

    Shared by _stored_plan_usage_charge and _plan_max_billable so a plan's
    tiers are read once per sweep regardless of how many periods cite it.
    """
    if rate_plan_id in cache:
        return cache[rate_plan_id]
    plan_type, tiers = None, []
    plan_row = conn.execute(
        "SELECT plan_type FROM rate_plan WHERE id = ?",
        (rate_plan_id,),
    ).fetchone()
    if plan_row:
        plan_type = row_to_dict(plan_row)["plan_type"]
        tier_rows = conn.execute(
            "SELECT tier_start, tier_end, rate FROM rate_tier "
            "WHERE rate_plan_id = ? ORDER BY sort_order",
            (rate_plan_id,),
        ).fetchall()
        tiers = [row_to_dict(t) for t in tier_rows]
    cache[rate_plan_id] = (plan_type, tiers)
    return plan_type, tiers


def _plan_max_billable(conn, rate_plan_id, ceiling, cache):
    """The most the plan's CURRENT tiers can bill in one period, or None.

    The attribution discriminator (2026-07-27 QA round 4, DEFECT-F): a
    stored usage_charge ABOVE this maximum is impossible under the stored
    plan — some other plan priced the period. A charge at or below it is
    explainable under the stored plan (including charges produced by since-
    raised rates), so the over-ceiling accusation stands.

    tiered: the full walk at the ceiling — charge is nondecreasing in
    consumption and flat beyond the top closed band, so walk(ceiling) is
    the maximum. volume_discount: charge = matched band's rate x total
    consumption, so the maximum over legal consumption is per-band —
    max over closed bands of round_currency(min(tier_end, ceiling) x rate).
    Returns None when no finite maximum exists (open band, garbage tier
    data, non-volume-band type). Never raises.
    """
    plan_type, tiers = _plan_pricing_rows(conn, rate_plan_id, cache)
    if plan_type not in VOLUME_BAND_PLAN_TYPES or not tiers:
        return None
    if not isinstance(ceiling, Decimal):
        return None
    try:
        if plan_type == "tiered":
            return _stored_plan_usage_charge(
                conn, rate_plan_id, ceiling, cache)
        best = Decimal("0")
        for tier in tiers:
            tier_end_val = tier.get("tier_end")
            if tier_end_val is None or not str(tier_end_val).strip():
                return None      # open band -> no finite maximum
            cap = min(to_decimal(str(tier_end_val).strip()), ceiling)
            charge = round_currency(cap * to_decimal(tier.get("rate", "0")))
            if charge > best:
                best = charge
        return best
    except (ValueError, TypeError, ArithmeticError):
        return None


def _stored_plan_usage_charge(conn, rate_plan_id, consumption, cache):
    """What the STORED plan's tiers would have charged for ``consumption``.

    Exact mirror of erpclaw-billing _calculate_charge's usage-charge leg for
    the volume-band types (tiered / volume_discount): tiers sorted by
    tier_start, per-tier round_currency, TRUTHY tier_end -> open-ended —
    byte-for-byte the arithmetic run-billing uses, so the recomputation
    agrees with a stored billing_period.usage_charge if and only if that
    stored plan's tiers actually produced it (usage_charge is independent of
    base_charge / minimum_charge, which only touch the period total).
    Read-only cross-module read of rate_plan/rate_tier (allowed).

    Returns a Decimal, or None when the charge cannot be recomputed (unknown
    plan, non-volume-band type, no tiers, or garbage tier data). NEVER
    raises — one bad tier row must not abort the company sweep (2026-07-26
    QA round 3). ``cache`` memoizes the fetched tier rows per plan.
    """
    plan_type, tiers = _plan_pricing_rows(conn, rate_plan_id, cache)
    if plan_type not in VOLUME_BAND_PLAN_TYPES or not tiers:
        return None
    try:
        sorted_tiers = sorted(
            tiers, key=lambda t: to_decimal(t.get("tier_start", "0")))
        if plan_type == "tiered":
            usage_charge = Decimal("0")
            remaining = consumption
            for tier in sorted_tiers:
                if remaining <= 0:
                    break
                tier_start = to_decimal(tier.get("tier_start", "0"))
                tier_end_val = tier.get("tier_end")
                tier_end = to_decimal(tier_end_val) if tier_end_val else None
                rate = to_decimal(tier.get("rate", "0"))
                band_width = (tier_end - tier_start) if tier_end else remaining
                applicable = min(remaining, band_width)
                usage_charge += round_currency(applicable * rate)
                remaining -= applicable
            return usage_charge
        # volume_discount: the single matched band's rate on ALL consumption.
        applicable_rate = Decimal("0")
        for tier in sorted_tiers:
            tier_start = to_decimal(tier.get("tier_start", "0"))
            tier_end_val = tier.get("tier_end")
            tier_end = to_decimal(tier_end_val) if tier_end_val else None
            if consumption >= tier_start and (
                    tier_end is None or consumption < tier_end):
                applicable_rate = to_decimal(tier.get("rate", "0"))
                break
        return round_currency(consumption * applicable_rate)
    except (ValueError, TypeError, ArithmeticError):
        return None


def _detect_usage_anomaly(conn, company_id, from_date, to_date,
                          window_is_explicit=False):
    """Wave F AI1 detector: consumption_spike + rate_plan_mismatch.

    Per OVERVIEW's AI1 table (Wave F row), two related usage anomalies on the
    foundation billing substrate (meters / readings / rate plans):

      1. consumption_spike — a meter's recent average per-reading consumption
         strictly exceeds USAGE_SPIKE_MULTIPLIER x its own historical baseline
         (needing >= USAGE_MIN_BASELINE_READINGS baseline readings so a single
         old reading never anchors a baseline). The window/baseline split
         depends on how the sweep was invoked (2026-07-25 QA bounce, HIGH 1 —
         the sweep window must not double as the baseline split point):
           - window_is_explicit=True (user passed --from-date): window =
             readings in [from_date, to_date]; baseline = all readings before
             from_date.
           - default sweep (no --from-date): meter-local recency split —
             window = the meter's last USAGE_DEFAULT_WINDOW_READINGS readings
             dated <= to_date; baseline = all earlier readings. Without this,
             the default from_date sentinel swallows every reading into the
             window and the baseline is always empty (type never emitted).
         All threshold comparisons are cross-multiplied (window_total * n_base
         vs baseline_total * n_win * MULT) so they are EXACT — an exactly-Nx
         window must not fire the strict > threshold (QA bounce, LOW defect);
         division appears only in display figures.
      2. rate_plan_mismatch — usage exceeds what the meter's assigned rate
         plan allows/defines ("customer using more than plan allows"):
           - prepaid_credit plans: the customer's prepaid_credit_balance for
             that plan is exhausted or carries overage (critical when actual
             overage has accrued, warning when merely exhausted);
           - volume-banded plans (VOLUME_BAND_PLAN_TYPES — exactly the tier
             shapes _calculate_charge walks cumulatively: tiered,
             volume_discount) where every rate_tier has a closed tier_end:
             the plan defines a hard consumption ceiling. Other plan types
             never fire this heuristic — for time_of_use/demand/hybrid the
             tiers are per-band caps, not a volume ceiling (2026-07-25 QA
             bounce #2, DEFECT-B). The ceiling is PER BILLING PERIOD (proven
             by erpclaw-billing's _calculate_charge, which walks tiers
             against ONE period's total_consumption), so the comparison is
             per-period, never against a whole-sweep-window sum (2026-07-25
             QA bounce, HIGH 2 — the old sum falsely accused fully compliant
             customers):
               * meter has billing_period rows (status != 'void') overlapping
                 the sweep window -> evaluate each period AGAINST ITS OWN
                 PLAN (billing_period.rate_plan_id, the plan the period
                 was/will be rated under — never the meter's current plan; a
                 plan change must not re-judge rated history, 2026-07-25 QA
                 bounce #2, DEFECT-A). 'open' periods: consumption = SUM of
                 readings dated within the period (usage_events omitted ->
                 undercount only, conservative); rated/invoiced/paid/disputed
                 periods: the stored total_consumption (authoritative — what
                 run-billing actually accounted), PLUS the attribution guard
                 (2026-07-26 QA round 3, DEFECT-C): accuse only if the stored
                 usage_charge agrees EXACTLY with what the stored plan's
                 tiers would have charged for that consumption
                 (_stored_plan_usage_charge, a mirror of run-billing's
                 arithmetic) — a mid-cycle plan change can leave a terminal
                 row whose rate_plan_id names a plan that never priced it,
                 and a period we cannot honestly attribute is never accused.
                 Entity = ('billing_period', period_id).
               * no billing_period rows -> single-reading lower bound against
                 the METER's current plan (unbilled readings will be rated
                 under it): fire only when ONE reading's consumption already
                 exceeds the ceiling (run-billing aggregates a reading into
                 exactly one period by reading_date, so a single reading
                 lower-bounds its period's total). Entity =
                 ('meter_reading', reading_id). With no period info and no
                 single over-ceiling reading, stay silent — never accuse on
                 a guessed accounting period.
             Open-ended plans (any NULL tier_end) never fire.

    Reads foundation billing tables (meter, meter_reading, rate_plan,
    rate_tier, billing_period, prepaid_credit_balance — cross-module READ,
    allowed); WRITES only growth's anomaly table via _insert_anomaly. Company
    scope rides meter -> customer.company_id. Grouping/averaging is done in
    Python from raw rows (dialect-safe, matching the Wave 2 detectors); all
    quantities are Decimal-as-text and every comparison uses Decimal. Returns
    a list of {"id","type","severity"} for each inserted anomaly.
    """
    emitted = []
    for required in ("meter", "meter_reading", "rate_plan"):
        if not _table_exists(conn, required):
            return emitted

    # Active meters for this company. Raw SQL — simple two-table join with
    # internal literals only, mirroring the Wave 2 detector style.
    meters = conn.execute(
        "SELECT m.id, m.meter_number, m.customer_id, m.rate_plan_id "
        "FROM meter m JOIN customer c ON m.customer_id = c.id "
        "WHERE c.company_id = ? AND m.status = 'active'",
        (company_id,),
    ).fetchall()

    # Per-sweep memos: _plan_volume_ceiling results and _stored_plan_usage_charge
    # tier rows (plans are shared across meters and periods).
    plan_ceilings = {}
    plan_pricing = {}

    for m_row in meters:
        m = row_to_dict(m_row)
        try:

            # All usable readings up to the sweep end, oldest first. Dates are
            # normalized per _reading_day; the raw-SQL ORDER BY stays monotonic
            # w.r.t. the normalized day (lexicographic prefix), with created_at/id
            # as deterministic tiebreak within a day.
            rows = conn.execute(
                "SELECT id, reading_date, consumption FROM meter_reading "
                "WHERE meter_id = ? AND consumption IS NOT NULL "
                "ORDER BY reading_date, created_at, id",
                (m["id"],),
            ).fetchall()
            readings = []
            for r in rows:
                rd = row_to_dict(r)
                day = _reading_day(rd["reading_date"])
                if day > to_date:
                    continue
                consumption = _sane_consumption(rd["consumption"] or "0")
                if consumption is None:
                    continue    # garbage figure: skip the row, never the sweep
                readings.append({"id": rd["id"], "day": day,
                                 "consumption": consumption})

            # --- 1. consumption_spike: window avg strictly > N x baseline avg ---
            if window_is_explicit:
                # User-declared window: examine it against everything before it.
                split = 0
                while split < len(readings) and readings[split]["day"] < from_date:
                    split += 1
                baseline, window = readings[:split], readings[split:]
            else:
                # Default sweep: latest readings vs their own history.
                n_recent = min(len(readings), USAGE_DEFAULT_WINDOW_READINGS)
                baseline = readings[:len(readings) - n_recent]
                window = readings[len(readings) - n_recent:]

            n_base, n_win = len(baseline), len(window)
            if n_base >= USAGE_MIN_BASELINE_READINGS and n_win:
                baseline_total = sum((r["consumption"] for r in baseline), Decimal("0"))
                window_total = sum((r["consumption"] for r in window), Decimal("0"))
                # Exact cross-multiplied threshold: no divide-then-multiply
                # rounding, so an exactly-Nx window never fires the strict >.
                lhs = window_total * n_base
                rhs = baseline_total * n_win
                if (baseline_total > 0
                        and lhs > rhs * USAGE_SPIKE_MULTIPLIER):
                    severity = ("critical"
                                if lhs > rhs * USAGE_SPIKE_MULTIPLIER * 2
                                else "warning")
                    # Display-only figures (the comparison above is exact).
                    baseline_avg = baseline_total / Decimal(n_base)
                    window_avg = window_total / Decimal(n_win)
                    ratio = (lhs / rhs).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP)
                    deviation_pct = ((lhs - rhs) / rhs * Decimal("100")).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP)
                    aid = _insert_anomaly(
                        conn, "consumption_spike", severity,
                        "meter", m["id"],
                        f"Meter {m['meter_number']} consumption spike: window "
                        f"average {round_currency(window_avg)} per reading is "
                        f"{ratio}x the historical baseline "
                        f"{round_currency(baseline_avg)} (fires strictly above "
                        f"{USAGE_SPIKE_MULTIPLIER}x).",
                        {"company_id": company_id, "meter_id": m["id"],
                         "customer_id": m["customer_id"],
                         "window_readings": n_win,
                         "baseline_readings": n_base,
                         "window_mode": ("explicit" if window_is_explicit
                                         else "recent_readings")},
                        baseline={"baseline_avg_consumption": str(round_currency(baseline_avg))},
                        actual={"window_avg_consumption": str(round_currency(window_avg))},
                        deviation_pct=str(deviation_pct),
                    )
                    if aid:
                        emitted.append({"id": aid, "type": "consumption_spike",
                                        "severity": severity})

            # --- 2. rate_plan_mismatch: usage beyond what the plan allows ---
            if not m["rate_plan_id"]:
                continue
            plan_row = conn.execute(
                "SELECT id, name, plan_type FROM rate_plan WHERE id = ?",
                (m["rate_plan_id"],),
            ).fetchone()
            if not plan_row:
                continue
            plan = row_to_dict(plan_row)

            if plan["plan_type"] == "prepaid_credit":
                if not _table_exists(conn, "prepaid_credit_balance"):
                    continue
                balances = conn.execute(
                    "SELECT id, remaining_amount, overage_amount, status "
                    "FROM prepaid_credit_balance "
                    "WHERE customer_id = ? AND rate_plan_id = ? "
                    "AND status IN ('active', 'exhausted')",
                    (m["customer_id"], plan["id"]),
                ).fetchall()
                for b_row in balances:
                    b = row_to_dict(b_row)
                    overage = to_decimal(str(b["overage_amount"] or "0"))
                    exhausted = b["status"] == "exhausted"
                    if overage <= 0 and not exhausted:
                        continue
                    severity = "critical" if overage > 0 else "warning"
                    aid = _insert_anomaly(
                        conn, "rate_plan_mismatch", severity,
                        "prepaid_credit_balance", b["id"],
                        f"Meter {m['meter_number']} usage exceeds prepaid plan "
                        f"'{plan['name']}': balance "
                        + (f"overage ${round_currency(overage)} accrued"
                           if overage > 0 else "exhausted")
                        + " — customer is consuming beyond the plan's allowance.",
                        {"company_id": company_id, "meter_id": m["id"],
                         "customer_id": m["customer_id"],
                         "rate_plan_id": plan["id"],
                         "balance_status": b["status"],
                         "overage_amount": str(round_currency(overage))},
                        baseline={"remaining_amount": str(round_currency(
                            to_decimal(str(b["remaining_amount"] or "0"))))},
                        actual={"overage_amount": str(round_currency(overage))},
                    )
                    if aid:
                        emitted.append({"id": aid, "type": "rate_plan_mismatch",
                                        "severity": severity})
            else:
                if not _table_exists(conn, "rate_tier"):
                    continue

                # The tier ceiling is PER BILLING PERIOD — never compare it to a
                # whole-sweep-window sum (2026-07-25 QA bounce, HIGH 2).
                periods = []
                if _table_exists(conn, "billing_period"):
                    periods = conn.execute(
                        "SELECT id, period_start, period_end, total_consumption, "
                        "usage_charge, status, rate_plan_id FROM billing_period "
                        "WHERE meter_id = ? AND status != 'void' "
                        "AND period_start <= ? AND period_end >= ?",
                        (m["id"], to_date, from_date),
                    ).fetchall()
                if periods:
                    for p_row in periods:
                        p = row_to_dict(p_row)
                        # Judge the period against ITS OWN plan
                        # (billing_period.rate_plan_id — the plan it was/will be
                        # rated under, the same per-plan scoping the prepaid
                        # branch above applies to balances). A meter plan change
                        # must never re-judge already-rated history (2026-07-25
                        # QA bounce #2, DEFECT-A).
                        ceiling_info = _plan_volume_ceiling(
                            conn, p["rate_plan_id"], plan_ceilings)
                        if ceiling_info is None:
                            continue
                        period_plan_name, ceiling = ceiling_info
                        if p["status"] == "open":
                            # Not yet rated: best-available per-period figure from
                            # the readings themselves (normalized dates; omitting
                            # usage_events can only under-count -> conservative).
                            period_consumption = sum(
                                (r["consumption"] for r in readings
                                 if p["period_start"] <= r["day"] <= p["period_end"]),
                                Decimal("0"))
                        else:
                            # rated/invoiced/paid/disputed: what run-billing
                            # actually accounted for this period. Garbage stored
                            # totals (non-finite / absurd magnitude) skip THIS
                            # period, never the sweep (QA round 4, DEFECT-E).
                            period_consumption = _sane_consumption(
                                p["total_consumption"] or "0")
                            if period_consumption is None:
                                continue
                        if period_consumption <= ceiling:
                            continue
                        if p["status"] != "open":
                            # ATTRIBUTION GUARD (QA round 3 DEFECT-C, reshaped in
                            # round 4 for DEFECT-F): run-billing's open-period
                            # UPDATE re-rates under the meter's CURRENT plan
                            # without rewriting the period's rate_plan_id, so a
                            # terminal row can claim plan X while plan Y priced
                            # it (mid-cycle upgrade). Exact charge-agreement was
                            # the wrong test — a routine update-rate-plan
                            # re-price changes the recomputation and permanently
                            # silenced TRUE accusations (DEFECT-F). The only
                            # attribution claim the stored data supports: a
                            # stored usage_charge EXCEEDING the most this plan
                            # can bill at its own ceiling is impossible under
                            # the stored plan — some other plan priced the
                            # period, stay silent. At or below that maximum the
                            # charge is explainable under the stored plan and
                            # the accusation stands. Known epistemic bound
                            # (recorded in the sim log): after a rate DECREASE a
                            # historical charge may exceed the new maximum and
                            # be silenced — undecidable without rate history;
                            # lane A's rate_plan_id stamping ends the lying-row
                            # supply at the source.
                            stored_charge = _sane_consumption(
                                p["usage_charge"] or "0")
                            max_billable = _plan_max_billable(
                                conn, p["rate_plan_id"], ceiling, plan_pricing)
                            if (stored_charge is None or max_billable is None
                                    or stored_charge > max_billable):
                                continue
                        over_pct = ((period_consumption - ceiling) / ceiling
                                    * Decimal("100")).quantize(
                            Decimal("0.01"), rounding=ROUND_HALF_UP)
                        aid = _insert_anomaly(
                            conn, "rate_plan_mismatch", "warning",
                            "billing_period", p["id"],
                            f"Meter {m['meter_number']} billing period "
                            f"{p['period_start']}..{p['period_end']} consumption "
                            f"{round_currency(period_consumption)} exceeds its "
                            f"rate plan '{period_plan_name}' top tier ceiling "
                            f"{round_currency(ceiling)} by {over_pct}% — the plan "
                            f"defines no pricing for that usage.",
                            {"company_id": company_id, "meter_id": m["id"],
                             "customer_id": m["customer_id"],
                             "rate_plan_id": p["rate_plan_id"],
                             "billing_period_id": p["id"],
                             "period_start": p["period_start"],
                             "period_end": p["period_end"],
                             "period_status": p["status"]},
                            baseline={"plan_ceiling": str(round_currency(ceiling))},
                            actual={"period_consumption": str(round_currency(period_consumption))},
                            deviation_pct=str(over_pct),
                        )
                        if aid:
                            emitted.append({"id": aid, "type": "rate_plan_mismatch",
                                            "severity": "warning"})
                else:
                    # No billing periods for this meter: the only provable claim
                    # is the single-reading lower bound (one reading bills into
                    # exactly one period, so it lower-bounds that period's total),
                    # judged against the METER's current plan — the plan unbilled
                    # readings will be rated under. Same volume-band restriction
                    # (2026-07-25 QA bounce #2, DEFECT-B).
                    ceiling_info = _plan_volume_ceiling(
                        conn, plan["id"], plan_ceilings)
                    if ceiling_info is None:
                        continue
                    _, ceiling = ceiling_info
                    worst = None
                    for r in readings:
                        if not (from_date <= r["day"] <= to_date):
                            continue
                        if worst is None or r["consumption"] > worst["consumption"]:
                            worst = r
                    if worst is None or worst["consumption"] <= ceiling:
                        continue
                    over_pct = ((worst["consumption"] - ceiling) / ceiling
                                * Decimal("100")).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP)
                    aid = _insert_anomaly(
                        conn, "rate_plan_mismatch", "warning",
                        "meter_reading", worst["id"],
                        f"Meter {m['meter_number']} single reading on "
                        f"{worst['day']} consumed "
                        f"{round_currency(worst['consumption'])}, already beyond "
                        f"rate plan '{plan['name']}' per-billing-period top tier "
                        f"ceiling {round_currency(ceiling)} by {over_pct}% — the "
                        f"plan defines no pricing for that usage.",
                        {"company_id": company_id, "meter_id": m["id"],
                         "customer_id": m["customer_id"],
                         "rate_plan_id": plan["id"],
                         "meter_reading_id": worst["id"],
                         "reading_date": worst["day"]},
                        baseline={"plan_ceiling": str(round_currency(ceiling))},
                        actual={"reading_consumption": str(round_currency(worst["consumption"]))},
                        deviation_pct=str(over_pct),
                    )
                    if aid:
                        emitted.append({"id": aid, "type": "rate_plan_mismatch",
                                        "severity": "warning"})
        except ArithmeticError as exc:
            # Defense-in-depth (QA round 4, DEFECT-E class): input
            # sanitization above is the primary guard, but NO residual
            # decimal explosion in one meter's figures may ever kill the
            # company sweep — contain, note, continue (mirrors the
            # billing lane's per-meter ArithmeticError containment).
            print(
                f"[erpclaw-ai-engine] usage-anomaly: meter {m.get('id')} "
                f"skipped ({exc.__class__.__name__}: {exc})",
                file=sys.stderr,
            )
            continue
    return emitted


# ============================================================================
# ACTION IMPLEMENTATIONS
# ============================================================================

# ---------------------------------------------------------------------------
# Anomaly Management (actions 3, 4)
# ---------------------------------------------------------------------------

def acknowledge_anomaly(conn, args):
    """Mark anomaly as acknowledged."""
    if not args.anomaly_id:
        err("--anomaly-id is required")

    a = Table("anomaly")
    q = Q.from_(a).select(a.star).where(a.id == P())
    anomaly = conn.execute(q.get_sql(), (args.anomaly_id,)).fetchone()
    if not anomaly:
        err(f"Anomaly not found: {args.anomaly_id}")

    if anomaly["status"] not in ("new",):
        err(f"Cannot acknowledge anomaly in status: {anomaly['status']}")

    q = Q.update(a).set(a.status, P()).where(a.id == P())
    conn.execute(q.get_sql(), ('acknowledged', args.anomaly_id))
    audit(conn, "erpclaw-ai-engine", "acknowledge-anomaly", "anomaly", args.anomaly_id)
    conn.commit()

    q = Q.from_(a).select(a.star).where(a.id == P())
    updated = row_to_dict(conn.execute(q.get_sql(), (args.anomaly_id,)).fetchone())
    ok({"anomaly": updated})


def dismiss_anomaly(conn, args):
    """Dismiss anomaly as false positive."""
    if not args.anomaly_id:
        err("--anomaly-id is required")

    a = Table("anomaly")
    q = Q.from_(a).select(a.star).where(a.id == P())
    anomaly = conn.execute(q.get_sql(), (args.anomaly_id,)).fetchone()
    if not anomaly:
        err(f"Anomaly not found: {args.anomaly_id}")

    if anomaly["status"] in ("dismissed", "resolved"):
        err(f"Anomaly already in terminal status: {anomaly['status']}")

    q = (Q.update(a)
         .set(a.status, P())
         .set(a.resolution_notes, P())
         .where(a.id == P()))
    conn.execute(q.get_sql(), ('dismissed', args.reason, args.anomaly_id))
    audit(conn, "erpclaw-ai-engine", "dismiss-anomaly", "anomaly", args.anomaly_id)
    conn.commit()

    q = Q.from_(a).select(a.star).where(a.id == P())
    updated = row_to_dict(conn.execute(q.get_sql(), (args.anomaly_id,)).fetchone())
    ok({"anomaly": updated})


# ---------------------------------------------------------------------------
# Scenario (actions 9, 10)
# ---------------------------------------------------------------------------

def create_scenario(conn, args):
    """Create a what-if scenario."""
    if not args.name:
        err("--name is required (scenario question)")
    if not args.company_id:
        err("--company-id is required")
    _validate_company(conn, args.company_id)

    scenario_type = args.scenario_type or "price_change"
    if scenario_type not in VALID_SCENARIO_TYPES:
        err(f"Invalid --scenario-type: {scenario_type}. "
             f"Must be one of: {', '.join(sorted(VALID_SCENARIO_TYPES))}")

    assumptions = {}
    if args.assumptions:
        assumptions = _parse_json_arg(args.assumptions, "assumptions")
    assumptions["company_id"] = args.company_id

    scenario_id = str(uuid.uuid4())
    s = Table("scenario")
    q = (Q.into(s)
         .columns("id", "question", "scenario_type", "assumptions", "created_at")
         .insert(P(), P(), P(), P(), P()))
    conn.execute(q.get_sql(),
        (scenario_id, args.name, scenario_type,
         json.dumps(assumptions), _now()))
    audit(conn, "erpclaw-ai-engine", "create-scenario", "scenario", scenario_id)
    conn.commit()

    q = Q.from_(s).select(s.star).where(s.id == P())
    row = row_to_dict(conn.execute(q.get_sql(), (scenario_id,)).fetchone())
    ok({"scenario": row})


def list_scenarios(conn, args):
    """List scenarios."""
    # raw SQL — json_extract() filter not supported by PyPika
    query = "SELECT * FROM scenario WHERE 1=1"
    params = []

    if args.company_id:
        query += " AND json_extract(assumptions, '$.company_id') = ?"
        params.append(args.company_id)

    count_query = query.replace("SELECT *", "SELECT COUNT(*) AS cnt", 1)
    total_count = conn.execute(count_query, params).fetchone()["cnt"]

    limit = int(args.limit)
    offset = int(args.offset)
    query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    rows = [row_to_dict(r) for r in conn.execute(query, params).fetchall()]
    ok({"scenarios": rows, "total_count": total_count,
         "limit": limit, "offset": offset,
         "has_more": offset + limit < total_count})


# ---------------------------------------------------------------------------
# Business Rules (actions 11, 12, 13)
# ---------------------------------------------------------------------------

def add_business_rule(conn, args):
    """Create a natural language business rule."""
    if not args.rule_text:
        err("--rule-text is required")

    severity = args.severity or "warn"
    action_val = SEVERITY_TO_ACTION.get(severity, severity)
    if action_val not in VALID_RULE_ACTION_TYPES:
        err(f"Invalid --severity: {severity}. "
             f"Must be one of: {', '.join(sorted(VALID_RULE_ACTION_TYPES))}")

    parsed_condition = {}
    if args.name:
        parsed_condition["name"] = args.name
    if args.company_id:
        parsed_condition["company_id"] = args.company_id

    rule_id = str(uuid.uuid4())
    br = Table("business_rule")
    q = (Q.into(br)
         .columns("id", "rule_text", "parsed_condition", "applies_to",
                  "action", "active", "times_triggered", "created_at", "updated_at")
         .insert(P(), P(), P(), P(), P(), 1, 0, P(), P()))
    conn.execute(q.get_sql(),
        (rule_id, args.rule_text,
         json.dumps(parsed_condition) if parsed_condition else None,
         args.company_id, action_val, _now(), _now()))
    audit(conn, "erpclaw-ai-engine", "add-business-rule", "business_rule", rule_id)
    conn.commit()

    q = Q.from_(br).select(br.star).where(br.id == P())
    row = row_to_dict(conn.execute(q.get_sql(), (rule_id,)).fetchone())
    ok({"business_rule": row})


def list_business_rules(conn, args):
    """List business rules."""
    # raw SQL — dynamic IS NULL filter not well supported by PyPika
    query = "SELECT * FROM business_rule WHERE 1=1"
    params = []

    if args.company_id:
        query += " AND (applies_to = ? OR applies_to IS NULL)"
        params.append(args.company_id)

    if args.is_active is not None and args.is_active != "":
        active_val = 1 if str(args.is_active).lower() in ("1", "true", "yes") else 0
        query += " AND active = ?"
        params.append(active_val)

    count_query = query.replace("SELECT *", "SELECT COUNT(*) AS cnt", 1)
    total_count = conn.execute(count_query, params).fetchone()["cnt"]

    limit = int(args.limit)
    offset = int(args.offset)
    query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    rows = [row_to_dict(r) for r in conn.execute(query, params).fetchall()]
    ok({"business_rules": rows, "total_count": total_count,
         "limit": limit, "offset": offset,
         "has_more": offset + limit < total_count})


def evaluate_business_rules(conn, args):
    """Evaluate business rules against a proposed action."""
    if not args.action_type:
        err("--action-type is required")
    if not args.action_data:
        err("--action-data is required (JSON)")

    action_data = _parse_json_arg(args.action_data, "action-data")

    # raw SQL — dynamic IS NULL filter not well supported by PyPika
    query = "SELECT * FROM business_rule WHERE active = 1"
    params = []
    if args.company_id:
        query += " AND (applies_to = ? OR applies_to IS NULL)"
        params.append(args.company_id)

    rules = conn.execute(query, params).fetchall()

    triggered_rules = []
    for rule in rules:
        rule_dict = row_to_dict(rule)
        parsed = {}
        if rule_dict.get("parsed_condition"):
            try:
                parsed = json.loads(rule_dict["parsed_condition"])
            except (json.JSONDecodeError, TypeError):
                parsed = {}

        # Check if rule applies to this action type
        applies_to_action = parsed.get("applies_to_action")
        if applies_to_action and applies_to_action != args.action_type:
            continue

        # Check conditions
        conditions = parsed.get("conditions", [])
        if not conditions:
            # Rule with no conditions matches everything
            matched = True
        else:
            matched = _evaluate_conditions(conditions, action_data)

        if matched:
            # raw SQL — SET col = col + 1 arithmetic not well supported by PyPika
            conn.execute(
                """UPDATE business_rule SET times_triggered = times_triggered + 1,
                   last_triggered_at = ? WHERE id = ?""",
                (_now(), rule_dict["id"]),
            )
            triggered_rules.append({
                "rule_id": rule_dict["id"],
                "rule_text": rule_dict["rule_text"],
                "action": rule_dict["action"],
                "name": parsed.get("name"),
            })

    conn.commit()

    if triggered_rules:
        # Return the most restrictive action
        action_priority = {"block": 5, "warn": 4, "notify": 3,
                          "auto_execute": 2, "suggest": 1}
        triggered_rules.sort(
            key=lambda r: action_priority.get(r["action"], 0), reverse=True
        )
        ok({
            "triggered": True,
            "rules": triggered_rules,
            "recommended_action": triggered_rules[0]["action"],
        })
    else:
        ok({"triggered": False, "rules": [], "recommended_action": None})


def _evaluate_conditions(conditions, action_data):
    """Evaluate a list of conditions against action data."""
    for cond in conditions:
        field = cond.get("field")
        operator = cond.get("operator")
        value = cond.get("value")

        if not field or not operator:
            continue

        actual_value = action_data.get(field)
        if actual_value is None:
            return False

        try:
            if operator in (">", "<", ">=", "<="):
                actual_num = Decimal(str(actual_value))
                threshold = Decimal(str(value))
                if operator == ">" and not (actual_num > threshold):
                    return False
                elif operator == "<" and not (actual_num < threshold):
                    return False
                elif operator == ">=" and not (actual_num >= threshold):
                    return False
                elif operator == "<=" and not (actual_num <= threshold):
                    return False
            elif operator == "=":
                if str(actual_value) != str(value):
                    return False
            elif operator == "!=":
                if str(actual_value) == str(value):
                    return False
            elif operator == "contains":
                if str(value).lower() not in str(actual_value).lower():
                    return False
        except (ValueError, TypeError):
            return False

    return True


# ---------------------------------------------------------------------------
# Categorization (actions 14, 15)
# ---------------------------------------------------------------------------

def add_categorization_rule(conn, args):
    """Create an auto-categorization pattern."""
    if not args.pattern:
        err("--pattern is required")
    if not args.account_id:
        err("--account-id is required")

    # Validate account FK
    acct_t = Table("account")
    q = Q.from_(acct_t).select(acct_t.id).where(
        (acct_t.id == P()) | (acct_t.name == P()))
    acct = conn.execute(q.get_sql(), (args.account_id, args.account_id)).fetchone()
    if not acct:
        err(f"Account not found: {args.account_id}")
    args.account_id = acct["id"]

    # Validate optional cost center FK
    if args.cost_center_id:
        cc_t = Table("cost_center")
        q = Q.from_(cc_t).select(cc_t.id).where(
            (cc_t.id == P()) | (cc_t.name == P()))
        cc = conn.execute(q.get_sql(), (args.cost_center_id, args.cost_center_id)).fetchone()
        if not cc:
            err(f"Cost center not found: {args.cost_center_id}")
        args.cost_center_id = cc["id"]

    source = args.source or "bank_feed"
    if source not in VALID_SOURCES:
        err(f"Invalid --source: {source}. "
             f"Must be one of: {', '.join(sorted(VALID_SOURCES))}")

    rule_id = str(uuid.uuid4())
    cr = Table("categorization_rule")
    q = (Q.into(cr)
         .columns("id", "pattern", "source", "target_account_id",
                  "target_cost_center_id", "confidence", "times_applied",
                  "times_overridden", "created_by", "created_at", "updated_at")
         .insert(P(), P(), P(), P(), P(), '0.5', 0, 0, 'user', P(), P()))
    conn.execute(q.get_sql(),
        (rule_id, args.pattern, source, args.account_id,
         args.cost_center_id, _now(), _now()))
    audit(conn, "erpclaw-ai-engine", "add-categorization-rule", "categorization_rule", rule_id)
    conn.commit()

    q = Q.from_(cr).select(cr.star).where(cr.id == P())
    row = row_to_dict(conn.execute(q.get_sql(), (rule_id,)).fetchone())
    ok({"categorization_rule": row})


def categorize_transaction(conn, args):
    """Auto-categorize a transaction using learned patterns."""
    if not args.description:
        err("--description is required")

    desc_lower = args.description.lower()

    # raw SQL — ORDER BY arithmetic expression (confidence + 0) not well supported by PyPika
    rules = conn.execute(
        """SELECT * FROM categorization_rule
           ORDER BY confidence + 0 DESC, times_applied DESC"""
    ).fetchall()

    best_match = None
    for rule in rules:
        rule_dict = row_to_dict(rule)
        pattern = rule_dict["pattern"].lower()
        if pattern in desc_lower:
            best_match = rule_dict
            break

    if best_match:
        # raw SQL — SET col = col + 1 arithmetic not well supported by PyPika
        conn.execute(
            """UPDATE categorization_rule SET times_applied = times_applied + 1,
               last_applied_at = ? WHERE id = ?""",
            (_now(), best_match["id"]),
        )
        conn.commit()
        ok({
            "match": True,
            "rule_id": best_match["id"],
            "pattern": best_match["pattern"],
            "account_id": best_match["target_account_id"],
            "cost_center_id": best_match["target_cost_center_id"],
            "confidence": best_match["confidence"],
        })
    else:
        ok({"match": False, "rule_id": None, "account_id": None,
             "confidence": "0"})


# ---------------------------------------------------------------------------
# Conversation Context (actions 18, 19)
# ---------------------------------------------------------------------------

def save_conversation_context(conn, args):
    """Persist current conversation state."""
    if not args.context_data:
        err("--context-data is required (JSON)")

    data = _parse_json_arg(args.context_data, "context-data")

    context_type = data.get("context_type", "active_workflow")
    if context_type not in VALID_CONTEXT_TYPES:
        err(f"Invalid context_type: {context_type}. "
             f"Must be one of: {', '.join(sorted(VALID_CONTEXT_TYPES))}")

    ctx_id = str(uuid.uuid4())
    cc = Table("conversation_context")
    q = (Q.into(cc)
         .columns("id", "user_id", "context_type", "summary",
                  "related_entities", "state", "last_active", "priority")
         .insert(P(), P(), P(), P(), P(), P(), P(), P()))
    conn.execute(q.get_sql(),
        (ctx_id, data.get("user_id"), context_type,
         data.get("summary"), json.dumps(data.get("related_entities")),
         json.dumps(data.get("state")), _now(),
         data.get("priority", 0)))
    conn.commit()

    q = Q.from_(cc).select(cc.star).where(cc.id == P())
    row = row_to_dict(conn.execute(q.get_sql(), (ctx_id,)).fetchone())
    ok({"context": row})


def get_conversation_context(conn, args):
    """Resume from saved conversation context."""
    cc = Table("conversation_context")
    if args.context_id:
        q = Q.from_(cc).select(cc.star).where(cc.id == P())
        row = conn.execute(q.get_sql(), (args.context_id,)).fetchone()
        if not row:
            err(f"Context not found: {args.context_id}")
    else:
        # Get latest active context
        q = Q.from_(cc).select(cc.star).orderby(cc.last_active, order=Order.desc).limit(1)
        row = conn.execute(q.get_sql()).fetchone()
        if not row:
            ok({"context": None, "message": "No saved context found"})

    ctx = row_to_dict(row)

    # Also fetch any pending decisions for this context
    pd = Table("pending_decision")
    q = (Q.from_(pd).select(pd.star)
         .where(pd.context_id == P())
         .where(pd.status == P())
         .orderby(pd.created_at, order=Order.desc))
    decisions = [row_to_dict(r) for r in conn.execute(
        q.get_sql(), (ctx["id"], 'pending')).fetchall()]

    ctx["pending_decisions"] = decisions
    ok({"context": ctx})


# ---------------------------------------------------------------------------
# Pending Decisions (action 20)
# ---------------------------------------------------------------------------

def add_pending_decision(conn, args):
    """Record a decision awaiting user input."""
    if not args.description:
        err("--description is required")

    options = None
    if args.options:
        options = _parse_json_arg(args.options, "options")

    # Use provided context_id or create a new context
    context_id = args.context_id
    if not context_id:
        context_id = str(uuid.uuid4())
        cc = Table("conversation_context")
        q = (Q.into(cc)
             .columns("id", "context_type", "summary", "last_active", "priority")
             .insert(P(), 'pending_decision', P(), P(), 0))
        conn.execute(q.get_sql(),
            (context_id, f"Decision: {args.description}", _now()))

    decision_id = str(uuid.uuid4())
    pdt = Table("pending_decision")
    q = (Q.into(pdt)
         .columns("id", "context_id", "question", "options",
                  "deadline", "impact", "status", "created_at")
         .insert(P(), P(), P(), P(), P(), P(), 'pending', P()))
    conn.execute(q.get_sql(),
        (decision_id, context_id, args.description,
         json.dumps(options) if options else None,
         args.to_date,  # reuse --to-date as deadline
         args.decision_type,  # reuse as impact description
         _now()))
    conn.commit()

    q = Q.from_(pdt).select(pdt.star).where(pdt.id == P())
    row = row_to_dict(conn.execute(q.get_sql(), (decision_id,)).fetchone())
    ok({"pending_decision": row, "context_id": context_id})


# ---------------------------------------------------------------------------
# Audit (action 21)
# ---------------------------------------------------------------------------

def log_audit_conversation(conn, args):
    """Record AI action audit trail."""
    if not args.action_name:
        err("--action-name is required")

    details = None
    if args.details:
        details = _parse_json_arg(args.details, "details")

    audit_id = str(uuid.uuid4())
    ac = Table("audit_conversation")
    q = (Q.into(ac)
         .columns("id", "timestamp", "voucher_type", "ai_interpretation", "actions_taken")
         .insert(P(), P(), P(), P(), P()))
    conn.execute(q.get_sql(),
        (audit_id, _now(), args.action_name,
         args.result,
         json.dumps(details) if details else None))
    conn.commit()

    q = Q.from_(ac).select(ac.star).where(ac.id == P())
    row = row_to_dict(conn.execute(q.get_sql(), (audit_id,)).fetchone())
    ok({"audit_entry": row})


# ---------------------------------------------------------------------------
# Relationship Scoring (actions 16, 17)
# ---------------------------------------------------------------------------

def score_relationship(conn, args):
    """Compute customer/supplier health score."""
    if not args.party_type:
        err("--party-type is required")
    if args.party_type not in VALID_PARTY_TYPES:
        err(f"Invalid --party-type: {args.party_type}. "
             f"Must be one of: {', '.join(sorted(VALID_PARTY_TYPES))}")
    if not args.party_id:
        err("--party-id is required")

    # Validate party exists
    if args.party_type == "customer":
        cust = Table("customer")
        q = Q.from_(cust).select(cust.id, cust.company_id).where(cust.id == P())
        party = conn.execute(q.get_sql(), (args.party_id,)).fetchone()
        if not party:
            err(f"Customer not found: {args.party_id}")
    else:
        supp = Table("supplier")
        q = Q.from_(supp).select(supp.id, supp.company_id).where(supp.id == P())
        party = conn.execute(q.get_sql(), (args.party_id,)).fetchone()
        if not party:
            err(f"Supplier not found: {args.party_id}")

    company_id = party["company_id"]
    today = _today()

    # --- Payment Score ---
    if args.party_type == "customer":
        si = Table("sales_invoice")
        q = (Q.from_(si)
             .select(si.posting_date, si.due_date, si.grand_total, si.outstanding_amount)
             .where(si.customer_id == P())
             .where(si.status.notin([P(), P()])))
        invoices = conn.execute(q.get_sql(), (args.party_id, 'draft', 'cancelled')).fetchall()
    else:
        pi = Table("purchase_invoice")
        q = (Q.from_(pi)
             .select(pi.posting_date, pi.due_date, pi.grand_total, pi.outstanding_amount)
             .where(pi.supplier_id == P())
             .where(pi.status.notin([P(), P()])))
        invoices = conn.execute(q.get_sql(), (args.party_id, 'draft', 'cancelled')).fetchall()

    pe = Table("payment_entry")
    q = (Q.from_(pe)
         .select(pe.posting_date, pe.paid_amount)
         .where(pe.party_type == P())
         .where(pe.party_id == P())
         .where(pe.status == P()))
    payments = conn.execute(q.get_sql(), (args.party_type, args.party_id, 'submitted')).fetchall()

    total_invoices = len(invoices)
    if total_invoices == 0:
        # No history — return default scores
        score_id = str(uuid.uuid4())
        rs = Table("relationship_score")
        q = (Q.into(rs)
             .columns("id", "party_type", "party_id", "score_date",
                      "overall_score", "payment_score", "volume_trend",
                      "profitability_score", "risk_score", "lifetime_value",
                      "factors", "ai_summary", "created_at")
             .insert(P(), P(), P(), P(), '50', '50', 'stable', '50', '50', '0',
                     P(), P(), P()))
        conn.execute(q.get_sql(),
            (score_id, args.party_type, args.party_id, today,
             json.dumps({"note": "No transaction history"}),
             "No transaction history available for scoring.",
             _now()))
        conn.commit()
        q = Q.from_(rs).select(rs.star).where(rs.id == P())
        row = row_to_dict(conn.execute(q.get_sql(), (score_id,)).fetchone())
        ok({"relationship_score": row})

    # Calculate payment score: on-time = 100, -2 per day late
    late_days_list = []
    for inv in invoices:
        if inv["due_date"] and inv["outstanding_amount"]:
            outstanding = to_decimal(str(inv["outstanding_amount"]))
            if outstanding > 0:
                days_overdue = (
                    datetime.strptime(today, "%Y-%m-%d")
                    - datetime.strptime(inv["due_date"], "%Y-%m-%d")
                ).days
                if days_overdue > 0:
                    late_days_list.append(days_overdue)

    if late_days_list:
        avg_late = sum(late_days_list) / len(late_days_list)
        payment_score = max(0, 100 - int(avg_late * 2))
    else:
        payment_score = 100

    # --- Volume Trend ---
    ninety_days_ago = (datetime.strptime(today, "%Y-%m-%d")
                       - timedelta(days=90)).strftime("%Y-%m-%d")
    one_eighty_days_ago = (datetime.strptime(today, "%Y-%m-%d")
                           - timedelta(days=180)).strftime("%Y-%m-%d")

    # raw SQL — COALESCE(decimal_sum(...)) aggregate with date range filters
    if args.party_type == "customer":
        recent_vol = to_decimal(str(conn.execute(
            """SELECT COALESCE(decimal_sum(grand_total), '0') as total
                FROM sales_invoice
                WHERE customer_id = ? AND posting_date >= ?
                AND status NOT IN ('draft', 'cancelled')""",
            (args.party_id, ninety_days_ago),
        ).fetchone()["total"]))
        prior_vol = to_decimal(str(conn.execute(
            """SELECT COALESCE(decimal_sum(grand_total), '0') as total
                FROM sales_invoice
                WHERE customer_id = ? AND posting_date >= ? AND posting_date < ?
                AND status NOT IN ('draft', 'cancelled')""",
            (args.party_id, one_eighty_days_ago, ninety_days_ago),
        ).fetchone()["total"]))
    else:
        recent_vol = to_decimal(str(conn.execute(
            """SELECT COALESCE(decimal_sum(grand_total), '0') as total
                FROM purchase_invoice
                WHERE supplier_id = ? AND posting_date >= ?
                AND status NOT IN ('draft', 'cancelled')""",
            (args.party_id, ninety_days_ago),
        ).fetchone()["total"]))
        prior_vol = to_decimal(str(conn.execute(
            """SELECT COALESCE(decimal_sum(grand_total), '0') as total
                FROM purchase_invoice
                WHERE supplier_id = ? AND posting_date >= ? AND posting_date < ?
                AND status NOT IN ('draft', 'cancelled')""",
            (args.party_id, one_eighty_days_ago, ninety_days_ago),
        ).fetchone()["total"]))

    if prior_vol > 0:
        vol_change = (recent_vol - prior_vol) / prior_vol
        if vol_change > Decimal("0.1"):
            volume_trend = "growing"
        elif vol_change < Decimal("-0.1"):
            volume_trend = "declining"
        else:
            volume_trend = "stable"
    else:
        volume_trend = "growing" if recent_vol > 0 else "stable"

    volume_score = 70  # default
    if volume_trend == "growing":
        volume_score = 90
    elif volume_trend == "declining":
        volume_score = 40

    # --- Profitability Score ---
    profitability_score = 70  # default — full COGS analysis would need more data

    # --- Risk Score ---
    overdue_count = len(late_days_list)
    if total_invoices > 0:
        overdue_pct = overdue_count / total_invoices
        risk_score = max(0, int(100 - overdue_pct * 100))
    else:
        risk_score = 50

    # raw SQL — COALESCE(decimal_sum(...)) aggregate
    if args.party_type == "customer":
        lifetime_row = conn.execute(
            """SELECT COALESCE(decimal_sum(grand_total), '0') as total
                FROM sales_invoice
                WHERE customer_id = ? AND status NOT IN ('draft', 'cancelled')""",
            (args.party_id,),
        ).fetchone()
    else:
        lifetime_row = conn.execute(
            """SELECT COALESCE(decimal_sum(grand_total), '0') as total
                FROM purchase_invoice
                WHERE supplier_id = ? AND status NOT IN ('draft', 'cancelled')""",
            (args.party_id,),
        ).fetchone()
    lifetime_value = str(round_currency(to_decimal(str(lifetime_row["total"]))))

    # --- Overall Score (weighted average) ---
    overall = int(
        payment_score * 0.30
        + volume_score * 0.20
        + profitability_score * 0.25
        + risk_score * 0.25
    )

    # Build summary
    summary_parts = []
    if payment_score >= 80:
        summary_parts.append("consistent payment history")
    elif payment_score < 50:
        summary_parts.append("frequent late payments")
    if volume_trend == "growing":
        summary_parts.append("growing transaction volume")
    elif volume_trend == "declining":
        summary_parts.append("declining transaction volume")
    if risk_score >= 80:
        summary_parts.append("low risk profile")
    elif risk_score < 50:
        summary_parts.append("elevated risk due to overdue invoices")

    ai_summary = (f"{args.party_type.title()} relationship score: "
                  f"{overall}/100. "
                  + (", ".join(summary_parts) + "." if summary_parts else ""))

    factors = {
        "payment_score": payment_score,
        "volume_score": volume_score,
        "profitability_score": profitability_score,
        "risk_score": risk_score,
        "total_invoices": total_invoices,
        "overdue_invoices": overdue_count,
        "volume_trend": volume_trend,
    }

    score_id = str(uuid.uuid4())
    rs = Table("relationship_score")
    q = (Q.into(rs)
         .columns("id", "party_type", "party_id", "score_date",
                  "overall_score", "payment_score", "volume_trend",
                  "profitability_score", "risk_score", "lifetime_value",
                  "factors", "ai_summary", "created_at")
         .insert(P(), P(), P(), P(), P(), P(), P(), P(), P(), P(), P(), P(), P()))
    conn.execute(q.get_sql(),
        (score_id, args.party_type, args.party_id, today,
         str(overall), str(payment_score), volume_trend,
         str(profitability_score), str(risk_score), lifetime_value,
         json.dumps(factors), ai_summary, _now()))
    conn.commit()

    q = Q.from_(rs).select(rs.star).where(rs.id == P())
    row = row_to_dict(conn.execute(q.get_sql(), (score_id,)).fetchone())
    ok({"relationship_score": row})


def list_relationship_scores(conn, args):
    """List relationship scores."""
    # raw SQL — complex LEFT JOIN with COALESCE for company filtering
    query = "SELECT * FROM relationship_score WHERE 1=1"
    params = []

    if args.party_type:
        query += " AND party_type = ?"
        params.append(args.party_type)

    if args.company_id:
        # Join through customer/supplier to filter by company
        query = """
            SELECT rs.* FROM relationship_score rs
            LEFT JOIN customer c ON rs.party_type = 'customer' AND rs.party_id = c.id
            LEFT JOIN supplier s ON rs.party_type = 'supplier' AND rs.party_id = s.id
            WHERE COALESCE(c.company_id, s.company_id) = ?
        """
        params = [args.company_id]
        if args.party_type:
            query += " AND rs.party_type = ?"
            params.append(args.party_type)

    count_query = query.replace("SELECT *", "SELECT COUNT(*) AS cnt", 1).replace("SELECT rs.*", "SELECT COUNT(*) AS cnt", 1)
    total_count = conn.execute(count_query, params).fetchone()["cnt"]

    limit = int(args.limit)
    offset = int(args.offset)
    query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    rows = [row_to_dict(r) for r in conn.execute(query, params).fetchall()]
    ok({"relationship_scores": rows, "total_count": total_count,
         "limit": limit, "offset": offset,
         "has_more": offset + limit < total_count})


# ---------------------------------------------------------------------------
# Anomaly Detection (actions 1, 2)
# ---------------------------------------------------------------------------

def detect_anomalies(conn, args):
    """Run anomaly detection across all modules."""
    if not args.company_id:
        err("--company-id is required")
    _validate_company(conn, args.company_id)

    from_date = args.from_date or "2000-01-01"
    to_date = args.to_date or _today()
    company_id = args.company_id

    new_anomalies = []
    by_type = {}
    by_severity = {}

    # raw SQL — complex self-join with ABS(julianday()) and correlated subquery
    dupes = conn.execute(
        """SELECT g1.id as id1, g2.id as id2,
                  g1.posting_date as date1, g2.posting_date as date2,
                  g1.account_id, g1.debit, g1.credit
           FROM gl_entry g1
           JOIN gl_entry g2 ON g1.account_id = g2.account_id
             AND g1.debit = g2.debit AND g1.credit = g2.credit
             AND g1.id < g2.id
             AND ABS(julianday(g2.posting_date) - julianday(g1.posting_date)) <= 7
           WHERE g1.account_id IN (SELECT id FROM account WHERE company_id = ?)
             AND g1.posting_date >= ? AND g1.posting_date <= ?
             AND g1.is_cancelled = 0 AND g2.is_cancelled = 0""",
        (company_id, from_date, to_date),
    ).fetchall()

    for dupe in dupes:
        d = row_to_dict(dupe)
        amount = d["debit"] if d["debit"] and d["debit"] != "0" else d["credit"]
        aid = _insert_anomaly(
            conn, "duplicate_possible", "warning",
            "gl_entry", d["id1"],
            f"Possible duplicate GL entry: same account and amount ${amount} "
            f"within 7 days ({d['date1']} and {d['date2']})",
            {"company_id": company_id, "duplicate_id": d["id2"],
             "amount": str(amount), "account_id": d["account_id"]},
        )
        if aid:
            new_anomalies.append(aid)
            by_type["duplicate_possible"] = by_type.get("duplicate_possible", 0) + 1
            by_severity["warning"] = by_severity.get("warning", 0) + 1

    # raw SQL — arithmetic expressions (col + 0, % 1000) and correlated subquery
    rounds = conn.execute(
        """SELECT id, posting_date, account_id, debit, credit
           FROM gl_entry
           WHERE account_id IN (SELECT id FROM account WHERE company_id = ?)
             AND posting_date >= ? AND posting_date <= ?
             AND is_cancelled = 0
             AND (
               (debit + 0 >= 1000 AND (debit + 0) % 1000 = 0
                AND debit != '0')
               OR
               (credit + 0 >= 1000 AND (credit + 0) % 1000 = 0
                AND credit != '0')
             )""",
        (company_id, from_date, to_date),
    ).fetchall()

    for entry in rounds:
        e = row_to_dict(entry)
        amount = e["debit"] if e["debit"] and e["debit"] != "0" else e["credit"]
        aid = _insert_anomaly(
            conn, "round_number", "info",
            "gl_entry", e["id"],
            f"Suspiciously round GL entry: ${amount} on {e['posting_date']}",
            {"company_id": company_id, "amount": str(amount),
             "account_id": e["account_id"]},
        )
        if aid:
            new_anomalies.append(aid)
            by_type["round_number"] = by_type.get("round_number", 0) + 1
            by_severity["info"] = by_severity.get("info", 0) + 1

    # --- Heuristic 3: budget_overrun ---
    bgt = Table("budget")
    q = (Q.from_(bgt)
         .select(bgt.id, bgt.account_id, bgt.cost_center_id, bgt.budget_amount)
         .where(bgt.company_id == P()))
    budgets = conn.execute(q.get_sql(), (company_id,)).fetchall()

    for b in budgets:
        bd = row_to_dict(b)
        budget_amt = to_decimal(str(bd["budget_amount"])) if bd["budget_amount"] else Decimal("0")

        if not bd["account_id"]:
            continue

        # raw SQL — COALESCE(decimal_sum()) arithmetic with dynamic WHERE
        actual_query = """
            SELECT COALESCE(decimal_sum(debit), '0') - COALESCE(decimal_sum(credit), '0') as actual
            FROM gl_entry
            WHERE account_id = ? AND posting_date >= ? AND posting_date <= ?
            AND is_cancelled = 0
        """
        actual_params = [bd["account_id"], from_date, to_date]
        if bd["cost_center_id"]:
            actual_query += " AND cost_center_id = ?"
            actual_params.append(bd["cost_center_id"])

        actual = to_decimal(str(conn.execute(actual_query, actual_params).fetchone()["actual"]))

        if budget_amt > 0 and actual > budget_amt:
            deviation = ((actual - budget_amt) / budget_amt * Decimal("100")).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP)
            severity = "critical" if deviation > Decimal("10") else "warning"
            aid = _insert_anomaly(
                conn, "budget_overrun", severity,
                "budget", bd["id"],
                f"Budget overrun: actual ${actual} exceeds "
                f"budget ${budget_amt} ({deviation}% over)",
                {"company_id": company_id, "budget_id": bd["id"],
                 "account_id": bd["account_id"],
                 "cost_center_id": bd["cost_center_id"]},
                baseline={"budget_amount": str(budget_amt)},
                actual={"actual_spend": str(round_currency(actual))},
                deviation_pct=str(deviation),
            )
            if aid:
                new_anomalies.append(aid)
                by_type["budget_overrun"] = by_type.get("budget_overrun", 0) + 1
                by_severity[severity] = by_severity.get(severity, 0) + 1

    # raw SQL — arithmetic expression (outstanding_amount + 0 > 0) for TEXT-to-number cast
    overdue = conn.execute(
        """SELECT id, customer_id, posting_date, due_date,
                  outstanding_amount, grand_total
           FROM sales_invoice
           WHERE company_id = ?
             AND status IN ('submitted', 'partially_paid', 'overdue')
             AND due_date < ?
             AND outstanding_amount + 0 > 0""",
        (company_id, to_date),
    ).fetchall()

    for inv in overdue:
        i = row_to_dict(inv)
        days_late = (
            datetime.strptime(to_date, "%Y-%m-%d")
            - datetime.strptime(i["due_date"], "%Y-%m-%d")
        ).days
        severity = "critical" if days_late >= 30 else "warning"
        aid = _insert_anomaly(
            conn, "late_pattern", severity,
            "sales_invoice", i["id"],
            f"Invoice {days_late} days past due. "
            f"Outstanding: ${i['outstanding_amount']} of ${i['grand_total']}",
            {"company_id": company_id, "customer_id": i["customer_id"],
             "days_late": days_late},
            deviation_pct=days_late,
        )
        if aid:
            new_anomalies.append(aid)
            by_type["late_pattern"] = by_type.get("late_pattern", 0) + 1
            by_severity[severity] = by_severity.get(severity, 0) + 1

    # --- Heuristic 5: volume_change ---
    period_days = (
        datetime.strptime(to_date, "%Y-%m-%d")
        - datetime.strptime(from_date, "%Y-%m-%d")
    ).days
    if period_days > 0:
        prior_from = (datetime.strptime(from_date, "%Y-%m-%d")
                      - timedelta(days=period_days)).strftime("%Y-%m-%d")
        prior_to = from_date

        # raw SQL — COALESCE(decimal_sum(...)) aggregates with date-range filters
        _vol_queries = [
            ("SELECT COUNT(*) as cnt, COALESCE(decimal_sum(grand_total), '0') as total "
             "FROM sales_invoice WHERE company_id = ? AND posting_date >= ? AND posting_date <= ? "
             "AND status NOT IN ('draft', 'cancelled')",
             "SELECT COUNT(*) as cnt, COALESCE(decimal_sum(grand_total), '0') as total "
             "FROM sales_invoice WHERE company_id = ? AND posting_date >= ? AND posting_date < ? "
             "AND status NOT IN ('draft', 'cancelled')",
             "sales"),
            ("SELECT COUNT(*) as cnt, COALESCE(decimal_sum(grand_total), '0') as total "
             "FROM purchase_invoice WHERE company_id = ? AND posting_date >= ? AND posting_date <= ? "
             "AND status NOT IN ('draft', 'cancelled')",
             "SELECT COUNT(*) as cnt, COALESCE(decimal_sum(grand_total), '0') as total "
             "FROM purchase_invoice WHERE company_id = ? AND posting_date >= ? AND posting_date < ? "
             "AND status NOT IN ('draft', 'cancelled')",
             "purchases"),
        ]
        for cur_sql, pri_sql, label in _vol_queries:
            current = conn.execute(cur_sql, (company_id, from_date, to_date)).fetchone()
            prior = conn.execute(pri_sql, (company_id, prior_from, prior_to)).fetchone()

            cur_total = to_decimal(str(current["total"]))
            pri_total = to_decimal(str(prior["total"]))
            if pri_total > 0:
                pct_change = ((cur_total - pri_total)
                              / pri_total * Decimal("100"))
                if abs(pct_change) > Decimal("30"):
                    severity = "critical" if abs(pct_change) > Decimal("50") else "warning"
                    direction = "increased" if pct_change > 0 else "decreased"
                    pct_rounded = pct_change.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
                    aid = _insert_anomaly(
                        conn, "volume_change", severity,
                        "company", f"{company_id}:{label}",
                        f"{label.title()} volume {direction} by "
                        f"{abs(pct_rounded)}% vs prior period",
                        {"company_id": company_id, "module": label,
                         "current_total": str(round_currency(cur_total)),
                         "prior_total": str(round_currency(pri_total))},
                        baseline={"prior_total": str(round_currency(pri_total)),
                                  "prior_count": prior["cnt"]},
                        actual={"current_total": str(round_currency(cur_total)),
                                "current_count": current["cnt"]},
                        deviation_pct=str(pct_rounded),
                    )
                    if aid:
                        new_anomalies.append(aid)
                        by_type["volume_change"] = by_type.get("volume_change", 0) + 1
                        by_severity[severity] = by_severity.get(severity, 0) + 1

    # --- Heuristic 6: asset_book_value_drift (AI1) ---
    # Accounting invariant: current_book_value == gross_value − accumulated_depreciation.
    # A material deviation signals an unexplained book-value spike/drop. Point-in-time
    # state check (assets carry no posting_date), so it ignores the date window.
    ast = Table("asset")
    q = (Q.from_(ast)
         .select(ast.id, ast.asset_name, ast.gross_value,
                 ast.accumulated_depreciation, ast.current_book_value)
         .where(ast.company_id == P())
         .where(ast.status.notin([P(), P()])))
    assets = conn.execute(
        q.get_sql(), (company_id, 'draft', 'under_construction')).fetchall()

    for a_row in assets:
        ad = row_to_dict(a_row)
        gross = to_decimal(str(ad["gross_value"] or "0"))
        accum = to_decimal(str(ad["accumulated_depreciation"] or "0"))
        actual_bv = to_decimal(str(ad["current_book_value"] or "0"))
        expected_bv = gross - accum
        if expected_bv <= 0:
            # Fully depreciated / draft-like values carry no meaningful invariant.
            continue
        deviation = (abs(actual_bv - expected_bv) / expected_bv
                     * Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if deviation > Decimal("5"):
            severity = "critical" if deviation > Decimal("25") else "warning"
            aid = _insert_anomaly(
                conn, "asset_book_value_drift", severity,
                "asset", ad["id"],
                f"Asset book value ${actual_bv} drifts {deviation}% from expected "
                f"${expected_bv} (gross ${gross} − accumulated depreciation ${accum})",
                {"company_id": company_id, "asset_id": ad["id"],
                 "asset_name": ad["asset_name"]},
                baseline={"expected_book_value": str(expected_bv)},
                actual={"current_book_value": str(actual_bv)},
                deviation_pct=str(deviation),
            )
            if aid:
                new_anomalies.append(aid)
                by_type["asset_book_value_drift"] = by_type.get("asset_book_value_drift", 0) + 1
                by_severity[severity] = by_severity.get(severity, 0) + 1

    # --- Heuristic 7: dimension_tag_drift (AI1, reads dimensions_json — M6) ---
    # Within an account_type, dimension tagging should be consistent. If a key
    # appears on some entries but is omitted on others (partial coverage), flag it.
    # Grouping/parsing is done in Python (no JSON SQL / GROUP BY), so it is dialect-safe.
    tag_rows = conn.execute(
        """SELECT a.account_type AS account_type, g.dimensions_json AS dims
           FROM gl_entry g
           JOIN account a ON g.account_id = a.id
           WHERE a.company_id = ?
             AND g.posting_date >= ? AND g.posting_date <= ?
             AND g.is_cancelled = 0""",
        (company_id, from_date, to_date),
    ).fetchall()

    # account_type -> {"total": n, "keys": {dimension_key: present_count}}
    tag_stats = {}
    for tr in tag_rows:
        t = row_to_dict(tr)
        acct_type = t["account_type"] or "unknown"
        stat = tag_stats.setdefault(acct_type, {"total": 0, "keys": {}})
        stat["total"] += 1
        try:
            dims = json.loads(t["dims"]) if t["dims"] else {}
        except (json.JSONDecodeError, TypeError):
            dims = {}
        if isinstance(dims, dict):
            for k, v in dims.items():
                if v is not None and v != "":
                    stat["keys"][k] = stat["keys"].get(k, 0) + 1

    MIN_TAG_ENTRIES = 3  # too few entries to judge a consistent convention
    for acct_type, stat in sorted(tag_stats.items()):
        total = stat["total"]
        if total < MIN_TAG_ENTRIES:
            continue
        for key in sorted(stat["keys"]):
            present = stat["keys"][key]
            if 0 < present < total:  # partial coverage == inconsistent tagging
                coverage = (Decimal(present) / Decimal(total) * Decimal("100")).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP)
                aid = _insert_anomaly(
                    conn, "dimension_tag_drift", "warning",
                    "account_type", f"{company_id}:{acct_type}:{key}",
                    f"Inconsistent '{key}' dimension tagging on {acct_type} entries: "
                    f"{present} of {total} tagged ({coverage}%)",
                    {"company_id": company_id, "account_type": acct_type,
                     "dimension_key": key, "tagged_count": present,
                     "total_count": total},
                    deviation_pct=str(coverage),
                )
                if aid:
                    new_anomalies.append(aid)
                    by_type["dimension_tag_drift"] = by_type.get("dimension_tag_drift", 0) + 1
                    by_severity["warning"] = by_severity.get("warning", 0) + 1

    # --- Heuristics 8 + 9: Wave 2 AI1 inventory hooks; 10: Wave F usage ---
    # reservation_over_available (reads M5 stock_reservation_entry + SLE),
    # subcontract_receipt_mismatch (reads S5 subcontracting_order), and the
    # Wave F usage detector (consumption_spike + rate_plan_mismatch, reads the
    # foundation billing substrate). Each detector emits through
    # _insert_anomaly and returns the anomalies it inserted so the sweep's
    # roll-up counters stay consistent. The usage detector is told whether the
    # USER supplied --from-date: on the default sweep the '2000-01-01'
    # sentinel would swallow every reading into the window (empty baseline ->
    # consumption_spike could never fire), so it switches to a meter-local
    # recency split instead of trusting the sentinel as a split point.
    for detector, extra in (
            (_detect_reservation_over_available, {}),
            (_detect_subcontract_receipt_mismatch, {}),
            (_detect_usage_anomaly,
             {"window_is_explicit": bool(args.from_date)})):
        for a in detector(conn, company_id, from_date, to_date, **extra):
            new_anomalies.append(a["id"])
            by_type[a["type"]] = by_type.get(a["type"], 0) + 1
            by_severity[a["severity"]] = by_severity.get(a["severity"], 0) + 1

    conn.commit()
    ok({
        "anomalies_detected": len(new_anomalies),
        "anomaly_ids": new_anomalies,
        "by_type": by_type,
        "by_severity": by_severity,
    })


def list_anomalies(conn, args):
    """Query detected anomalies."""
    # raw SQL — json_extract() filter not supported by PyPika
    query = "SELECT * FROM anomaly WHERE 1=1"
    params = []

    if args.company_id:
        query += " AND json_extract(evidence, '$.company_id') = ?"
        params.append(args.company_id)

    if args.severity:
        if args.severity not in VALID_SEVERITY:
            err(f"Invalid --severity: {args.severity}")
        query += " AND severity = ?"
        params.append(args.severity)

    if args.status:
        if args.status not in VALID_ANOMALY_STATUSES:
            err(f"Invalid --status: {args.status}")
        query += " AND status = ?"
        params.append(args.status)

    count_query = query.replace("SELECT *", "SELECT COUNT(*) AS cnt", 1)
    total_count = conn.execute(count_query, params).fetchone()["cnt"]

    limit = int(args.limit)
    offset = int(args.offset)
    query += " ORDER BY detected_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    rows = [row_to_dict(r) for r in conn.execute(query, params).fetchall()]
    ok({"anomalies": rows, "total_count": total_count,
         "limit": limit, "offset": offset,
         "has_more": offset + limit < total_count})


# ---------------------------------------------------------------------------
# Cash Flow Forecasting (actions 5, 6)
# ---------------------------------------------------------------------------

def forecast_cash_flow(conn, args):
    """Generate cash flow forecast with 3 scenarios."""
    if not args.company_id:
        err("--company-id is required")
    _validate_company(conn, args.company_id)

    company_id = args.company_id
    horizon = int(args.horizon_days) if args.horizon_days else 30
    today = _today()
    horizon_date = (datetime.strptime(today, "%Y-%m-%d")
                    + timedelta(days=horizon)).strftime("%Y-%m-%d")

    # raw SQL — JOIN with COALESCE(decimal_sum()) aggregates
    bal_row = conn.execute(
        """SELECT COALESCE(decimal_sum(debit), '0') as total_debit,
                  COALESCE(decimal_sum(credit), '0') as total_credit
           FROM gl_entry g
           JOIN account a ON g.account_id = a.id
           WHERE a.company_id = ? AND a.account_type IN ('bank', 'cash')
           AND g.is_cancelled = 0""",
        (company_id,),
    ).fetchone()
    starting_balance = round_currency(to_decimal(str(bal_row["total_debit"])) - to_decimal(str(bal_row["total_credit"])))

    # raw SQL — arithmetic expression (outstanding_amount + 0 > 0) for TEXT-to-number cast
    ar_rows = conn.execute(
        """SELECT due_date, outstanding_amount FROM sales_invoice
           WHERE company_id = ?
           AND status IN ('submitted', 'partially_paid', 'overdue')
           AND outstanding_amount + 0 > 0""",
        (company_id,),
    ).fetchall()

    inflows = []
    total_inflows = Decimal("0")
    for ar in ar_rows:
        amt = to_decimal(str(ar["outstanding_amount"]))
        due = ar["due_date"] or today
        inflows.append({"date": due, "amount": str(amt)})
        total_inflows += amt

    # raw SQL — arithmetic expression (outstanding_amount + 0 > 0) for TEXT-to-number cast
    ap_rows = conn.execute(
        """SELECT due_date, outstanding_amount FROM purchase_invoice
           WHERE company_id = ?
           AND status IN ('submitted', 'partially_paid', 'overdue')
           AND outstanding_amount + 0 > 0""",
        (company_id,),
    ).fetchall()

    outflows = []
    total_outflows = Decimal("0")
    for ap in ap_rows:
        amt = to_decimal(str(ap["outstanding_amount"]))
        due = ap["due_date"] or today
        outflows.append({"date": due, "amount": str(amt)})
        total_outflows += amt

    # Generate 3 scenarios
    scenarios_data = {
        "pessimistic": {"inflow_mult": Decimal("0.7"),
                        "outflow_mult": Decimal("1.2")},
        "expected": {"inflow_mult": Decimal("0.9"),
                     "outflow_mult": Decimal("1.0")},
        "optimistic": {"inflow_mult": Decimal("1.0"),
                       "outflow_mult": Decimal("0.8")},
    }

    forecast_ids = []
    balances = {}
    start_bal = to_decimal(str(starting_balance))

    for scenario_name, mults in scenarios_data.items():
        adj_inflows = total_inflows * mults["inflow_mult"]
        adj_outflows = total_outflows * mults["outflow_mult"]
        projected = start_bal + adj_inflows - adj_outflows

        forecast_id = str(uuid.uuid4())
        cff = Table("cash_flow_forecast")
        q = (Q.into(cff)
             .columns("id", "forecast_date", "generated_at", "horizon_days",
                      "starting_balance", "projected_inflows", "projected_outflows",
                      "projected_balance", "confidence_interval", "assumptions",
                      "scenario")
             .insert(P(), P(), P(), P(), P(), P(), P(), P(), P(), P(), P()))
        conn.execute(q.get_sql(),
            (forecast_id, today, _now(), horizon,
             str(start_bal),
             json.dumps(inflows),
             json.dumps(outflows),
             str(round_currency(projected)),
             None,  # filled after all scenarios
             json.dumps({"company_id": company_id,
                         "inflow_multiplier": str(mults["inflow_mult"]),
                         "outflow_multiplier": str(mults["outflow_mult"])}),
             scenario_name))
        forecast_ids.append(forecast_id)
        balances[scenario_name] = round_currency(projected)

    # Update confidence intervals on all forecasts
    ci = {
        "low": str(balances["pessimistic"]),
        "mid": str(balances["expected"]),
        "high": str(balances["optimistic"]),
    }
    cff_upd = Table("cash_flow_forecast")
    q_upd = Q.update(cff_upd).set(cff_upd.confidence_interval, P()).where(cff_upd.id == P())
    for fid in forecast_ids:
        conn.execute(q_upd.get_sql(), (json.dumps(ci), fid))

    conn.commit()

    ok({
        "forecast_ids": forecast_ids,
        "starting_balance": str(start_bal),
        "horizon_days": horizon,
        "scenarios": {
            name: str(bal) for name, bal in balances.items()
        },
        "confidence_interval": ci,
        "total_ar": str(round_currency(total_inflows)),
        "total_ap": str(round_currency(total_outflows)),
    })


def get_forecast(conn, args):
    """Retrieve latest forecast."""
    # raw SQL — json_extract() filter not supported by PyPika
    query = """SELECT * FROM cash_flow_forecast WHERE 1=1"""
    params = []

    if args.company_id:
        query += " AND json_extract(assumptions, '$.company_id') = ?"
        params.append(args.company_id)

    query += " ORDER BY generated_at DESC LIMIT 3"

    rows = [row_to_dict(r) for r in conn.execute(query, params).fetchall()]
    if not rows:
        ok({"forecasts": [], "message": "No forecasts found"})

    ok({"forecasts": rows, "count": len(rows)})


# ---------------------------------------------------------------------------
# Correlation Discovery (actions 7, 8)
# ---------------------------------------------------------------------------

def discover_correlations(conn, args):
    """Find cross-module patterns."""
    if not args.company_id:
        err("--company-id is required")
    _validate_company(conn, args.company_id)

    company_id = args.company_id
    from_date = args.from_date or "2000-01-01"
    to_date = args.to_date or _today()

    new_correlations = []

    # raw SQL — COALESCE(decimal_sum(...)) aggregates with date-range filters
    sales = conn.execute(
        """SELECT COALESCE(decimal_sum(grand_total), '0') as total,
                  COUNT(*) as cnt
           FROM sales_invoice
           WHERE company_id = ? AND posting_date >= ? AND posting_date <= ?
           AND status NOT IN ('draft', 'cancelled')""",
        (company_id, from_date, to_date),
    ).fetchone()

    purchases = conn.execute(
        """SELECT COALESCE(decimal_sum(grand_total), '0') as total,
                  COUNT(*) as cnt
           FROM purchase_invoice
           WHERE company_id = ? AND posting_date >= ? AND posting_date <= ?
           AND status NOT IN ('draft', 'cancelled')""",
        (company_id, from_date, to_date),
    ).fetchone()

    sales_total = to_decimal(str(sales["total"]))
    purchases_total = to_decimal(str(purchases["total"]))
    if sales["cnt"] > 0 and purchases["cnt"] > 0:
        # Check if both are positive (same direction)
        ratio = (purchases_total / sales_total).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP) if sales_total > 0 else Decimal("0")
        if Decimal("0.3") < ratio < Decimal("3.0"):
            strength = "strong" if Decimal("0.5") < ratio < Decimal("2.0") else "moderate"
        else:
            strength = "weak"

        corr_id = str(uuid.uuid4())
        corr = Table("correlation")
        q = (Q.into(corr)
             .columns("id", "discovered_at", "module_a", "module_b",
                      "description", "evidence", "strength", "statistical_confidence",
                      "actionable", "suggested_action", "status")
             .insert(P(), P(), 'selling', 'buying', P(), P(), P(), P(), P(), P(), 'new'))
        conn.execute(q.get_sql(),
            (corr_id, _now(),
             f"Sales-to-purchase ratio of {ratio} detected. "
             f"Sales: ${round_currency(sales_total)}, Purchases: ${round_currency(purchases_total)}",
             json.dumps({"company_id": company_id,
                         "sales_total": str(round_currency(sales_total)),
                         "purchases_total": str(round_currency(purchases_total)),
                         "ratio": str(ratio)}),
             strength,
             f"{min(sales['cnt'], purchases['cnt']) * 10}",
             1 if strength in ("strong", "moderate") else 0,
             "Review procurement efficiency relative to sales volume"
             if ratio > Decimal("0.7") else None))
        new_correlations.append(corr_id)

    # raw SQL — complex CASE with correlated subqueries, AVG(julianday()), GROUP BY
    pay_data = conn.execute(
        """SELECT pe.party_type, COUNT(*) as cnt,
                  AVG(julianday(pe.posting_date) - julianday(
                    CASE WHEN pe.party_type = 'customer'
                         THEN (SELECT si.posting_date FROM sales_invoice si
                               WHERE si.customer_id = pe.party_id LIMIT 1)
                         ELSE (SELECT pi.posting_date FROM purchase_invoice pi
                               WHERE pi.supplier_id = pe.party_id LIMIT 1)
                    END
                  )) as avg_days
           FROM payment_entry pe
           WHERE pe.company_id = ? AND pe.posting_date >= ? AND pe.posting_date <= ?
           AND pe.status = 'submitted'
           GROUP BY pe.party_type""",
        (company_id, from_date, to_date),
    ).fetchall()

    if len(pay_data) >= 1:
        for pd in pay_data:
            pdd = row_to_dict(pd)
            if pdd["avg_days"] is not None:
                avg_d = round(pdd["avg_days"], 1)
                strength = "strong" if abs(avg_d) < 15 else "moderate" if abs(avg_d) < 30 else "weak"
                corr_id = str(uuid.uuid4())
                corr2 = Table("correlation")
                q2 = (Q.into(corr2)
                      .columns("id", "discovered_at", "module_a", "module_b",
                               "description", "evidence", "strength", "actionable", "status")
                      .insert(P(), P(), 'payments', P(), P(), P(), P(), 0, 'new'))
                conn.execute(q2.get_sql(),
                    (corr_id, _now(),
                     pdd["party_type"],
                     f"Average {pdd['party_type']} payment timing: "
                     f"{avg_d} days from invoice",
                     json.dumps({"company_id": company_id,
                                 "party_type": pdd["party_type"],
                                 "avg_payment_days": avg_d,
                                 "payment_count": pdd["cnt"]}),
                     strength))
                new_correlations.append(corr_id)

    conn.commit()
    ok({
        "correlations_discovered": len(new_correlations),
        "correlation_ids": new_correlations,
    })


def list_correlations(conn, args):
    """List discovered correlations."""
    # raw SQL — json_extract() filter and dynamic IN clause
    query = "SELECT * FROM correlation WHERE 1=1"
    params = []

    if args.company_id:
        query += " AND json_extract(evidence, '$.company_id') = ?"
        params.append(args.company_id)

    if args.min_strength:
        min_val = STRENGTH_ORDER.get(args.min_strength, 0)
        strengths = [s for s, v in STRENGTH_ORDER.items() if v >= min_val]
        if strengths:
            placeholders = ",".join(["?" for _ in strengths])
            query += f" AND strength IN ({placeholders})"
            params.extend(strengths)

    count_query = query.replace("SELECT *", "SELECT COUNT(*) AS cnt", 1)
    total_count = conn.execute(count_query, params).fetchone()["cnt"]

    limit = int(args.limit)
    offset = int(args.offset)
    query += " ORDER BY discovered_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    rows = [row_to_dict(r) for r in conn.execute(query, params).fetchall()]
    ok({"correlations": rows, "total_count": total_count,
         "limit": limit, "offset": offset,
         "has_more": offset + limit < total_count})


# ---------------------------------------------------------------------------
# Status (action 22)
# ---------------------------------------------------------------------------

def status_action(conn, args):
    """AI engine summary."""
    result = {}

    # raw SQL — json_extract() filter with GROUP BY
    anomaly_q = "SELECT severity, COUNT(*) as cnt FROM anomaly WHERE status = 'new'"
    anomaly_params = []
    if args.company_id:
        anomaly_q += " AND json_extract(evidence, '$.company_id') = ?"
        anomaly_params.append(args.company_id)
    anomaly_q += " GROUP BY severity"

    anomaly_counts = {}
    for row in conn.execute(anomaly_q, anomaly_params).fetchall():
        anomaly_counts[row["severity"]] = row["cnt"]
    result["anomalies"] = {
        "new_total": sum(anomaly_counts.values()),
        "by_severity": anomaly_counts,
    }

    # raw SQL — json_extract() filter
    forecast_q = "SELECT COUNT(*) as cnt FROM cash_flow_forecast"
    forecast_params = []
    if args.company_id:
        forecast_q += " WHERE json_extract(assumptions, '$.company_id') = ?"
        forecast_params.append(args.company_id)
    result["forecasts"] = conn.execute(forecast_q, forecast_params).fetchone()["cnt"]

    # Rules — simple counts
    br = Table("business_rule")
    q_active = Q.from_(br).select(fn.Count("*").as_("cnt")).where(br.active == 1)
    q_total = Q.from_(br).select(fn.Count("*").as_("cnt"))
    result["business_rules"] = {
        "active": conn.execute(q_active.get_sql()).fetchone()["cnt"],
        "total": conn.execute(q_total.get_sql()).fetchone()["cnt"],
    }

    # Categorization rules — simple count
    cr = Table("categorization_rule")
    q = Q.from_(cr).select(fn.Count("*").as_("cnt"))
    result["categorization_rules"] = conn.execute(q.get_sql()).fetchone()["cnt"]

    # raw SQL — json_extract() filter
    corr_q = "SELECT COUNT(*) as cnt FROM correlation WHERE status = 'new'"
    corr_params = []
    if args.company_id:
        corr_q += " AND json_extract(evidence, '$.company_id') = ?"
        corr_params.append(args.company_id)
    result["correlations"] = conn.execute(corr_q, corr_params).fetchone()["cnt"]

    # raw SQL — json_extract() filter
    scen_q = "SELECT COUNT(*) as cnt FROM scenario"
    scen_params = []
    if args.company_id:
        scen_q += " WHERE json_extract(assumptions, '$.company_id') = ?"
        scen_params.append(args.company_id)
    result["scenarios"] = conn.execute(scen_q, scen_params).fetchone()["cnt"]

    # Relationship scores — simple count
    rs_t = Table("relationship_score")
    q = Q.from_(rs_t).select(fn.Count("*").as_("cnt"))
    result["relationship_scores"] = conn.execute(q.get_sql()).fetchone()["cnt"]

    # Pending decisions — simple count with filter
    pd_t = Table("pending_decision")
    q = Q.from_(pd_t).select(fn.Count("*").as_("cnt")).where(pd_t.status == P())
    result["pending_decisions"] = conn.execute(q.get_sql(), ('pending',)).fetchone()["cnt"]

    # Conversation contexts — simple count
    cc_t = Table("conversation_context")
    q = Q.from_(cc_t).select(fn.Count("*").as_("cnt"))
    result["active_contexts"] = conn.execute(q.get_sql()).fetchone()["cnt"]

    # Audit log entries — simple count
    ac_t = Table("audit_conversation")
    q = Q.from_(ac_t).select(fn.Count("*").as_("cnt"))
    result["audit_entries"] = conn.execute(q.get_sql()).fetchone()["cnt"]

    ok(result)


# ============================================================================
# ACTION REGISTRY
# ============================================================================

ACTIONS = {
    "detect-anomalies": detect_anomalies,
    "list-anomalies": list_anomalies,
    "acknowledge-anomaly": acknowledge_anomaly,
    "dismiss-anomaly": dismiss_anomaly,
    "forecast-cash-flow": forecast_cash_flow,
    "get-forecast": get_forecast,
    "discover-correlations": discover_correlations,
    "list-correlations": list_correlations,
    "create-scenario": create_scenario,
    "list-scenarios": list_scenarios,
    "add-business-rule": add_business_rule,
    "list-business-rules": list_business_rules,
    "evaluate-business-rules": evaluate_business_rules,
    "add-categorization-rule": add_categorization_rule,
    "categorize-transaction": categorize_transaction,
    "score-relationship": score_relationship,
    "list-relationship-scores": list_relationship_scores,
    "save-conversation-context": save_conversation_context,
    "get-conversation-context": get_conversation_context,
    "add-pending-decision": add_pending_decision,
    "log-audit-conversation": log_audit_conversation,
    "status": status_action,
}


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = SafeArgumentParser(description="ERPClaw AI Engine")
    parser.add_argument("--action", required=True, choices=list(ACTIONS.keys()))
    parser.add_argument("--db-path", default=None)

    # Entity IDs
    parser.add_argument("--anomaly-id")
    parser.add_argument("--context-id")
    parser.add_argument("--company-id")

    # Detection / filter
    parser.add_argument("--from-date")
    parser.add_argument("--to-date")
    parser.add_argument("--severity")
    parser.add_argument("--status")

    # Forecast
    parser.add_argument("--horizon-days")

    # Scenario
    parser.add_argument("--scenario-type")
    parser.add_argument("--assumptions")
    parser.add_argument("--name")

    # Business rules
    parser.add_argument("--rule-text")
    parser.add_argument("--is-active")
    parser.add_argument("--action-type")
    parser.add_argument("--action-data")

    # Categorization
    parser.add_argument("--pattern")
    parser.add_argument("--account-id")
    parser.add_argument("--description")
    parser.add_argument("--amount")
    parser.add_argument("--source")
    parser.add_argument("--cost-center-id")

    # Relationship
    parser.add_argument("--party-type")
    parser.add_argument("--party-id")

    # Context / Decision
    parser.add_argument("--context-data")
    parser.add_argument("--decision-type")
    parser.add_argument("--options")

    # Audit
    parser.add_argument("--action-name")
    parser.add_argument("--details")
    parser.add_argument("--result")

    # Correlation
    parser.add_argument("--min-strength")

    # General
    parser.add_argument("--limit", default="20")
    parser.add_argument("--offset", default="0")
    parser.add_argument("--reason")

    args, unknown = parser.parse_known_args()
    check_unknown_args(parser, unknown)
    check_input_lengths(args)

    db_path = args.db_path
    if db_path:
        os.environ["ERPCLAW_DB_PATH"] = db_path

    ensure_db_exists()
    conn = get_connection()

    # Dependency check
    _dep = check_required_tables(conn, REQUIRED_TABLES)
    if _dep:
        _dep["suggestion"] = "clawhub install " + " ".join(_dep.get("missing_skills", []))
        print(json.dumps(_dep, indent=2))
        conn.close()
        sys.exit(1)

    try:
        ACTIONS[args.action](conn, args)
    except SystemExit:
        raise
    except Exception as e:
        sys.stderr.write(f"[erpclaw-ai-engine] {e}\n")
        err("An unexpected error occurred")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
