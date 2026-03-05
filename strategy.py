"""
Dual-trigger allocator — v5 (Manual conversion-date reset + daily Peak_trade update)

Current behavior:
✅ Before first final DD stage trigger, staged DD decisions use rolling-window peak DD.
✅ After first final DD stage trigger, staged DD decisions switch to trade-peak DD.
✅ After final DD stage full-fill (e.g. 40% + 100%), strategy enters
   "wait conversion" state; user can fill conversion date later.
✅ Once pending conversion date is reached (trading day), we reset:
   trade_anchor_date = conversion date, trade_peak = basket_idx[that day],
   and clear dd_triggered.
✅ After reset, every day we update Peak_trade:
   - on anchor day: Peak_trade = basket_idx[anchor]
   - thereafter: Peak_trade = max(Peak_trade, basket_idx[today])

This version keeps your existing framework:
- Basket index = equal-weight across N stock funds (N funds => 1/N each)
- Cash excluded from decision ratios: decision_base = S + B
- FORCE_LOW: W < force_low_w => buy toward target_stock_w (cash first, then sell bonds, bond floor applies)
- FORCE_HIGH smoothing: W > force_high_w => sell only down to force_high_target_w (stock floor applies)
- DD staged add: dd_trade crosses 20/30/40 => fill gap 40/70/100 (event trigger)
- Floors: min_bond_value, min_stock_total_value
- Cooldown (calendar days) after FORCE_LOW blocks DD_ADD (but not FORCE_HIGH)

========================
INPUT SPEC (UI)
========================
A) nav_df: DataFrame
   - index: datetime (trading days)
   - columns: stock fund ids (strings)
   - values: NAV floats

B) funds: List[str]  # which columns included in stock basket

C) bond_value: float
D) cash_value: float  # funding source only (excluded from ratios)
E) stock_values: Dict[str,float]  # per fund current market value; keys in funds (missing treated as 0)

F) params: DualTriggerParams (see dataclass)

G) state: DualTriggerState (persist)
   - dd_triggered: list[float]
   - cooldown_until_date: str or None
   - last_decision_date: str or None
   - trade_anchor_date: str or None
   - trade_peak: float or None

H) today: pd.Timestamp

========================
OUTPUT SPEC (UI)
========================
decide(...) returns dict:
- action: "FORCE_REBALANCE" | "DD_ADD" | "HOLD"
- reason: str
- metrics: dict including:
  - basket_idx_today
  - trade_anchor_date
  - peak_trade
  - dd_trade
  - peak_1y (for display)
  - dd_display_1y (for display)
  - S, B, C, decision_base, W, target_stock_value, gap_to_target
- constraints_hit: list[str]
- unfilled_buy_amount: float
- unfilled_sell_amount: float
- orders:
  - summary totals
  - per_fund allocations
- new_state: dict (persist)

"""

from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple
import pandas as pd


# ----------------------------
# Parameters & state
# ----------------------------

@dataclass
class DualTriggerParams:
    # Target on decision base (S+B)
    target_stock_w: float = 0.30

    # Forced bands for W = S/(S+B)
    force_low_w: float = 0.20
    force_high_w: float = 0.40

    # FORCE_HIGH smoothing target (sell down only to this weight)
    force_high_target_w: float = 0.35

    # DD staged add based on DD_trade
    dd_stages: Tuple[float, float, float] = (0.20, 0.30, 0.40)
    gap_fill: Tuple[float, float, float] = (0.40, 0.70, 1.00)

    # Rolling window for DISPLAY (1Y peak)
    peak_rolling_days: int = 252

    # Cooldown after FORCE_LOW (calendar days)
    force_low_cooldown_days: int = 60

    # Decision frequency
    decision_freq: str = "weekly"  # daily/weekly/monthly/quarterly

    # Floors
    min_bond_value: float = 0.0
    min_stock_total_value: float = 0.0

    # Inside-stock target weights (None => equal across funds)
    target_stock_fund_weights: Optional[Dict[str, float]] = None

    # Optional: clear dd_triggered when a NEW HIGH in trade cycle happens
    clear_dd_on_trade_new_high: bool = True

    # Manual reset date after last DD stage is fired (YYYY-MM-DD)
    # Empty/None means no manual reset will be scheduled.
    last_stage_reset_date: Optional[str] = None

    # tolerance
    eps: float = 1e-12


@dataclass
class DualTriggerState:
    dd_triggered: List[float] = None
    cooldown_until_date: Optional[str] = None
    last_decision_date: Optional[str] = None

    # Trade-cycle state
    trade_anchor_date: Optional[str] = None
    trade_peak: Optional[float] = None
    pending_reset_date: Optional[str] = None
    last_stage_wait_conversion: bool = False
    # False: use rolling-window peak DD for staged triggers
    # True:  use trade_peak DD for staged triggers
    use_trade_peak_for_dd: bool = False

    def __post_init__(self):
        if self.dd_triggered is None:
            self.dd_triggered = []


# ----------------------------
# Frequency gating
# ----------------------------

def _should_run_today(today: pd.Timestamp, last: Optional[str], freq: str) -> bool:
    if freq == "daily" or last is None:
        return True
    last_dt = pd.Timestamp(last)
    if freq == "weekly":
        return (today.isocalendar().week != last_dt.isocalendar().week) or (today.year != last_dt.year)
    if freq == "monthly":
        return (today.year != last_dt.year) or (today.month != last_dt.month)
    if freq == "quarterly":
        q = (today.month - 1) // 3
        q_last = (last_dt.month - 1) // 3
        return (today.year != last_dt.year) or (q != q_last)
    raise ValueError("decision_freq must be one of: daily/weekly/monthly/quarterly")


def _in_cooldown(today: pd.Timestamp, cooldown_until: Optional[str]) -> bool:
    if not cooldown_until:
        return False
    return today.normalize() < pd.Timestamp(cooldown_until).normalize()


# ----------------------------
# Basket index (equal weights 1/N)
# ----------------------------

def stock_basket_index_equal_weights(nav_df: pd.DataFrame, funds: List[str]) -> pd.Series:
    nav_df = nav_df.sort_index()
    missing = [f for f in funds if f not in nav_df.columns]
    if missing:
        raise ValueError(f"nav_df missing columns: {missing}")
    if not funds:
        raise ValueError("funds must be non-empty")

    ret = nav_df[funds].pct_change()
    basket_ret = ret.mean(axis=1).fillna(0.0)  # equal weights
    idx = (1.0 + basket_ret).cumprod()
    idx.name = f"basket_equal_{len(funds)}"
    return idx


# ----------------------------
# Allocation helpers
# ----------------------------

def _normalize_inside_weights(target: Optional[Dict[str, float]], funds: List[str]) -> Dict[str, float]:
    if target is None:
        n = len(funds)
        return {f: 1.0 / n for f in funds}
    w = {f: float(target.get(f, 0.0)) for f in funds}
    s = sum(w.values())
    if s <= 0:
        raise ValueError("Sum of target_stock_fund_weights must be >0, or set to None for equal weights.")
    return {f: w[f] / s for f in funds}


def allocate_buy_underweight(amount: float, stock_values: Dict[str, float], target_w: Dict[str, float]) -> Dict[str, float]:
    amount = float(max(0.0, amount))
    funds = list(target_w.keys())
    if amount <= 0:
        return {f: 0.0 for f in funds}
    S = float(sum(stock_values.get(f, 0.0) for f in funds))
    if S <= 0:
        return {f: amount * target_w[f] for f in funds}

    # Use post-buy stock total to avoid concentrating all buy amount into one fund.
    S_after = S + amount
    target_vals = {f: target_w[f] * S_after for f in funds}
    deficits = {f: max(0.0, target_vals[f] - float(stock_values.get(f, 0.0))) for f in funds}
    sum_def = sum(deficits.values())

    if sum_def > 0:
        alloc = {f: amount * (deficits[f] / sum_def) for f in funds}
        # Numerical residue adjustment.
        residue = amount - sum(alloc.values())
        if abs(residue) > 1e-10:
            key = max(funds, key=lambda x: deficits.get(x, 0.0))
            alloc[key] = alloc.get(key, 0.0) + residue
        return alloc
    return {f: amount * target_w[f] for f in funds}


def allocate_sell_overweight(amount: float, stock_values: Dict[str, float], target_w: Dict[str, float]) -> Dict[str, float]:
    amount = float(max(0.0, amount))
    funds = list(target_w.keys())
    S = float(sum(stock_values.get(f, 0.0) for f in funds))
    if S <= 0:
        return {f: 0.0 for f in funds}

    target_vals = {f: target_w[f] * S for f in funds}
    excess = {f: max(0.0, float(stock_values.get(f, 0.0)) - target_vals[f]) for f in funds}
    sum_ex = sum(excess.values())

    if sum_ex > 0:
        alloc = {f: amount * (excess[f] / sum_ex) for f in funds}
    else:
        curr_w = {f: float(stock_values.get(f, 0.0)) / S for f in funds}
        alloc = {f: amount * curr_w[f] for f in funds}

    return {f: min(float(stock_values.get(f, 0.0)), alloc[f]) for f in funds}


def split_buy_sources(buy_alloc: Dict[str, float], cash_used: float, bond_used: float) -> Tuple[Dict[str, float], Dict[str, float]]:
    total_buy = sum(buy_alloc.values())
    funds = list(buy_alloc.keys())
    if total_buy <= 0:
        return ({f: 0.0 for f in funds}, {f: 0.0 for f in funds})

    cash_used = float(max(0.0, cash_used))
    bond_used = float(max(0.0, bond_used))

    buy_cash, buy_bond = {}, {}
    for f, a in buy_alloc.items():
        ratio = a / total_buy
        buy_cash[f] = cash_used * ratio
        buy_bond[f] = bond_used * ratio
    return buy_cash, buy_bond


def sell_amount_to_reach_weight(S: float, B: float, w_target: float) -> float:
    # Sell x stocks and buy bonds: W'=(S-x)/(S+B). Want W'=w_target => x = S - w_target*(S+B)
    return max(0.0, float(S) - float(w_target) * float(S + B))


# ----------------------------
# Trade-cycle Peak update + DD_trade
# ----------------------------

def update_trade_peak_and_dd(
    today: pd.Timestamp,
    basket_idx: pd.Series,
    state: DualTriggerState,
    params: DualTriggerParams,
    allow_clear_dd: bool = True,
) -> Tuple[float, float, str]:
    """
    Implements exactly what you described:

    - If anchor is None: initialize anchor=today, peak_trade=basket_idx[today]
    - Else:
        peak_trade = max(peak_trade, basket_idx[today])
    - dd_trade = (peak_trade - idx_today)/peak_trade

    Returns: (idx_today, dd_trade, anchor_date_str)
    """
    idx_today = float(basket_idx.loc[today])

    if state.trade_anchor_date is None or state.trade_peak is None:
        state.trade_anchor_date = today.date().isoformat()
        state.trade_peak = idx_today
    else:
        # Daily update:
        if idx_today > float(state.trade_peak) + params.eps:
            state.trade_peak = idx_today
            if params.clear_dd_on_trade_new_high and allow_clear_dd:
                state.dd_triggered = []  # new cycle on trade new high

    peak_trade = float(state.trade_peak)
    dd_trade = (peak_trade - idx_today) / peak_trade if peak_trade > 0 else 0.0
    return idx_today, dd_trade, state.trade_anchor_date


def apply_pending_trade_reset_if_due(
    today: pd.Timestamp,
    basket_idx: pd.Series,
    state: DualTriggerState,
) -> Tuple[Optional[str], Optional[float]]:
    """
    If state.pending_reset_date is set and we already have basket data on/after that date
    (up to `today`), reset trade cycle baseline to that effective date.

    Returns: (applied_reset_date, applied_reset_peak)
    """
    pending = str(state.pending_reset_date or "").strip()
    if not pending:
        return None, None
    try:
        target_ts = pd.Timestamp(pending)
    except Exception:
        return None, None

    eligible = basket_idx.loc[:today]
    eligible = eligible.loc[eligible.index >= target_ts]
    if eligible.empty:
        return None, None

    effective_ts = eligible.index[0]
    effective_peak = float(eligible.iloc[0])
    state.trade_anchor_date = effective_ts.date().isoformat()
    state.trade_peak = effective_peak
    state.dd_triggered = []
    state.pending_reset_date = None
    state.last_stage_wait_conversion = False
    state.use_trade_peak_for_dd = True
    return state.trade_anchor_date, effective_peak


# ----------------------------
# Decision engine
# ----------------------------

def decide(
    today: pd.Timestamp,
    basket_idx: pd.Series,
    bond_value: float,
    cash_value: float,
    stock_values: Dict[str, float],
    funds: List[str],
    params: DualTriggerParams,
    state: DualTriggerState,
) -> Dict:
    if today not in basket_idx.index:
        raise ValueError("today not in basket index")

    # Compatibility/migration: if strategy is already in conversion workflow,
    # we should be in trade-peak DD mode.
    if (state.pending_reset_date or state.last_stage_wait_conversion) and (not state.use_trade_peak_for_dd):
        state.use_trade_peak_for_dd = True

    # 0) Sync pending reset date from params after last-stage DD_ADD has happened.
    #    This allows user to fill/modify conversion date AFTER last stage was triggered.
    if bool(state.last_stage_wait_conversion):
        reset_date = str(params.last_stage_reset_date or "").strip()
        state.pending_reset_date = reset_date or None

    # 1) Apply pending manual reset (if due)
    manual_reset_applied_date, manual_reset_applied_peak = apply_pending_trade_reset_if_due(today, basket_idx, state)

    # 2) Update trade-cycle peak & dd_trade(raw) daily.
    # Before first final DD stage, we still compute this, but staged trigger uses rolling DD.
    idx_today, dd_trade_raw, anchor_date = update_trade_peak_and_dd(
        today,
        basket_idx,
        state,
        params,
        allow_clear_dd=bool(state.use_trade_peak_for_dd),
    )

    # 3) Rolling-window DD (used for display, and for staged trigger before first final stage)
    window = basket_idx.loc[:today]
    if params.peak_rolling_days is not None:
        window = window.tail(int(params.peak_rolling_days))
    peak_1y = float(window.max())
    dd_display_1y = (peak_1y - idx_today) / peak_1y if peak_1y > 0 else 0.0

    dd_for_stage = dd_trade_raw if state.use_trade_peak_for_dd else dd_display_1y
    dd_mode = "trade_peak" if state.use_trade_peak_for_dd else "rolling_peak"

    # 4) Frequency gating (applies to DD_ADD only; FORCE_LOW/FORCE_HIGH always run)
    should_run_today = _should_run_today(today, state.last_decision_date, params.decision_freq)

    # 5) Cooldown check
    in_cd = _in_cooldown(today, state.cooldown_until_date)

    # 6) Portfolio values (cash excluded from ratios)
    inside_w = _normalize_inside_weights(params.target_stock_fund_weights, funds)
    S = float(sum(stock_values.get(f, 0.0) for f in funds))
    B = float(bond_value)
    C = float(cash_value)

    decision_base = S + B
    if decision_base <= 0:
        raise ValueError("S + B must be > 0")

    W = S / decision_base

    target_stock_value = params.target_stock_w * decision_base
    gap = target_stock_value - S

    max_bond_sell = max(0.0, B - params.min_bond_value)
    max_stock_sell = max(0.0, S - params.min_stock_total_value)

    constraints_hit: List[str] = []
    unfilled_buy_amount = 0.0
    unfilled_sell_amount = 0.0

    orders = {
        "summary": {
            "cash_buy_stock_total": 0.0,
            "sell_bond_buy_stock_total": 0.0,
            "sell_stock_buy_bond_total": 0.0,
        },
        "per_fund": {
            "buy_stock_by_fund_cash": {f: 0.0 for f in funds},
            "buy_stock_by_fund_bond": {f: 0.0 for f in funds},
            "sell_stock_by_fund_to_bond": {f: 0.0 for f in funds},
        }
    }

    def _build_buy_calc_info(buy_total: float, buy_alloc: Dict[str, float]) -> Dict:
        s_before = float(S)
        s_after = float(S + buy_total)
        rows: List[Dict] = []
        for f in funds:
            cur_val = float(stock_values.get(f, 0.0))
            target_after = float(inside_w.get(f, 0.0)) * s_after
            gap_val = max(0.0, target_after - cur_val)
            alloc_val = float(buy_alloc.get(f, 0.0))
            rows.append(
                {
                    "code": f,
                    "current_value": cur_val,
                    "target_after_value": target_after,
                    "gap_value": gap_val,
                    "alloc_value": alloc_val,
                }
            )
        return {
            "rule": "post_buy_target_gap_fill",
            "stock_total_before": s_before,
            "buy_total": float(buy_total),
            "stock_total_after": s_after,
            "funds": rows,
        }

    # ----------------------------
    # PRIORITY A: FORCE_LOW
    # ----------------------------
    if W < params.force_low_w:
        need_total = max(0.0, gap)
        cash_used = min(C, need_total)
        remaining = need_total - cash_used
        bond_used = min(max_bond_sell, max(0.0, remaining))
        buy_total = cash_used + bond_used

        if bond_used < remaining:
            constraints_hit.append("BOND_FLOOR_BLOCKED")
            unfilled_buy_amount = remaining - bond_used

        buy_alloc = allocate_buy_underweight(buy_total, stock_values, inside_w)
        buy_cash, buy_bond = split_buy_sources(buy_alloc, cash_used, bond_used)

        orders["summary"]["cash_buy_stock_total"] = cash_used
        orders["summary"]["sell_bond_buy_stock_total"] = bond_used
        orders["per_fund"]["buy_stock_by_fund_cash"] = buy_cash
        orders["per_fund"]["buy_stock_by_fund_bond"] = buy_bond

        # cooldown 60 days from today
        state.cooldown_until_date = (today.normalize() + pd.Timedelta(days=params.force_low_cooldown_days)).date().isoformat()

        # sync dd_triggered: mark stages met by current dd_trade (prevents immediate re-fire)
        met = [st for st in params.dd_stages if dd_for_stage >= st]
        state.dd_triggered = sorted(list(set(state.dd_triggered).union(met)))

        state.last_decision_date = today.date().isoformat()

        return {
            "action": "FORCE_REBALANCE",
            "reason": f"FORCE_LOW: W={W:.4f} < {params.force_low_w}",
            "metrics": {
                "today": today.date().isoformat(),
                "basket_idx_today": idx_today,
                "trade_anchor_date": anchor_date,
                "peak_trade": float(state.trade_peak),
                "dd_trade": dd_for_stage,
                "dd_trade_raw": dd_trade_raw,
                "dd_mode": dd_mode,
                "peak_1y": peak_1y,
                "dd_display_1y": dd_display_1y,
                "S": S, "B": B, "C": C,
                "decision_base": decision_base,
                "W": W,
                "target_stock_value": target_stock_value,
                "gap_to_target": gap,
                "max_bond_sell": max_bond_sell,
                "min_bond_value": params.min_bond_value,
                "cooldown_until_date": state.cooldown_until_date,
                "dd_triggered": list(state.dd_triggered),
                "buy_total_executed": buy_total,
                "buy_calc": _build_buy_calc_info(buy_total, buy_alloc),
                "pending_reset_date": state.pending_reset_date,
                "last_stage_wait_conversion": bool(state.last_stage_wait_conversion),
                "manual_reset_applied_date": manual_reset_applied_date,
                "manual_reset_applied_peak": manual_reset_applied_peak,
            },
            "constraints_hit": constraints_hit,
            "unfilled_buy_amount": float(unfilled_buy_amount),
            "unfilled_sell_amount": 0.0,
            "orders": orders,
            "new_state": asdict(state),
        }

    # ----------------------------
    # PRIORITY B: FORCE_HIGH smoothing
    # ----------------------------
    if W > params.force_high_w:
        desired_w = params.force_high_target_w
        if not (0.0 < desired_w < params.force_high_w):
            raise ValueError("force_high_target_w must be >0 and < force_high_w")

        sell_needed = sell_amount_to_reach_weight(S, B, desired_w)
        sell_total = min(max_stock_sell, sell_needed)

        if sell_total < sell_needed:
            constraints_hit.append("STOCK_FLOOR_BLOCKED")
            unfilled_sell_amount = sell_needed - sell_total

        sell_alloc = allocate_sell_overweight(sell_total, stock_values, inside_w)
        orders["summary"]["sell_stock_buy_bond_total"] = sum(sell_alloc.values())
        orders["per_fund"]["sell_stock_by_fund_to_bond"] = sell_alloc

        # sync dd_triggered with current dd_trade
        met = [st for st in params.dd_stages if dd_for_stage >= st]
        state.dd_triggered = sorted(list(set(state.dd_triggered).union(met)))

        state.last_decision_date = today.date().isoformat()

        return {
            "action": "FORCE_REBALANCE",
            "reason": f"FORCE_HIGH: W={W:.4f} > {params.force_high_w}; sell to W={desired_w:.2f}",
            "metrics": {
                "today": today.date().isoformat(),
                "basket_idx_today": idx_today,
                "trade_anchor_date": anchor_date,
                "peak_trade": float(state.trade_peak),
                "dd_trade": dd_for_stage,
                "dd_trade_raw": dd_trade_raw,
                "dd_mode": dd_mode,
                "peak_1y": peak_1y,
                "dd_display_1y": dd_display_1y,
                "S": S, "B": B, "C": C,
                "decision_base": decision_base,
                "W": W,
                "sell_needed_to_target_w": sell_needed,
                "sell_total_executed": sell_total,
                "max_stock_sell": max_stock_sell,
                "min_stock_total_value": params.min_stock_total_value,
                "dd_triggered": list(state.dd_triggered),
                "pending_reset_date": state.pending_reset_date,
                "last_stage_wait_conversion": bool(state.last_stage_wait_conversion),
                "manual_reset_applied_date": manual_reset_applied_date,
                "manual_reset_applied_peak": manual_reset_applied_peak,
            },
            "constraints_hit": constraints_hit,
            "unfilled_buy_amount": 0.0,
            "unfilled_sell_amount": float(unfilled_sell_amount),
            "orders": orders,
            "new_state": asdict(state),
        }

    # ----------------------------
    # PRIORITY C: DD staged add (uses dd_trade) — only if NOT in cooldown
    # ----------------------------
    if not should_run_today:
        return {
            "action": "HOLD",
            "reason": f"freq_gate({params.decision_freq})",
            "metrics": {
                "today": today.date().isoformat(),
                "basket_idx_today": idx_today,
                "trade_anchor_date": anchor_date,
                "peak_trade": float(state.trade_peak),
                "dd_trade": dd_for_stage,
                "dd_trade_raw": dd_trade_raw,
                "dd_mode": dd_mode,
                "peak_1y": peak_1y,
                "dd_display_1y": dd_display_1y,
                "S": S, "B": B, "C": C,
                "decision_base": decision_base,
                "W": W,
                "target_stock_value": target_stock_value,
                "gap_to_target": gap,
                "cooldown_until_date": state.cooldown_until_date,
                "dd_triggered": list(state.dd_triggered),
                "pending_reset_date": state.pending_reset_date,
                "last_stage_wait_conversion": bool(state.last_stage_wait_conversion),
                "manual_reset_applied_date": manual_reset_applied_date,
                "manual_reset_applied_peak": manual_reset_applied_peak,
            },
            "constraints_hit": [],
            "unfilled_buy_amount": 0.0,
            "unfilled_sell_amount": 0.0,
            "orders": orders,
            "new_state": asdict(state),
        }

    if not in_cd:
        stage_to_fire = None
        fill_ratio = None
        for st, fr in zip(params.dd_stages, params.gap_fill):
            if dd_for_stage >= st and st not in state.dd_triggered:
                stage_to_fire = st
                fill_ratio = fr  # ends as highest newly crossed stage

        if stage_to_fire is not None:
            need_gap = max(0.0, gap)
            add_plan = need_gap * float(fill_ratio)

            cash_used = min(C, add_plan)
            remaining = add_plan - cash_used
            bond_used = min(max_bond_sell, max(0.0, remaining))
            add_total = cash_used + bond_used

            if bond_used < remaining:
                constraints_hit.append("BOND_FLOOR_BLOCKED")
                unfilled_buy_amount = remaining - bond_used

            buy_alloc = allocate_buy_underweight(add_total, stock_values, inside_w)
            buy_cash, buy_bond = split_buy_sources(buy_alloc, cash_used, bond_used)

            orders["summary"]["cash_buy_stock_total"] = cash_used
            orders["summary"]["sell_bond_buy_stock_total"] = bond_used
            orders["per_fund"]["buy_stock_by_fund_cash"] = buy_cash
            orders["per_fund"]["buy_stock_by_fund_bond"] = buy_bond

            state.dd_triggered = sorted(list(set(state.dd_triggered + [stage_to_fire])))
            state.last_decision_date = today.date().isoformat()

            # If this is the final DD stage with full gap-fill, schedule manual reset date.
            stages = list(params.dd_stages or [])
            fills = list(params.gap_fill or [])
            last_stage = stages[-1] if stages else None
            last_fill = fills[-1] if fills else None
            is_last_stage_full_fill = (
                (last_stage is not None)
                and (last_fill is not None)
                and (stage_to_fire is not None)
                and (abs(float(stage_to_fire) - float(last_stage)) < 1e-9)
                and (abs(float(fill_ratio) - float(last_fill)) < 1e-9)
            )
            if is_last_stage_full_fill:
                reset_date = str(params.last_stage_reset_date or "").strip()
                state.pending_reset_date = reset_date or None
                state.last_stage_wait_conversion = True
                state.use_trade_peak_for_dd = True

            return {
                "action": "DD_ADD",
                "reason": f"DD_ADD: dd_trade={dd_for_stage:.2%} crossed {stage_to_fire:.0%}, fill_gap={fill_ratio:.0%}",
                "metrics": {
                    "today": today.date().isoformat(),
                    "basket_idx_today": idx_today,
                    "trade_anchor_date": state.trade_anchor_date,
                    "peak_trade": float(state.trade_peak),
                    "dd_trade": dd_for_stage,
                    "dd_trade_raw": dd_trade_raw,
                    "dd_mode": dd_mode,
                    "peak_1y": peak_1y,
                    "dd_display_1y": dd_display_1y,
                    "S": S, "B": B, "C": C,
                    "decision_base": decision_base,
                    "W": W,
                    "target_stock_value": target_stock_value,
                    "gap_to_target": gap,
                    "dd_stage_fired": stage_to_fire,
                    "gap_fill_ratio": float(fill_ratio),
                    "add_plan": add_plan,
                    "add_total_executed": add_total,
                    "buy_calc": _build_buy_calc_info(add_total, buy_alloc),
                    "cooldown_until_date": state.cooldown_until_date,
                    "dd_triggered": list(state.dd_triggered),
                    "trade_cycle_reset_on_last_stage": is_last_stage_full_fill,
                    "pending_reset_date": state.pending_reset_date,
                    "last_stage_wait_conversion": bool(state.last_stage_wait_conversion),
                    "manual_reset_applied_date": manual_reset_applied_date,
                    "manual_reset_applied_peak": manual_reset_applied_peak,
                },
                "constraints_hit": constraints_hit,
                "unfilled_buy_amount": float(unfilled_buy_amount),
                "unfilled_sell_amount": 0.0,
                "orders": orders,
                "new_state": asdict(state),
            }

    # HOLD
    state.last_decision_date = today.date().isoformat()
    return {
        "action": "HOLD",
        "reason": "no trigger" + (" (cooldown)" if in_cd else ""),
        "metrics": {
            "today": today.date().isoformat(),
            "basket_idx_today": idx_today,
            "trade_anchor_date": anchor_date,
            "peak_trade": float(state.trade_peak),
            "dd_trade": dd_for_stage,
            "dd_trade_raw": dd_trade_raw,
            "dd_mode": dd_mode,
            "peak_1y": peak_1y,
            "dd_display_1y": dd_display_1y,
            "S": S, "B": B, "C": C,
            "decision_base": decision_base,
            "W": W,
            "target_stock_value": target_stock_value,
            "gap_to_target": gap,
            "cooldown_until_date": state.cooldown_until_date,
            "dd_triggered": list(state.dd_triggered),
            "pending_reset_date": state.pending_reset_date,
            "last_stage_wait_conversion": bool(state.last_stage_wait_conversion),
            "manual_reset_applied_date": manual_reset_applied_date,
            "manual_reset_applied_peak": manual_reset_applied_peak,
        },
        "constraints_hit": [],
        "unfilled_buy_amount": 0.0,
        "unfilled_sell_amount": 0.0,
        "orders": orders,
        "new_state": asdict(state),
    }


# ----------------------------
# Example (manual test)
# ----------------------------
if __name__ == "__main__":
    import numpy as np

    dates = pd.bdate_range("2025-01-01", "2025-12-31")
    rng = np.random.default_rng(7)
    nav_df = pd.DataFrame(
        {
            "A": 1.0 * np.cumprod(1 + rng.normal(0.0002, 0.010, len(dates))),
            "B": 1.0 * np.cumprod(1 + rng.normal(0.0001, 0.011, len(dates))),
            "C": 1.0 * np.cumprod(1 + rng.normal(0.00015, 0.012, len(dates))),
            "D": 1.0 * np.cumprod(1 + rng.normal(0.00005, 0.009, len(dates))),
        },
        index=dates,
    )

    funds = ["A", "B", "C", "D"]
    basket = stock_basket_index_equal_weights(nav_df, funds)

    bond_value = 700_000.0
    cash_value = 50_000.0
    stock_values = {"A": 70_000.0, "B": 80_000.0, "C": 60_000.0, "D": 40_000.0}

    params = DualTriggerParams(
        target_stock_w=0.30,
        force_low_w=0.20,
        force_high_w=0.40,
        force_high_target_w=0.35,
        dd_stages=(0.20, 0.30, 0.40),
        gap_fill=(0.40, 0.70, 1.00),
        peak_rolling_days=252,
        force_low_cooldown_days=60,
        decision_freq="weekly",
        min_bond_value=100_000.0,
        min_stock_total_value=50_000.0,
        target_stock_fund_weights=None,  # None => equal inside
        clear_dd_on_trade_new_high=True,
    )

    state = DualTriggerState()

    today = basket.index[-1]
    out = decide(today, basket, bond_value, cash_value, stock_values, funds, params, state)

    print("=== DECISION ===")
    print("Action:", out["action"])
    print("Reason:", out["reason"])
    print("Constraints:", out["constraints_hit"])
    print("Metrics:", out["metrics"])
    print("Orders summary:", out["orders"]["summary"])
    print("Buy by fund (cash):", out["orders"]["per_fund"]["buy_stock_by_fund_cash"])
    print("Buy by fund (bond):", out["orders"]["per_fund"]["buy_stock_by_fund_bond"])
    print("Sell by fund (to bond):", out["orders"]["per_fund"]["sell_stock_by_fund_to_bond"])
    print("New state:", out["new_state"])
