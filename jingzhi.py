"""
Dual-trigger allocator (Bond + N Equity Funds as a Stock Basket) — v3

You requested v3 changes:
✅ Cooldown = 60 DAYS (calendar days), not "period counts"
✅ Reset dd_triggered when basket index makes a NEW HIGH (创新高清空)

Also included (highly recommended for UI & correctness):
✅ Explicit constraint feedback:
   - constraints_hit: list[str]
   - unfilled_buy_amount / unfilled_sell_amount

Core behavior kept from v2:
- Basket index uses FIXED EQUAL WEIGHTS based on number of funds (N => 1/N each)
- Cash does NOT participate in decision ratios (decision_base = S + B)
- FORCE_LOW: if W < force_low_w, buy stocks towards target (cash first, then sell bonds)
- FORCE_HIGH smoothing: if W > force_high_w, sell ONLY down to force_high_target_w
- DD staged add: dd>=20/30/40 triggers staged buys (event trigger, one-time per stage)
- Floors:
  - min_bond_value: cannot sell bonds below this
  - min_stock_total_value: cannot sell total stocks below this

========================
INPUT SPEC (for UI)
========================
A) nav_df: DataFrame
   - index: datetime (trading days)
   - columns: stock fund ids (strings)
   - values: NAV floats

B) funds: List[str]
   - which columns in nav_df are included in the basket

C) bond_value: float
D) cash_value: float  (funding only, excluded from ratios)
E) stock_values: Dict[str,float]  (per fund current market value)
   - keys should match funds (missing keys treated as 0)

F) params: DualTriggerParams (see below)
G) state: DualTriggerState (persist across runs)
H) today: pd.Timestamp (must exist in basket index)

========================
OUTPUT SPEC (for UI)
========================
decide(...) returns dict:
- action: "FORCE_REBALANCE" | "DD_ADD" | "HOLD"
- reason: str
- metrics: dict (dd, W, gap, floors, etc.)
- constraints_hit: list[str]
- unfilled_buy_amount: float
- unfilled_sell_amount: float
- orders:
  - summary totals
  - per_fund allocations for buys/sells
- new_state: dict (save for next run)
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
    # Structure target on decision base (S+B)
    target_stock_w: float = 0.30

    # Forced bands for W = S/(S+B)
    force_low_w: float = 0.20
    force_high_w: float = 0.40

    # When FORCE_HIGH triggers, sell only down to this smoother weight (e.g. 35%)
    force_high_target_w: float = 0.35

    # DD stages (basket drawdown)
    dd_stages: Tuple[float, float, float] = (0.20, 0.30, 0.40)
    gap_fill: Tuple[float, float, float] = (0.40, 0.70, 1.00)

    # Peak window for DD (rolling)
    peak_rolling_days: Optional[int] = 252

    # Cooldown after FORCE_LOW (calendar days)
    force_low_cooldown_days: int = 60

    # Decision frequency
    decision_freq: str = "weekly"  # daily/weekly/monthly/quarterly

    # Floors (cannot sell below)
    min_bond_value: float = 0.0
    min_stock_total_value: float = 0.0

    # Target weights inside stock bucket (if None => equal weights across funds)
    target_stock_fund_weights: Optional[Dict[str, float]] = None

    # New-high reset tolerance
    new_high_epsilon: float = 1e-10


@dataclass
class DualTriggerState:
    # Stages already triggered in current cycle
    dd_triggered: List[float] = None

    # Cooldown end date (YYYY-MM-DD) or None
    cooldown_until_date: Optional[str] = None

    # Last decision date (YYYY-MM-DD) or None (for freq gating)
    last_decision_date: Optional[str] = None

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
# Basket index with EQUAL weights (N => 1/N)
# ----------------------------

def stock_basket_index_equal_weights(nav_df: pd.DataFrame, funds: List[str]) -> pd.Series:
    nav_df = nav_df.sort_index()
    missing = [f for f in funds if f not in nav_df.columns]
    if missing:
        raise ValueError(f"nav_df missing columns for funds: {missing}")
    if len(funds) == 0:
        raise ValueError("funds must be non-empty")

    ret = nav_df[funds].pct_change()
    basket_ret = ret.mean(axis=1).fillna(0.0)  # equal weights => average return
    idx = (1.0 + basket_ret).cumprod()
    idx.name = f"stock_basket_index_equal_{len(funds)}"
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
        raise ValueError("Sum of target_stock_fund_weights must be >0, or set it to None for equal weights.")
    return {f: w[f] / s for f in funds}


def allocate_buy_underweight(amount: float, stock_values: Dict[str, float], target_w: Dict[str, float]) -> Dict[str, float]:
    amount = float(max(0.0, amount))
    funds = list(target_w.keys())
    S = float(sum(stock_values.get(f, 0.0) for f in funds))
    if S <= 0:
        return {f: amount * target_w[f] for f in funds}

    target_vals = {f: target_w[f] * S for f in funds}
    deficits = {f: max(0.0, target_vals[f] - float(stock_values.get(f, 0.0))) for f in funds}
    sum_def = sum(deficits.values())

    if sum_def > 0:
        return {f: amount * (deficits[f] / sum_def) for f in funds}
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

    # clip
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
        raise ValueError("today not found in basket index series")

    # 0) Reset dd_triggered on NEW HIGH (创新高清空)
    # Define "new high" as basket index reaching current rolling peak -> dd ~= 0
    idx_today = float(basket_idx.loc[today])
    if params.peak_rolling_days is None:
        peak_for_reset = float(basket_idx.loc[:today].max())
    else:
        peak_for_reset = float(basket_idx.loc[:today].tail(params.peak_rolling_days).max())

    if idx_today >= peak_for_reset * (1.0 - params.new_high_epsilon):
        # new high (or equal within tolerance)
        state.dd_triggered = []

    # 1) Cooldown check (calendar days)
    in_cd = _in_cooldown(today, state.cooldown_until_date)

    # 2) Portfolio values (cash excluded from decision base)
    inside_w = _normalize_inside_weights(params.target_stock_fund_weights, funds)
    S = float(sum(stock_values.get(f, 0.0) for f in funds))
    B = float(bond_value)
    C = float(cash_value)

    decision_base = S + B
    if decision_base <= 0:
        raise ValueError("S + B must be > 0 (cash excluded from decision base).")

    W = S / decision_base

    # 3) Drawdown DD (for decisions)
    if params.peak_rolling_days is None:
        peak = float(basket_idx.loc[:today].max())
    else:
        peak = float(basket_idx.loc[:today].tail(params.peak_rolling_days).max())

    dd = (peak - idx_today) / peak if peak > 0 else 0.0

    # 4) Gap to target stock (on S+B)
    target_stock_value = params.target_stock_w * decision_base
    gap = target_stock_value - S  # >0 need buy stocks, <0 need sell stocks

    # 5) Floors / caps
    max_bond_sell = max(0.0, B - params.min_bond_value)
    max_stock_sell = max(0.0, S - params.min_stock_total_value)

    constraints_hit: List[str] = []
    unfilled_buy_amount = 0.0
    unfilled_sell_amount = 0.0

    # Orders
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

    # Helper: sync dd_triggered after FORCE actions (mark all stages already met)
    def sync_dd_triggered():
        met = [st for st in params.dd_stages if dd >= st]
        state.dd_triggered = sorted(list(set(state.dd_triggered).union(met)))

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
        cooldown_until = (today.normalize() + pd.Timedelta(days=params.force_low_cooldown_days)).date().isoformat()
        state.cooldown_until_date = cooldown_until

        sync_dd_triggered()
        state.last_decision_date = today.date().isoformat()

        return {
            "action": "FORCE_REBALANCE",
            "reason": f"FORCE_LOW: W={W:.4f} < {params.force_low_w}",
            "metrics": {
                "today": today.date().isoformat(),
                "stock_basket_index": idx_today,
                "peak": peak,
                "dd": dd,
                "S": S, "B": B, "C": C,
                "decision_base_S_plus_B": decision_base,
                "stock_weight_W": W,
                "target_stock_value": target_stock_value,
                "gap_to_target": gap,
                "max_bond_sell": max_bond_sell,
                "min_bond_value": params.min_bond_value,
                "cooldown_until_date": state.cooldown_until_date,
                "dd_triggered": list(state.dd_triggered),
                "buy_total_executed": buy_total,
            },
            "constraints_hit": constraints_hit,
            "unfilled_buy_amount": float(unfilled_buy_amount),
            "unfilled_sell_amount": 0.0,
            "orders": orders,
            "new_state": asdict(state),
        }

    # ----------------------------
    # PRIORITY B: FORCE_HIGH smoothing (sell only to force_high_target_w)
    # ----------------------------
    if W > params.force_high_w:
        desired_w = params.force_high_target_w
        if not (0.0 < desired_w < params.force_high_w):
            raise ValueError("force_high_target_w must be >0 and < force_high_w.")

        sell_needed = sell_amount_to_reach_weight(S, B, desired_w)
        sell_total = min(max_stock_sell, sell_needed)

        if sell_total < sell_needed:
            constraints_hit.append("STOCK_FLOOR_BLOCKED")
            unfilled_sell_amount = sell_needed - sell_total

        sell_alloc = allocate_sell_overweight(sell_total, stock_values, inside_w)

        orders["summary"]["sell_stock_buy_bond_total"] = sum(sell_alloc.values())
        orders["per_fund"]["sell_stock_by_fund_to_bond"] = sell_alloc

        sync_dd_triggered()
        state.last_decision_date = today.date().isoformat()

        return {
            "action": "FORCE_REBALANCE",
            "reason": f"FORCE_HIGH: W={W:.4f} > {params.force_high_w}; sell to W={desired_w:.2f}",
            "metrics": {
                "today": today.date().isoformat(),
                "stock_basket_index": idx_today,
                "peak": peak,
                "dd": dd,
                "S": S, "B": B, "C": C,
                "decision_base_S_plus_B": decision_base,
                "stock_weight_W": W,
                "force_high_target_w": desired_w,
                "sell_needed_to_target_w": sell_needed,
                "sell_total_executed": sell_total,
                "max_stock_sell": max_stock_sell,
                "min_stock_total_value": params.min_stock_total_value,
                "dd_triggered": list(state.dd_triggered),
            },
            "constraints_hit": constraints_hit,
            "unfilled_buy_amount": 0.0,
            "unfilled_sell_amount": float(unfilled_sell_amount),
            "orders": orders,
            "new_state": asdict(state),
        }

    # Frequency gate only blocks non-forced decisions.
    if not _should_run_today(today, state.last_decision_date, params.decision_freq):
        sync_dd_triggered()
        return {
            "action": "HOLD",
            "reason": f"freq_gate({params.decision_freq})",
            "metrics": {
                "today": today.date().isoformat(),
                "stock_basket_index": idx_today,
                "peak": peak,
                "dd": dd,
                "S": S, "B": B, "C": C,
                "decision_base_S_plus_B": decision_base,
                "stock_weight_W": W,
                "target_stock_value": target_stock_value,
                "gap_to_target": gap,
                "max_bond_sell": max_bond_sell,
                "max_stock_sell": max_stock_sell,
                "min_bond_value": params.min_bond_value,
                "min_stock_total_value": params.min_stock_total_value,
                "cooldown_until_date": state.cooldown_until_date,
                "dd_triggered": list(state.dd_triggered),
            },
            "constraints_hit": [],
            "unfilled_buy_amount": 0.0,
            "unfilled_sell_amount": 0.0,
            "orders": orders,
            "new_state": asdict(state),
        }

    # ----------------------------
    # PRIORITY C: DD staged add (only if NOT in cooldown)
    # ----------------------------
    if not in_cd:
        stage_to_fire = None
        fill_ratio = None
        for st, fr in zip(params.dd_stages, params.gap_fill):
            if dd >= st and st not in state.dd_triggered:
                stage_to_fire = st
                fill_ratio = fr  # ends at highest newly crossed stage

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

            return {
                "action": "DD_ADD",
                "reason": f"DD_ADD: dd={dd:.2%} crossed {stage_to_fire:.0%}, fill_gap={fill_ratio:.0%}",
                "metrics": {
                    "today": today.date().isoformat(),
                    "stock_basket_index": idx_today,
                    "peak": peak,
                    "dd": dd,
                    "S": S, "B": B, "C": C,
                    "decision_base_S_plus_B": decision_base,
                    "stock_weight_W": W,
                    "target_stock_value": target_stock_value,
                    "gap_to_target": gap,
                    "dd_stage_fired": stage_to_fire,
                    "gap_fill_ratio": float(fill_ratio),
                    "add_plan": add_plan,
                    "add_total_executed": add_total,
                    "max_bond_sell": max_bond_sell,
                    "min_bond_value": params.min_bond_value,
                    "cooldown_until_date": state.cooldown_until_date,
                    "dd_triggered": list(state.dd_triggered),
                },
                "constraints_hit": constraints_hit,
                "unfilled_buy_amount": float(unfilled_buy_amount),
                "unfilled_sell_amount": 0.0,
                "orders": orders,
                "new_state": asdict(state),
            }

    # HOLD
    sync_dd_triggered()
    state.last_decision_date = today.date().isoformat()

    return {
        "action": "HOLD",
        "reason": "no trigger" + (" (cooldown)" if in_cd else ""),
        "metrics": {
            "today": today.date().isoformat(),
            "stock_basket_index": idx_today,
            "peak": peak,
            "dd": dd,
            "S": S, "B": B, "C": C,
            "decision_base_S_plus_B": decision_base,
            "stock_weight_W": W,
            "target_stock_value": target_stock_value,
            "gap_to_target": gap,
            "max_bond_sell": max_bond_sell,
            "max_stock_sell": max_stock_sell,
            "min_bond_value": params.min_bond_value,
            "min_stock_total_value": params.min_stock_total_value,
            "cooldown_until_date": state.cooldown_until_date,
            "dd_triggered": list(state.dd_triggered),
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
        target_stock_fund_weights=None,  # None => equal across funds
    )

    state = DualTriggerState()

    today = basket.index[-1]
    out = decide(today, basket, bond_value, cash_value, stock_values, funds, params, state)

    print("=== DECISION ===")
    print("Action:", out["action"])
    print("Reason:", out["reason"])
    print("Constraints:", out["constraints_hit"])
    print("Unfilled buy:", out["unfilled_buy_amount"])
    print("Unfilled sell:", out["unfilled_sell_amount"])
    print("Metrics:", out["metrics"])
    print("Orders summary:", out["orders"]["summary"])
    print("Buy by fund (cash):", out["orders"]["per_fund"]["buy_stock_by_fund_cash"])
    print("Buy by fund (bond):", out["orders"]["per_fund"]["buy_stock_by_fund_bond"])
    print("Sell by fund (to bond):", out["orders"]["per_fund"]["sell_stock_by_fund_to_bond"])
    print("New state:", out["new_state"])
