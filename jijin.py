import argparse
import datetime as dt
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import openpyxl
import requests
import yaml


EASTMONEY_LSJZ_URL = "https://api.fund.eastmoney.com/f10/lsjz"
TX_HEADERS = ["date", "fund_code", "action", "shares", "nav", "note"]
SYSTEM_SHEETS = {"summary", "positions", "汇总", "持仓明细"}
TEMPLATE_SHEET_NAME = "模板"
META_NAME_LABEL = "基金名称"
META_CODE_LABEL = "基金代码"
META_NAV_LABEL = "最新净值"
BASE_DATA_HEADERS = ["日期", "份额", "购买净值", "备注"]
DATA_HEADERS = BASE_DATA_HEADERS + ["最新净值", "盈利利率", "盈利额"]
DATA_HEADER_ROW = 5
DATA_START_ROW = DATA_HEADER_ROW + 1

DEFAULT_CONFIG = {
    "app": {
        "xlsx_path": "data/jijin.xlsx",
        "threshold": 0.40,
        "ntfy_url": "",
    },
    "funds": [],
}


def deep_merge(base: Dict, patch: Dict) -> Dict:
    result = dict(base)
    for key, value in (patch or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def normalize_fund_code(value) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    if s.isdigit():
        s = s.zfill(6)
    if re.fullmatch(r"\d{6}", s):
        return s
    return ""


def to_float(value, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().replace(",", "")
    if not s:
        return default
    try:
        return float(s)
    except ValueError:
        return default


def parse_date(value) -> str:
    if value is None:
        return ""
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    s = str(value).strip()
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d"):
        try:
            return dt.datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return s


def row_headers(ws, row_idx: int, width: int) -> List[str]:
    return [str(ws.cell(row=row_idx, column=i + 1).value or "").strip() for i in range(width)]


def parse_sheet_meta(ws) -> Tuple[str, str, Optional[float]]:
    fund_name = str(ws.cell(row=1, column=2).value or "").strip()
    label_code = str(ws.cell(row=2, column=1).value or "").strip()
    code = normalize_fund_code(ws.cell(row=2, column=2).value)
    latest_nav = to_float(ws.cell(row=3, column=2).value, default=None)
    if label_code != META_CODE_LABEL:
        return fund_name, "", latest_nav
    return fund_name, code, latest_nav


def is_transaction_sheet(ws) -> bool:
    _, code, _ = parse_sheet_meta(ws)
    if not code:
        return False
    base_headers = row_headers(ws, DATA_HEADER_ROW, len(BASE_DATA_HEADERS))
    if base_headers != BASE_DATA_HEADERS:
        return False
    full_headers = row_headers(ws, DATA_HEADER_ROW, len(DATA_HEADERS))
    return full_headers == DATA_HEADERS or full_headers[: len(BASE_DATA_HEADERS)] == BASE_DATA_HEADERS


def init_template_sheet(ws) -> None:
    ws.delete_rows(1, ws.max_row)
    ws.cell(row=1, column=1).value = META_NAME_LABEL
    ws.cell(row=1, column=2).value = "示例基金"
    ws.cell(row=2, column=1).value = META_CODE_LABEL
    ws.cell(row=2, column=2).value = ""
    ws.cell(row=3, column=1).value = META_NAV_LABEL
    ws.cell(row=3, column=2).value = ""
    for i, h in enumerate(DATA_HEADERS, start=1):
        ws.cell(row=DATA_HEADER_ROW, column=i).value = h


def resolve_transaction_sheet_names(wb: openpyxl.Workbook) -> List[str]:
    return [
        name
        for name in wb.sheetnames
        if name not in SYSTEM_SHEETS and name != "transactions" and is_transaction_sheet(wb[name])
    ]


def load_config(path: Path) -> Dict:
    if not path.exists():
        return deep_merge(DEFAULT_CONFIG, {})
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return deep_merge(DEFAULT_CONFIG, raw)


def normalize_ntfy_url(ntfy_url: str) -> str:
    url = str(ntfy_url or "").strip()
    if not url:
        return ""
    if not re.match(r"^[A-Za-z][A-Za-z0-9+.\-]*://", url):
        url = f"https://{url.lstrip('/')}"
    return url


def ensure_transactions_workbook(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        wb = openpyxl.load_workbook(path)
        has_transaction_data_sheet = any(
            name not in SYSTEM_SHEETS and name != "transactions" and is_transaction_sheet(wb[name])
            for name in wb.sheetnames
        )
        if has_transaction_data_sheet:
            return

        ws = wb[TEMPLATE_SHEET_NAME] if TEMPLATE_SHEET_NAME in wb.sheetnames else wb.create_sheet(TEMPLATE_SHEET_NAME)
        init_template_sheet(ws)
        wb.save(path)
        return

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = TEMPLATE_SHEET_NAME
    init_template_sheet(ws)
    wb.save(path)


def load_transactions(xlsx_path: Path) -> List[Dict]:
    ensure_transactions_workbook(xlsx_path)
    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    target_sheet_names = resolve_transaction_sheet_names(wb)

    txns: List[Dict] = []
    uid = 1
    for sheet_name in target_sheet_names:
        ws = wb[sheet_name]
        _, code, _ = parse_sheet_meta(ws)
        for row in ws.iter_rows(min_row=DATA_START_ROW, values_only=True):
            date_str = parse_date(row[0]) if len(row) > 0 else ""
            shares_raw = to_float(row[1] if len(row) > 1 else None, default=None)
            nav = to_float(row[2] if len(row) > 2 else None, default=None)
            note = str(row[3] if len(row) > 3 and row[3] is not None else "").strip()
            if shares_raw is None or abs(shares_raw) <= 0:
                continue
            action = "SELL" if shares_raw < 0 else "BUY"
            shares = abs(float(shares_raw))

            txns.append(
                {
                    "uid": uid,
                    "date": date_str or dt.date.today().isoformat(),
                    "fund_code": code,
                    "action": action,
                    "shares": shares,
                    "nav": float(nav) if nav is not None else None,
                    "note": note,
                }
            )
            uid += 1

    txns.sort(key=lambda x: (x["date"], x["uid"]))
    return txns


def load_fund_name_map(xlsx_path: Path) -> Dict[str, str]:
    if not xlsx_path.exists():
        return {}
    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    name_map: Dict[str, str] = {}
    for name in resolve_transaction_sheet_names(wb):
        ws = wb[name]
        fund_name, code, _ = parse_sheet_meta(ws)
        if code:
            name_map[code] = fund_name or name
    return name_map


def fetch_latest_nav(code: str, session: requests.Session) -> Tuple[str, float]:
    params = {
        "fundCode": code,
        "pageIndex": 1,
        "pageSize": 1,
        "startDate": "",
        "endDate": "",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://fundf10.eastmoney.com/",
        "Accept": "application/json, text/plain, */*",
    }

    resp = session.get(EASTMONEY_LSJZ_URL, params=params, headers=headers, timeout=20)
    resp.raise_for_status()
    text = resp.text.strip()

    if text.startswith(("jQuery", "jsonp", "callback")) and "(" in text:
        text = re.sub(r"^[^(]+\(", "", text)
        text = re.sub(r"\)\s*;?\s*$", "", text)

    data = json.loads(text)
    item = data["Data"]["LSJZList"][0]
    return str(item["FSRQ"]), float(item["DWJZ"])


def build_positions(transactions: List[Dict]) -> Tuple[List[Dict], float]:
    lots: List[Dict] = []
    oversold = 0.0

    for txn in sorted(transactions, key=lambda x: (x["date"], x["uid"])):
        if txn["action"] == "BUY":
            lots.append(
                {
                    "source_uid": txn["uid"],
                    "buy_date": txn["date"],
                    "buy_nav": float(txn["nav"] or 0.0),
                    "remaining_shares": float(txn["shares"]),
                }
            )
            continue

        remain_to_sell = float(txn["shares"])
        for lot in lots:
            if remain_to_sell <= 0:
                break
            if lot["remaining_shares"] <= 0:
                continue
            can_consume = min(lot["remaining_shares"], remain_to_sell)
            lot["remaining_shares"] -= can_consume
            remain_to_sell -= can_consume

        if remain_to_sell > 1e-12:
            oversold += remain_to_sell

    clean_lots = [x for x in lots if x["remaining_shares"] > 1e-12]
    return clean_lots, oversold


def summarize_fund(transactions: List[Dict], nav_date: str, latest_nav: float) -> Dict:
    lots, oversold = build_positions(transactions)

    holding_shares = sum(x["remaining_shares"] for x in lots)
    total_cost = sum(x["remaining_shares"] * x["buy_nav"] for x in lots)
    market_value = holding_shares * latest_nav
    profit_amount = market_value - total_cost
    profit_rate = (profit_amount / total_cost) if total_cost > 0 else 0.0

    for lot in lots:
        lot_cost = lot["remaining_shares"] * lot["buy_nav"]
        lot_profit = lot["remaining_shares"] * (latest_nav - lot["buy_nav"])
        lot["cost"] = lot_cost
        lot["latest_nav"] = latest_nav
        lot["profit_amount"] = lot_profit
        lot["profit_rate"] = (lot_profit / lot_cost) if lot_cost > 0 else 0.0

    return {
        "nav_date": nav_date,
        "latest_nav": latest_nav,
        "holding_shares": holding_shares,
        "total_cost": total_cost,
        "market_value": market_value,
        "profit_amount": profit_amount,
        "profit_rate": profit_rate,
        "oversold": oversold,
        "lots": lots,
    }


def write_output(input_path: Path, output_path: Path, summaries: Dict[str, Dict]) -> None:
    ensure_transactions_workbook(input_path)
    wb = openpyxl.load_workbook(input_path)

    for name in ("summary", "positions", "汇总", "持仓明细"):
        if name in wb.sheetnames:
            wb.remove(wb[name])

    summary_ws = wb.create_sheet("汇总")
    summary_ws.append(
        [
            "基金代码",
            "净值日期",
            "最新净值",
            "持仓份额",
            "总成本",
            "市值",
            "盈利金额",
            "盈利比例",
            "超卖份额",
        ]
    )

    positions_ws = wb.create_sheet("持仓明细")
    positions_ws.append(
        [
            "基金代码",
            "买入日期",
            "买入净值",
            "剩余份额",
            "成本",
            "最新净值",
            "盈利金额",
            "盈利比例",
        ]
    )

    for code in sorted(summaries.keys()):
        s = summaries[code]
        summary_ws.append(
            [
                code,
                s["nav_date"],
                s["latest_nav"],
                s["holding_shares"],
                s["total_cost"],
                s["market_value"],
                s["profit_amount"],
                s["profit_rate"],
                s["oversold"],
            ]
        )

        for lot in s["lots"]:
            positions_ws.append(
                [
                    code,
                    lot["buy_date"],
                    lot["buy_nav"],
                    lot["remaining_shares"],
                    lot["cost"],
                    lot["latest_nav"],
                    lot["profit_amount"],
                    lot["profit_rate"],
                ]
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)


def send_ntfy(ntfy_url: str, title: str, message: str, tags: str = "moneybag") -> None:
    target = normalize_ntfy_url(ntfy_url)
    if not target:
        return
    safe_title = str(title or "").strip() or "Fund Alert"
    safe_tags = str(tags or "").strip() or "moneybag"
    try:
        safe_title.encode("latin-1")
    except UnicodeEncodeError:
        safe_title = "Fund Alert"
    try:
        safe_tags.encode("latin-1")
    except UnicodeEncodeError:
        safe_tags = "moneybag"
    headers = {
        "Title": safe_title,
        "Tags": safe_tags,
        "Priority": "max",
        "Content-Type": "text/plain; charset=utf-8",
    }
    resp = requests.post(target, data=message.encode("utf-8"), headers=headers, timeout=20)
    resp.raise_for_status()


def run_job(config: Dict, xlsx_override: str, out_override: str, threshold_override: Optional[float], ntfy_override: str) -> None:
    app = config["app"]
    xlsx_path = Path(xlsx_override or app["xlsx_path"])
    output_path = Path(out_override or app.get("output_path") or app["xlsx_path"])
    threshold = float(threshold_override if threshold_override is not None else app["threshold"])
    ntfy_url = ntfy_override or os.getenv("NTFY_URL", "").strip() or app.get("ntfy_url", "")

    transactions = load_transactions(xlsx_path)
    if not transactions:
        write_output(xlsx_path, output_path, summaries={})
        print(f"No transactions found. Empty summary saved to: {output_path}")
        return

    by_fund: Dict[str, List[Dict]] = {}
    for txn in transactions:
        by_fund.setdefault(txn["fund_code"], []).append(txn)
    fund_name_map = load_fund_name_map(xlsx_path)

    session = requests.Session()
    summaries: Dict[str, Dict] = {}
    errors: List[str] = []

    for code, fund_txns in sorted(by_fund.items()):
        try:
            nav_date, latest_nav = fetch_latest_nav(code, session)
            summaries[code] = summarize_fund(fund_txns, nav_date, latest_nav)
        except Exception as exc:
            errors.append(f"{code}: {exc}")

    if not summaries:
        raise RuntimeError("All fund NAV queries failed: " + " | ".join(errors))

    write_output(xlsx_path, output_path, summaries)

    if ntfy_url:
        for code, fund_txns in sorted(by_fund.items()):
            s = summaries.get(code)
            if not s:
                continue
            latest_nav = float(s["latest_nav"])
            fund_name = str(fund_name_map.get(code, "") or "").strip() or code
            for txn in sorted(fund_txns, key=lambda x: (x["date"], x["uid"])):
                if str(txn.get("action", "")).upper() != "BUY":
                    continue
                nav = to_float(txn.get("nav"), default=None)
                if nav is None or nav <= 0:
                    continue
                rate = (latest_nav - nav) / nav
                if rate < threshold:
                    continue
                profit = float(txn["shares"]) * (latest_nav - nav)
                lines = [
                    f"基金名称: {fund_name}",
                    f"购买日期: {txn['date']}",
                    f"份额: {txn['shares']:.2f}",
                    f"成交净值: {nav:.4f}",
                    f"最新净值: {latest_nav:.4f}",
                    f"盈利利率: {rate * 100:.2f}%",
                    f"盈利额: {profit:.2f}",
                ]
                send_ntfy(ntfy_url, title="基金收益提醒", message="\n".join(lines), tags="moneybag,rotating_light")

    print(f"Saved: {output_path}")
    for code, s in sorted(summaries.items()):
        print(
            f"{code} nav@{s['nav_date']}={s['latest_nav']:.4f} holding={s['holding_shares']:.2f} "
            f"profit_rate={s['profit_rate'] * 100:.2f}% profit={s['profit_amount']:.2f}"
        )

    if errors:
        print("Errors:")
        for err in errors:
            print("  -", err)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/funds.yaml", help="config file path")
    parser.add_argument("--xlsx", default="", help="override xlsx path")
    parser.add_argument("--out", default="", help="override output xlsx path")
    parser.add_argument("--threshold", type=float, default=None, help="override threshold, e.g. 0.40")
    parser.add_argument("--ntfy", default="", help="override ntfy url")
    args = parser.parse_args()

    config = load_config(Path(args.config))
    run_job(
        config=config,
        xlsx_override=args.xlsx,
        out_override=args.out,
        threshold_override=args.threshold,
        ntfy_override=args.ntfy,
    )


if __name__ == "__main__":
    main()
