import argparse
import copy
import datetime as dt
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import openpyxl
import requests
import tkinter as tk
import tkinter.font as tkfont
from tkinter import filedialog, messagebox, simpledialog, ttk
import yaml


EASTMONEY_LSJZ_URL = "https://api.fund.eastmoney.com/f10/lsjz"
EASTMONEY_FUND_INFO_URL = "https://fund.eastmoney.com/pingzhongdata/{code}.js"
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
    result = copy.deepcopy(base)
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


def sanitize_sheet_title(title: str) -> str:
    invalid = set('\\/*?:[]')
    return "".join(ch for ch in str(title or "").strip() if ch not in invalid).strip()


def normalize_fund_display_name(name: str, code: str = "") -> str:
    s = str(name or "").strip()
    s = s.replace("（LOF）", "").replace("(LOF)", "")
    s = re.sub(r"\([^)]*\)", "", s).strip()
    s = re.sub(r"[A-Za-z]+", "", s).strip()
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return f"基金{code}" if code else "基金"
    return s


def alloc_sheet_title(base: str, used_titles: set) -> str:
    base_clean = sanitize_sheet_title(base) or "基金"
    if len(base_clean) > 31:
        base_clean = base_clean[:31]
    title = base_clean
    idx = 1
    while title in used_titles:
        suffix = f"_{idx}"
        keep = 31 - len(suffix)
        title = f"{base_clean[:keep]}{suffix}"
        idx += 1
    used_titles.add(title)
    return title


def resolve_transaction_sheet_names(wb: openpyxl.Workbook) -> List[str]:
    return [
        name
        for name in wb.sheetnames
        if name not in SYSTEM_SHEETS and name != "transactions" and is_transaction_sheet(wb[name])
    ]


def load_latest_nav_map(xlsx_path: Path) -> Dict[str, float]:
    if not xlsx_path.exists():
        return {}
    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    nav_map: Dict[str, float] = {}
    for name in resolve_transaction_sheet_names(wb):
        ws = wb[name]
        _, code, nav = parse_sheet_meta(ws)
        if code and nav is not None and nav > 0:
            nav_map[code] = float(nav)
    return nav_map


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


def load_or_default(path: Path) -> Dict:
    if not path.exists():
        return copy.deepcopy(DEFAULT_CONFIG)
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return deep_merge(DEFAULT_CONFIG, raw)


def save_config(path: Path, config: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, allow_unicode=True, sort_keys=False)


def normalize_ntfy_url(ntfy_url: str) -> str:
    url = str(ntfy_url or "").strip()
    if not url:
        return ""
    if not re.match(r"^[A-Za-z][A-Za-z0-9+.\-]*://", url):
        url = f"https://{url.lstrip('/')}"
    return url


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

    txns = []
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


def save_transactions(
    xlsx_path: Path,
    transactions: List[Dict],
    latest_nav_map: Optional[Dict[str, float]] = None,
    fund_name_map: Optional[Dict[str, str]] = None,
) -> None:
    ensure_transactions_workbook(xlsx_path)
    wb = openpyxl.load_workbook(xlsx_path)
    merged_nav_map = load_latest_nav_map(xlsx_path)
    merged_name_map = load_fund_name_map(xlsx_path)
    for code, value in (latest_nav_map or {}).items():
        norm_code = normalize_fund_code(code)
        nav = to_float(value, default=None)
        if norm_code and nav is not None and nav > 0:
            merged_nav_map[norm_code] = float(nav)
    for code, name in (fund_name_map or {}).items():
        norm_code = normalize_fund_code(code)
        if norm_code and str(name or "").strip():
            merged_name_map[norm_code] = str(name).strip()

    tx_sheet_names = [
        name
        for name in wb.sheetnames
        if name not in SYSTEM_SHEETS and (name == TEMPLATE_SHEET_NAME or name == "transactions" or is_transaction_sheet(wb[name]))
    ]
    for name in tx_sheet_names:
        wb.remove(wb[name])

    if "Sheet" in wb.sheetnames:
        ws_default = wb["Sheet"]
        if ws_default.max_row <= 1 and ws_default.max_column <= 1 and ws_default.cell(row=1, column=1).value in (None, ""):
            wb.remove(ws_default)

    grouped: Dict[str, List[Dict]] = {}
    for txn in sorted(transactions, key=lambda x: (x["date"], x["uid"]), reverse=True):
        code = normalize_fund_code(txn.get("fund_code"))
        if not code:
            continue
        grouped.setdefault(code, []).append(txn)

    if not grouped:
        ws = wb.create_sheet(TEMPLATE_SHEET_NAME)
        init_template_sheet(ws)
    else:
        used_titles = set(wb.sheetnames)
        for code in sorted(grouped.keys()):
            fund_name = normalize_fund_display_name(merged_name_map.get(code, ""), code=code)
            title = alloc_sheet_title(fund_name or code, used_titles)
            ws = wb.create_sheet(title)
            ws.cell(row=1, column=1).value = META_NAME_LABEL
            ws.cell(row=1, column=2).value = fund_name or code
            ws.cell(row=2, column=1).value = META_CODE_LABEL
            ws.cell(row=2, column=2).value = code
            ws.cell(row=3, column=1).value = META_NAV_LABEL
            latest_nav = merged_nav_map.get(code)
            ws.cell(row=3, column=2).value = "" if latest_nav is None else float(latest_nav)
            for i, h in enumerate(DATA_HEADERS, start=1):
                ws.cell(row=DATA_HEADER_ROW, column=i).value = h
            write_txns = sorted(grouped[code], key=lambda x: (x["date"], x["uid"]), reverse=True)
            for txn in write_txns:
                shares = float(txn["shares"])
                action = str(txn.get("action", "BUY")).upper()
                if action == "SELL":
                    shares = -shares
                nav = to_float(txn["nav"], default=None)
                rate_text = ""
                profit_amount = ""
                if action == "BUY":
                    if latest_nav is not None and nav is not None and nav > 0:
                        rate_text = f"{((latest_nav - nav) / nav) * 100:.2f}%"
                        profit_amount = round(float(txn["shares"]) * (latest_nav - nav), 2)
                ws.append(
                    [
                        txn["date"],
                        shares,
                        "" if nav is None else float(nav),
                        txn.get("note", ""),
                        "" if latest_nav is None else float(latest_nav),
                        rate_text,
                        profit_amount,
                    ]
                )

    wb.save(xlsx_path)


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
    data = resp.json()
    item = data["Data"]["LSJZList"][0]
    return str(item["FSRQ"]), float(item["DWJZ"])


def fetch_fund_name(code: str, session: requests.Session) -> str:
    url = EASTMONEY_FUND_INFO_URL.format(code=code)
    params = {"v": str(int(dt.datetime.now().timestamp()))}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://fundf10.eastmoney.com/",
        "Accept": "*/*",
    }

    resp = session.get(url, params=params, headers=headers, timeout=20)
    resp.raise_for_status()
    text = resp.text
    matched = re.search(r'fS_name\s*=\s*"([^"]+)"', text)
    if not matched:
        raise ValueError(f"Cannot parse fund name for {code}")
    return matched.group(1).strip()


def build_positions(transactions: List[Dict]) -> Tuple[List[Dict], Dict[int, float], float]:
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
            consume = min(lot["remaining_shares"], remain_to_sell)
            lot["remaining_shares"] -= consume
            remain_to_sell -= consume

        if remain_to_sell > 1e-12:
            oversold += remain_to_sell

    remain_map = {lot["source_uid"]: lot["remaining_shares"] for lot in lots}
    clean_lots = [x for x in lots if x["remaining_shares"] > 1e-12]
    return clean_lots, remain_map, oversold


class JijinUI(tk.Tk):
    def __init__(self, config_path: Path):
        super().__init__()
        self.title("\u57fa\u91d1\u8bb0\u5f55\u7ba1\u7406")
        self.geometry("680x500")
        self.minsize(640, 460)
        self._init_theme()

        self.config_path = config_path
        self.config: Dict = load_or_default(config_path)
        self.transactions: List[Dict] = []
        self.sheet_fund_codes: List[str] = []
        self.next_uid = 1
        self.latest_nav_cache: Dict[str, Tuple[str, float]] = {}
        self.fund_name_cache: Dict[str, str] = {}
        self.session = requests.Session()
        self.tabs: Optional[ttk.Notebook] = None
        self.general_tab: Optional[ttk.Frame] = None
        self._last_tab_id = ""
        self._is_switching_tab = False
        self.saved_app_state: Dict = copy.deepcopy(self.config.get("app", {}))

        self.selected_fund_var = tk.StringVar()
        self.sort_column: Optional[str] = None
        self.sort_descending = True
        self.sortable_columns = {"date", "rate", "profit"}
        self.header_labels: Dict[str, str] = {}
        self.fund_display_to_code: Dict[str, str] = {}
        self.rate_cell_meta: Dict[str, Optional[float]] = {}
        self.rate_overlay_labels: Dict[str, tk.Label] = {}
        self._overlay_job: Optional[str] = None
        self.ntfy_sent_cache: set[str] = set()
        self._startup_notify_done = False
        self.vars: Dict[str, tk.Variable] = {
            "app.xlsx_path": tk.StringVar(),
            "app.threshold": tk.StringVar(),
            "app.ntfy_url": tk.StringVar(),
        }

        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self.on_window_close)
        self.apply_config_to_form()
        self.load_transactions_from_doc(silent=True)
        self.after(80, self._run_startup_sync_and_notify_once)

    def _init_theme(self) -> None:
        self.colors = {
            "bg": "#F3F6FB",
            "surface": "#FFFFFF",
            "accent": "#2B5DB9",
            "accent_active": "#214D9D",
            "danger": "#C84B4B",
            "danger_active": "#A93B3B",
            "text": "#20283A",
            "muted": "#5F6C86",
            "tab": "#E6ECF7",
            "tab_active": "#FFFFFF",
            "table_alt": "#F8FAFF",
        }

        self.fonts = {
            "title": tkfont.Font(family="Microsoft YaHei UI", size=13, weight="bold"),
            "body": tkfont.Font(family="Microsoft YaHei UI", size=9),
            "small": tkfont.Font(family="Microsoft YaHei UI", size=8),
            "head": tkfont.Font(family="Microsoft YaHei UI", size=9, weight="bold"),
        }

        self.configure(bg=self.colors["bg"])
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure("App.TFrame", background=self.colors["bg"])
        style.configure("Card.TFrame", background=self.colors["surface"])

        style.configure("Body.TLabel", background=self.colors["bg"], foreground=self.colors["text"], font=self.fonts["body"])
        style.configure("Title.TLabel", background=self.colors["bg"], foreground=self.colors["text"], font=self.fonts["title"])
        style.configure(
            "Subtle.TLabel",
            background=self.colors["bg"],
            foreground=self.colors["muted"],
            font=self.fonts["small"],
        )
        style.configure(
            "Summary.TLabel",
            background=self.colors["surface"],
            foreground=self.colors["text"],
            font=self.fonts["body"],
        )
        style.configure(
            "SummaryHead.TLabel",
            background=self.colors["surface"],
            foreground=self.colors["muted"],
            font=self.fonts["small"],
        )
        style.configure(
            "SummaryValue.TLabel",
            background=self.colors["surface"],
            foreground=self.colors["text"],
            font=self.fonts["small"],
        )
        style.configure(
            "SummaryRateNeutral.TLabel",
            background=self.colors["surface"],
            foreground=self.colors["text"],
            font=self.fonts["small"],
        )
        style.configure(
            "SummaryRatePos.TLabel",
            background="#ECA9A7",
            foreground="#FFFFFF",
            font=self.fonts["small"],
        )
        style.configure(
            "SummaryRateNeg.TLabel",
            background="#97CFAD",
            foreground="#FFFFFF",
            font=self.fonts["small"],
        )

        style.configure("TNotebook", background=self.colors["bg"], borderwidth=0)
        style.configure("TNotebook", tabmargins=(0, 0, 0, 0))
        style.configure(
            "TNotebook.Tab",
            background=self.colors["tab"],
            foreground=self.colors["text"],
            padding=(10, 6),
            font=self.fonts["body"],
            relief="flat",
            borderwidth=0,
            width=9,
        )
        style.map(
            "TNotebook.Tab",
            background=[("selected", self.colors["tab"]), ("!selected", self.colors["tab"])],
            foreground=[("selected", self.colors["text"]), ("!selected", self.colors["text"])],
            relief=[("selected", "flat"), ("!selected", "flat")],
            font=[("selected", self.fonts["body"]), ("!selected", self.fonts["body"])],
            padding=[("selected", (10, 6)), ("!selected", (10, 6))],
            borderwidth=[("selected", 0), ("!selected", 0)],
        )

        style.configure("TEntry", padding=(4, 2), font=self.fonts["body"])
        style.configure("TCombobox", padding=(4, 2), font=self.fonts["body"])

        style.configure("TButton", padding=(6, 3), font=self.fonts["body"])
        style.configure("Toolbar.TButton", padding=(3, 1), font=self.fonts["small"])
        style.configure("Primary.TButton", background=self.colors["accent"], foreground="#FFFFFF")
        style.map("Primary.TButton", background=[("active", self.colors["accent_active"])])
        style.configure("Danger.TButton", background=self.colors["danger"], foreground="#FFFFFF")
        style.map("Danger.TButton", background=[("active", self.colors["danger_active"])])
        style.configure("ToolbarPrimary.TButton", padding=(3, 1), font=self.fonts["small"], background=self.colors["accent"], foreground="#FFFFFF")
        style.map("ToolbarPrimary.TButton", background=[("active", self.colors["accent_active"])])
        style.configure("ToolbarDanger.TButton", padding=(3, 1), font=self.fonts["small"], background=self.colors["danger"], foreground="#FFFFFF")
        style.map("ToolbarDanger.TButton", background=[("active", self.colors["danger_active"])])

        style.configure(
            "Summary.TLabelframe",
            background=self.colors["surface"],
            relief="solid",
            borderwidth=1,
        )
        style.configure(
            "Summary.TLabelframe.Label",
            background=self.colors["surface"],
            foreground=self.colors["text"],
            font=self.fonts["head"],
        )

        style.configure(
            "Data.Treeview",
            background=self.colors["surface"],
            fieldbackground=self.colors["surface"],
            foreground=self.colors["text"],
            rowheight=21,
            font=self.fonts["body"],
        )
        style.configure(
            "Data.Treeview.Heading",
            background="#E9EFFA",
            foreground=self.colors["text"],
            font=self.fonts["head"],
            relief="flat",
            padding=(4, 3),
        )
        style.map("Data.Treeview", background=[("selected", "#DCE7FF")], foreground=[("selected", self.colors["text"])])

    def _build_ui(self) -> None:
        root = ttk.Frame(self, style="App.TFrame", padding=(6, 5, 6, 5))
        root.pack(fill=tk.BOTH, expand=True)
        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)

        self.tabs = ttk.Notebook(root)
        self.tabs.grid(row=0, column=0, sticky="nsew")

        fund_tab = ttk.Frame(self.tabs, style="App.TFrame", padding=6)
        self.general_tab = ttk.Frame(self.tabs, style="App.TFrame", padding=6)
        self.tabs.add(fund_tab, text="\u57fa\u91d1")
        self.tabs.add(self.general_tab, text="\u901a\u7528\u8bbe\u7f6e")

        self._build_fund_tab(fund_tab)
        self._build_general_tab(self.general_tab)
        self._last_tab_id = self.tabs.select()
        self.tabs.bind("<<NotebookTabChanged>>", self.on_tab_changed)

    def _build_fund_tab(self, frame: ttk.Frame) -> None:
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)

        bar = ttk.Frame(frame, style="Card.TFrame", padding=(6, 5))
        bar.grid(row=0, column=0, sticky="w", pady=(0, 5))
        bar.columnconfigure(5, weight=0)

        ttk.Label(bar, text="\u57fa\u91d1", style="Body.TLabel").grid(row=0, column=0, sticky="w")
        self.fund_combo = ttk.Combobox(bar, textvariable=self.selected_fund_var, state="normal", width=20)
        self.fund_combo.grid(row=0, column=1, sticky="w", padx=(3, 2))
        self.fund_combo.bind("<<ComboboxSelected>>", lambda _: self.on_fund_selected())

        ttk.Button(bar, text="\u65b0\u589e\u57fa\u91d1", command=self.add_fund, style="Toolbar.TButton").grid(row=0, column=2, padx=0)
        ttk.Button(bar, text="\u65b0\u589e\u4e70\u5165", command=self.add_buy, style="ToolbarPrimary.TButton").grid(row=0, column=3, padx=0)
        ttk.Button(bar, text="\u5220\u9664\u8bb0\u5f55", command=self.delete_selected_records, style="ToolbarDanger.TButton").grid(row=0, column=4, padx=0)

        table_wrap = ttk.Frame(frame, style="Card.TFrame", padding=(5, 5))
        table_wrap.grid(row=1, column=0, sticky="nsew")
        table_wrap.columnconfigure(0, weight=1)
        table_wrap.rowconfigure(0, weight=1)

        cols = ("date", "shares", "nav", "rate", "profit")
        self.tx_tree = ttk.Treeview(table_wrap, columns=cols, show="headings", height=13, style="Data.Treeview")
        self.header_labels = {
            "date": "\u65e5\u671f",
            "shares": "\u4efd\u989d",
            "nav": "\u6210\u4ea4\u51c0\u503c",
            "rate": "\u76c8\u5229\u5229\u7387",
            "profit": "\u76c8\u5229\u989d",
        }
        widths = {
            "date": 100,
            "shares": 85,
            "nav": 85,
            "rate": 85,
            "profit": 100,
        }
        for c in cols:
            self.tx_tree.column(c, width=widths[c], anchor="center", stretch=True, minwidth=40)
        self._refresh_tree_headings()

        self.tx_tree.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(table_wrap, orient=tk.VERTICAL, command=self.tx_tree.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        self.tx_tree.config(yscrollcommand=scroll.set)
        self.tx_tree.tag_configure("row_even", background=self.colors["surface"])
        self.tx_tree.tag_configure("row_odd", background=self.colors["table_alt"])
        self.tx_tree.bind("<Button-3>", self.on_tree_right_click)
        self.tx_tree.bind("<Configure>", lambda _e: self._queue_rate_overlay_refresh())
        self.tx_tree.bind("<MouseWheel>", lambda _e: self._queue_rate_overlay_refresh())
        self.tx_tree.bind("<ButtonRelease-1>", lambda _e: self._queue_rate_overlay_refresh())
        scroll.bind("<B1-Motion>", lambda _e: self._queue_rate_overlay_refresh())
        scroll.bind("<ButtonRelease-1>", lambda _e: self._queue_rate_overlay_refresh())

        self.tx_menu = tk.Menu(self, tearoff=0)
        self.tx_menu.add_command(label="\u4fee\u6539", command=self.edit_selected_record)

        summary_box = ttk.LabelFrame(frame, text="\u5f53\u524d\u57fa\u91d1\u6c47\u603b", padding=2, style="Summary.TLabelframe")
        summary_box.grid(row=2, column=0, sticky="ew", pady=(4, 0))
        for i in range(6):
            summary_box.columnconfigure(i, weight=1, uniform="summary")

        headers = ["\u57fa\u91d1\u540d\u79f0", "\u6700\u65b0\u51c0\u503c", "\u6301\u4ed3\u4efd\u989d", "\u6210\u672c", "\u76c8\u5229\u989d", "\u76c8\u5229\u5229\u7387"]
        col_width_chars = [10, 10, 10, 10, 10, 10]
        for i, h in enumerate(headers):
            ttk.Label(summary_box, text=h, style="SummaryHead.TLabel", anchor="center", width=col_width_chars[i]).grid(row=0, column=i, sticky="ew", padx=0, pady=(0, 0))

        self.summary_fund_var = tk.StringVar(value="-")
        self.summary_latest_var = tk.StringVar(value="-")
        self.summary_shares_var = tk.StringVar(value="-")
        self.summary_cost_var = tk.StringVar(value="-")
        self.summary_profit_var = tk.StringVar(value="-")
        self.summary_rate_var = tk.StringVar(value="-")

        ttk.Label(summary_box, textvariable=self.summary_fund_var, style="SummaryValue.TLabel", anchor="center", width=col_width_chars[0]).grid(row=1, column=0, sticky="ew", padx=0, pady=(0, 0))
        ttk.Label(summary_box, textvariable=self.summary_latest_var, style="SummaryValue.TLabel", anchor="center", width=col_width_chars[1]).grid(row=1, column=1, sticky="ew", padx=0, pady=(0, 0))
        ttk.Label(summary_box, textvariable=self.summary_shares_var, style="SummaryValue.TLabel", anchor="center", width=col_width_chars[2]).grid(row=1, column=2, sticky="ew", padx=0, pady=(0, 0))
        ttk.Label(summary_box, textvariable=self.summary_cost_var, style="SummaryValue.TLabel", anchor="center", width=col_width_chars[3]).grid(row=1, column=3, sticky="ew", padx=0, pady=(0, 0))
        ttk.Label(summary_box, textvariable=self.summary_profit_var, style="SummaryValue.TLabel", anchor="center", width=col_width_chars[4]).grid(row=1, column=4, sticky="ew", padx=0, pady=(0, 0))
        self.summary_rate_label = ttk.Label(summary_box, textvariable=self.summary_rate_var, style="SummaryRateNeutral.TLabel", anchor="center")
        self.summary_rate_label.configure(width=col_width_chars[5])
        self.summary_rate_label.grid(row=1, column=5, sticky="ew", padx=0, pady=(0, 0))

    def _build_general_tab(self, frame: ttk.Frame) -> None:
        frame.columnconfigure(1, weight=1)
        frame.rowconfigure(3, weight=0)

        ttk.Label(frame, text="\u4ea4\u6613\u6587\u6863", style="Body.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Entry(frame, textvariable=self.vars["app.xlsx_path"]).grid(row=0, column=1, sticky="ew", padx=6)
        ttk.Button(frame, text="\u9009\u62e9", command=self.choose_xlsx_path).grid(row=0, column=2, padx=2)
        ttk.Button(frame, text="\u52a0\u8f7d\u6587\u6863", command=self.load_transactions_from_doc, style="Primary.TButton").grid(row=0, column=3, padx=2)

        ttk.Label(frame, text="\u6536\u76ca\u9608\u503c", style="Body.TLabel").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(frame, textvariable=self.vars["app.threshold"], width=20).grid(row=1, column=1, sticky="w", padx=6, pady=(8, 0))

        ttk.Label(frame, text="ntfy_url", style="Body.TLabel").grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(frame, textvariable=self.vars["app.ntfy_url"]).grid(row=2, column=1, columnspan=3, sticky="ew", padx=6, pady=(8, 0))

        ttk.Button(frame, text="\u4fdd\u5b58\u901a\u7528\u8bbe\u7f6e", command=self.save_settings_only, style="Primary.TButton").grid(
            row=3, column=0, columnspan=4, pady=(12, 0), sticky="e"
        )

    def apply_config_to_form(self) -> None:
        app = self.config.get("app", {})
        self.vars["app.xlsx_path"].set(str(app.get("xlsx_path", "data/jijin.xlsx")))
        self.vars["app.threshold"].set(str(app.get("threshold", 0.40)))
        self.vars["app.ntfy_url"].set(str(app.get("ntfy_url", "")))

    def collect_form_to_config(self) -> None:
        threshold_text = self.vars["app.threshold"].get().strip() or "0.40"
        try:
            threshold = float(threshold_text)
        except ValueError as exc:
            raise ValueError("threshold 必须是数字") from exc

        self.config.setdefault("app", {})
        self.config["app"]["xlsx_path"] = self.vars["app.xlsx_path"].get().strip() or "data/jijin.xlsx"
        self.config["app"]["threshold"] = threshold
        self.config["app"]["ntfy_url"] = self.vars["app.ntfy_url"].get().strip()
        self.config["app"].pop("notify_all", None)

        funds = sorted(set(self.get_all_fund_codes()))
        self.config["funds"] = funds

    def choose_xlsx_path(self) -> None:
        path = filedialog.asksaveasfilename(
            title="\u9009\u62e9\u4ea4\u6613\u8bb0\u5f55\u6587\u4ef6",
            defaultextension=".xlsx",
            initialfile=Path(self.vars["app.xlsx_path"].get() or "jijin.xlsx").name,
            filetypes=[("Excel \u6587\u4ef6", "*.xlsx"), ("\u6240\u6709\u6587\u4ef6", "*.*")],
        )
        if path:
            self.vars["app.xlsx_path"].set(path)

    def load_transactions_from_doc(self, silent: bool = False) -> None:
        try:
            self.collect_form_to_config()
            xlsx_path = Path(self.config["app"]["xlsx_path"])
            self.transactions = load_transactions(xlsx_path)
            self.next_uid = max([x["uid"] for x in self.transactions], default=0) + 1
            stored_nav_map = load_latest_nav_map(xlsx_path)
            self.latest_nav_cache = {code: ("workbook", nav) for code, nav in stored_nav_map.items()}
            sheet_name_map = load_fund_name_map(xlsx_path)
            self.fund_name_cache.update(sheet_name_map)
            self.sheet_fund_codes = sorted(set(sheet_name_map.keys()) | {txn["fund_code"] for txn in self.transactions})
            self.refresh_fund_list(keep_current=False)
            self.refresh_fund_view()
        except Exception as exc:
            messagebox.showerror("\u52a0\u8f7d\u5931\u8d25", str(exc))

    def save_settings_only(self) -> None:
        self._save_settings(show_success=True)

    def save_all(self) -> None:
        try:
            self.collect_form_to_config()
            xlsx_path = Path(self.config["app"]["xlsx_path"])
            latest_nav_map = {code: val[1] for code, val in self.latest_nav_cache.items() if val and len(val) >= 2}
            fund_name_map = {normalize_fund_code(code): name for code, name in self.fund_name_cache.items() if normalize_fund_code(code)}
            save_transactions(xlsx_path, self.transactions, latest_nav_map=latest_nav_map, fund_name_map=fund_name_map)
            self.sheet_fund_codes = sorted({normalize_fund_code(txn["fund_code"]) for txn in self.transactions if normalize_fund_code(txn["fund_code"])})
            save_config(self.config_path, self.config)
            self.saved_app_state = copy.deepcopy(self.config.get("app", {}))
            messagebox.showinfo("\u63d0\u793a", f"\u5df2\u4fdd\u5b58\uff1a\n{self.config_path}\n{xlsx_path}")
        except Exception as exc:
            messagebox.showerror("\u4fdd\u5b58\u5931\u8d25", str(exc))

    def _save_settings(self, show_success: bool = False) -> bool:
        try:
            self.collect_form_to_config()
            save_config(self.config_path, self.config)
            self.saved_app_state = copy.deepcopy(self.config.get("app", {}))
            if show_success:
                messagebox.showinfo("\u63d0\u793a", f"\u8bbe\u7f6e\u5df2\u4fdd\u5b58\u5230\uff1a\n{self.config_path}")
            return True
        except Exception as exc:
            messagebox.showerror("\u4fdd\u5b58\u5931\u8d25", str(exc))
            return False

    def _reload_form_from_saved_state(self) -> None:
        app = self.saved_app_state or {}
        self.vars["app.xlsx_path"].set(str(app.get("xlsx_path", "data/jijin.xlsx")))
        self.vars["app.threshold"].set(str(app.get("threshold", 0.40)))
        self.vars["app.ntfy_url"].set(str(app.get("ntfy_url", "")))

    def _has_unsaved_settings(self) -> bool:
        app = self.saved_app_state or {}

        current_xlsx = self.vars["app.xlsx_path"].get().strip() or "data/jijin.xlsx"
        saved_xlsx = str(app.get("xlsx_path", "data/jijin.xlsx")).strip() or "data/jijin.xlsx"
        if current_xlsx != saved_xlsx:
            return True

        current_ntfy = self.vars["app.ntfy_url"].get().strip()
        saved_ntfy = str(app.get("ntfy_url", "")).strip()
        if current_ntfy != saved_ntfy:
            return True

        current_threshold_raw = self.vars["app.threshold"].get().strip() or "0.40"
        current_threshold = to_float(current_threshold_raw, default=None)
        saved_threshold = to_float(app.get("threshold", 0.40), default=0.40)
        if current_threshold is None:
            return True
        if saved_threshold is None:
            saved_threshold = 0.40
        return abs(current_threshold - saved_threshold) > 1e-12

    def _ask_save_settings_action(self, for_close: bool = False) -> str:
        if not self._has_unsaved_settings():
            return "none"

        prompt = "\u901a\u7528\u8bbe\u7f6e\u5df2\u4fee\u6539\uff0c\u662f\u5426\u4fdd\u5b58\u540e\u518d\u9000\u51fa\uff1f" if for_close else "\u901a\u7528\u8bbe\u7f6e\u5df2\u4fee\u6539\uff0c\u662f\u5426\u5148\u4fdd\u5b58\uff1f"
        answer = messagebox.askyesnocancel("\u672a\u4fdd\u5b58\u7684\u8bbe\u7f6e", prompt)
        if answer is None:
            return "cancel"
        if answer:
            return "save" if self._save_settings(show_success=False) else "cancel"
        return "discard"

    def on_tab_changed(self, _event=None) -> None:
        if self._is_switching_tab or self.tabs is None or self.general_tab is None:
            return

        current_tab = self.tabs.select()
        previous_tab = self._last_tab_id
        self._last_tab_id = current_tab

        if not previous_tab:
            return

        leaving_general = previous_tab == str(self.general_tab) and current_tab != previous_tab
        if not leaving_general:
            return

        action = self._ask_save_settings_action(for_close=False)
        if action in ("none", "save"):
            return
        if action == "discard":
            self._reload_form_from_saved_state()
            return

        self._is_switching_tab = True
        try:
            self.tabs.select(previous_tab)
            self._last_tab_id = previous_tab
        finally:
            self._is_switching_tab = False

    def on_window_close(self) -> None:
        action = self._ask_save_settings_action(for_close=True)
        if action == "cancel":
            return
        if self._overlay_job:
            try:
                self.after_cancel(self._overlay_job)
            except Exception:
                pass
            self._overlay_job = None
        self.destroy()

    def get_all_fund_codes(self) -> List[str]:
        return list(self.sheet_fund_codes)

    def _format_fund_display(self, code: str) -> str:
        code = normalize_fund_code(code)
        if not code:
            return ""
        name = str(self.fund_name_cache.get(code, "") or "").strip()
        return f"{name}({code})" if name else code

    def _resolve_selected_fund_code(self) -> str:
        raw = self.selected_fund_var.get().strip()
        code = normalize_fund_code(raw)
        if code:
            return code
        mapped = self.fund_display_to_code.get(raw, "")
        if mapped:
            return mapped
        matched = re.search(r"(\d{6})\s*\)?\s*$", raw)
        if matched:
            return normalize_fund_code(matched.group(1))
        return ""

    def _set_selected_fund_code(self, code: str) -> None:
        code = normalize_fund_code(code)
        self.selected_fund_var.set(self._format_fund_display(code) if code else "")

    def _set_rate_bg_by_value(self, rate: Optional[float]) -> None:
        if not hasattr(self, "summary_rate_label"):
            return
        if rate is None:
            self.summary_rate_label.configure(style="SummaryRateNeutral.TLabel")
        elif rate > 0:
            self.summary_rate_label.configure(style="SummaryRatePos.TLabel")
        else:
            self.summary_rate_label.configure(style="SummaryRateNeg.TLabel")

    @staticmethod
    def _compact_name(name: str, max_len: int = 10) -> str:
        s = str(name or "").strip()
        if len(s) <= max_len:
            return s
        return f"{s[:max_len]}..."

    def refresh_fund_list(self, keep_current: bool = True) -> None:
        prev_code = self._resolve_selected_fund_code()
        funds = self.get_all_fund_codes()
        values = [self._format_fund_display(code) for code in funds]
        self.fund_display_to_code = {display: code for display, code in zip(values, funds)}
        self.fund_combo["values"] = values

        if keep_current and prev_code in funds:
            self._set_selected_fund_code(prev_code)
        elif funds:
            self._set_selected_fund_code(funds[0])
        else:
            self.selected_fund_var.set("")

    def fund_transactions(self, code: str) -> List[Dict]:
        return [x for x in self.transactions if x["fund_code"] == code]

    def get_latest_nav_cached(self, code: str) -> Optional[Tuple[str, float]]:
        return self.latest_nav_cache.get(code)

    def build_display_rows(self, code: str, latest_nav: Optional[float]) -> Tuple[List[Dict], Dict[str, float]]:
        txns = sorted(self.fund_transactions(code), key=lambda x: (x["date"], x["uid"]))
        lots, _, oversold = build_positions(txns)

        total_shares = sum(x["remaining_shares"] for x in lots)
        total_cost = sum(x["remaining_shares"] * x["buy_nav"] for x in lots)
        total_profit = 0.0
        total_rate = 0.0

        rows = []
        for txn in txns:
            rate_str = "-"
            profit_str = "-"
            rate_value = None
            profit_value = None

            if txn["action"] == "BUY":
                nav = float(txn["nav"] or 0.0)
                if latest_nav is not None and nav > 0:
                    rate = (latest_nav - nav) / nav
                    profit = float(txn["shares"]) * (latest_nav - nav)
                    rate_value = rate
                    profit_value = profit
                    rate_str = f"{rate * 100:.2f}%"
                    profit_str = f"{profit:.2f}"

            rows.append(
                {
                    "uid": str(txn["uid"]),
                    "date": txn["date"],
                    "shares": f"{txn['shares']:.2f}",
                    "nav": "" if txn["nav"] is None else f"{txn['nav']:.4f}",
                    "rate": rate_str,
                    "profit": profit_str,
                    "note": txn.get("note", ""),
                    "date_sort": txn["date"],
                    "rate_sort": rate_value,
                    "profit_sort": profit_value,
                }
            )

        if latest_nav is not None:
            total_profit = total_shares * latest_nav - total_cost
            total_rate = (total_profit / total_cost) if total_cost > 0 else 0.0

        summary = {
            "shares": total_shares,
            "cost": total_cost,
            "profit": total_profit,
            "rate": total_rate,
            "oversold": oversold,
        }
        return rows, summary

    @staticmethod
    def _sort_optional_numeric_rows(rows: List[Dict], key_name: str, descending: bool) -> List[Dict]:
        valid_rows = [x for x in rows if x.get(key_name) is not None]
        empty_rows = [x for x in rows if x.get(key_name) is None]
        valid_rows.sort(key=lambda x: x[key_name], reverse=descending)
        return valid_rows + empty_rows

    def _header_text(self, column: str) -> str:
        base = self.header_labels.get(column, column)
        if column not in self.sortable_columns:
            return base
        if self.sort_column == column:
            icon = "\u25bc" if self.sort_descending else "\u25b2"
            return f"{base} {icon}"
        return f"{base} \u25b3\u25bd"

    def _refresh_tree_headings(self) -> None:
        for column in self.tx_tree["columns"]:
            if column in self.sortable_columns:
                self.tx_tree.heading(
                    column,
                    text=self._header_text(column),
                    command=lambda c=column: self.on_sort_header_click(c),
                )
            else:
                self.tx_tree.heading(column, text=self._header_text(column))

    def on_sort_header_click(self, column: str) -> None:
        if column not in self.sortable_columns:
            return
        if self.sort_column == column:
            self.sort_descending = not self.sort_descending
        else:
            self.sort_column = column
            self.sort_descending = True
        self.refresh_fund_view()

    def apply_row_sort(self, rows: List[Dict]) -> List[Dict]:
        result = list(rows)
        if self.sort_column == "date":
            result.sort(key=lambda x: (x["date_sort"], int(x["uid"])), reverse=self.sort_descending)
        elif self.sort_column == "rate":
            result = self._sort_optional_numeric_rows(result, "rate_sort", descending=self.sort_descending)
        elif self.sort_column == "profit":
            result = self._sort_optional_numeric_rows(result, "profit_sort", descending=self.sort_descending)
        return result

    def _clear_rate_overlays(self) -> None:
        for label in self.rate_overlay_labels.values():
            try:
                label.destroy()
            except Exception:
                pass
        self.rate_overlay_labels = {}

    def _on_rate_overlay_click(self, iid: str) -> None:
        try:
            self.tx_tree.selection_set(iid)
            self.tx_tree.focus(iid)
        except Exception:
            pass

    def _refresh_rate_overlays(self) -> None:
        self._overlay_job = None
        if not hasattr(self, "tx_tree") or not self.tx_tree.winfo_exists():
            return
        columns = list(self.tx_tree["columns"])
        rate_col_token = f"#{columns.index('rate') + 1}" if "rate" in columns else "#4"
        tree_w = self.tx_tree.winfo_width()
        tree_h = self.tx_tree.winfo_height()
        visible_iids = set(str(iid) for iid in self.tx_tree.get_children())
        stale = [iid for iid in list(self.rate_overlay_labels.keys()) if iid not in visible_iids]
        for iid in stale:
            try:
                self.rate_overlay_labels[iid].destroy()
            except Exception:
                pass
            self.rate_overlay_labels.pop(iid, None)

        for iid in self.tx_tree.get_children():
            iid = str(iid)
            rate_value = self.rate_cell_meta.get(str(iid))
            if rate_value is None:
                if iid in self.rate_overlay_labels:
                    try:
                        self.rate_overlay_labels[iid].destroy()
                    except Exception:
                        pass
                    self.rate_overlay_labels.pop(iid, None)
                continue
            bbox = self.tx_tree.bbox(iid, "rate")
            if not bbox:
                if iid in self.rate_overlay_labels:
                    self.rate_overlay_labels[iid].place_forget()
                continue
            x, y, w, h = bbox
            if w <= 2 or h <= 2:
                if iid in self.rate_overlay_labels:
                    self.rate_overlay_labels[iid].place_forget()
                continue
            # Only overlay fully visible rate cells, avoiding stray labels in blank area.
            if x < 0 or y < 0 or x + w > tree_w or y + h > tree_h:
                if iid in self.rate_overlay_labels:
                    self.rate_overlay_labels[iid].place_forget()
                continue
            center_x = x + max(1, w // 2)
            center_y = y + max(1, h // 2)
            if self.tx_tree.identify_row(center_y) != iid or self.tx_tree.identify_column(center_x) != rate_col_token:
                if iid in self.rate_overlay_labels:
                    self.rate_overlay_labels[iid].place_forget()
                continue
            rate_text = self.tx_tree.set(iid, "rate")
            bg = "#ECA9A7" if rate_value > 0 else "#97CFAD"
            label = self.rate_overlay_labels.get(iid)
            if label is None:
                label = tk.Label(
                    self.tx_tree,
                    text=rate_text,
                    bg=bg,
                    fg="#FFFFFF",
                    bd=0,
                    padx=2,
                    pady=0,
                    anchor="center",
                    font=self.fonts["small"],
                )
                label.bind("<Button-1>", lambda _e, rid=iid: self._on_rate_overlay_click(rid))
                self.rate_overlay_labels[iid] = label
            else:
                label.configure(text=rate_text, bg=bg, fg="#FFFFFF")
            label.place(x=x + 1, y=y + 1, width=w - 2, height=h - 2)

    def _queue_rate_overlay_refresh(self, delay_ms: int = 16) -> None:
        if not self.winfo_exists():
            return
        if self._overlay_job:
            try:
                self.after_cancel(self._overlay_job)
            except Exception:
                pass
        self._overlay_job = self.after(delay_ms, self._refresh_rate_overlays)

    def refresh_fund_view(self) -> None:
        code = self._resolve_selected_fund_code()
        self._set_selected_fund_code(code)
        self._refresh_tree_headings()

        self.rate_cell_meta = {}
        self._clear_rate_overlays()
        for item in self.tx_tree.get_children():
            self.tx_tree.delete(item)

        if not code:
            self.summary_fund_var.set("-")
            self.summary_latest_var.set("-")
            self.summary_shares_var.set("-")
            self.summary_cost_var.set("-")
            self.summary_profit_var.set("-")
            self.summary_rate_var.set("-")
            self._set_rate_bg_by_value(None)
            self._queue_rate_overlay_refresh()
            return

        if code not in self.fund_name_cache:
            try:
                self.fund_name_cache[code] = fetch_fund_name(code, self.session)
            except Exception:
                self.fund_name_cache[code] = ""
        fund_name = self.fund_name_cache.get(code, "")
        self.summary_fund_var.set(self._compact_name(fund_name or code, max_len=7))

        latest = self.get_latest_nav_cached(code)
        latest_nav = latest[1] if latest else None

        rows, summary = self.build_display_rows(code, latest_nav)
        sorted_rows = self.apply_row_sort(rows)
        for idx, row in enumerate(sorted_rows):
            tags = ("row_even",) if idx % 2 == 0 else ("row_odd",)
            self.tx_tree.insert(
                "",
                tk.END,
                iid=row["uid"],
                tags=tags,
                values=(row["date"], row["shares"], row["nav"], row["rate"], row["profit"]),
            )
            self.rate_cell_meta[str(row["uid"])] = row.get("rate_sort")

        if latest:
            self.summary_latest_var.set(f"{latest[1]:.4f}")
        else:
            self.summary_latest_var.set("-")

        self.summary_shares_var.set(f"{summary['shares']:.2f}")
        self.summary_cost_var.set(f"{summary['cost']:.2f}")
        self.summary_profit_var.set(f"{summary['profit']:.2f}")
        self.summary_rate_var.set(f"{summary['rate'] * 100:.2f}%")
        self._set_rate_bg_by_value(summary["rate"])
        self._queue_rate_overlay_refresh()
        self.after_idle(self._queue_rate_overlay_refresh)

    def refresh_fund_profit(self) -> None:
        self.on_fund_selected()

    def on_fund_selected(self) -> None:
        code = self._resolve_selected_fund_code()
        if not code:
            self.refresh_fund_view()
            return

        # Show cached data immediately, then refresh NAV.
        self.refresh_fund_view()
        try:
            nav_date, latest_nav = fetch_latest_nav(code, self.session)
            self.latest_nav_cache[code] = (nav_date, latest_nav)
            self.refresh_fund_view()
            self._save_trade_doc()
        except Exception:
            messagebox.showerror("\u5237\u65b0\u5931\u8d25", "\u83b7\u53d6\u4e0d\u5230\u6700\u65b0\u57fa\u91d1\u51c0\u503c")

    def _run_startup_sync_and_notify_once(self) -> None:
        if self._startup_notify_done:
            return
        self._startup_notify_done = True

        fund_codes = list(self.get_all_fund_codes())
        if not fund_codes:
            return

        url_text = self.vars["app.ntfy_url"].get().strip() or str(self.config.get("app", {}).get("ntfy_url", "")).strip()
        ntfy_url = normalize_ntfy_url(url_text)

        threshold = to_float(self.vars["app.threshold"].get().strip(), default=None)
        if threshold is None:
            threshold = to_float(self.config.get("app", {}).get("threshold", 0.40), default=0.40)
        threshold = float(threshold if threshold is not None else 0.40)

        notify_rows: List[Tuple[str, Dict, float]] = []
        nav_errors: List[str] = []
        for code in fund_codes:
            try:
                nav_date, latest_nav = fetch_latest_nav(code, self.session)
                self.latest_nav_cache[code] = (nav_date, latest_nav)
            except Exception as exc:
                nav_errors.append(f"{code}: {exc}")
                continue

            rows, _ = self.build_display_rows(code, latest_nav)
            for row in rows:
                rate_val = row.get("rate_sort")
                if rate_val is None or float(rate_val) < threshold:
                    continue
                notify_rows.append((code, row, latest_nav))

        self.refresh_fund_view()
        self._save_trade_doc()

        if not ntfy_url:
            if nav_errors:
                messagebox.showwarning("\u51c0\u503c\u66f4\u65b0\u63d0\u793a", "\u90e8\u5206\u57fa\u91d1\u51c0\u503c\u66f4\u65b0\u5931\u8d25\uff1a\n" + "\n".join(nav_errors[:3]))
            return

        errors = []
        for code, row, latest_nav in notify_rows:
            nav_date = str(self.latest_nav_cache.get(code, ("", latest_nav))[0])
            alert_key = f"startup|{code}|{row['uid']}|{nav_date}|{latest_nav:.6f}|{threshold:.6f}"
            if alert_key in self.ntfy_sent_cache:
                continue
            fund_name = str(self.fund_name_cache.get(code, "") or "").strip() or code
            lines = [
                f"基金名称: {fund_name}",
                f"购买日期: {row['date']}",
                f"份额: {row['shares']}",
                f"成交净值: {row['nav']}",
                f"最新净值: {latest_nav:.4f}",
                f"盈利利率: {row['rate']}",
                f"盈利额: {row['profit']}",
            ]
            try:
                send_ntfy(ntfy_url, title="基金收益提醒", message="\n".join(lines), tags="moneybag,rotating_light")
                self.ntfy_sent_cache.add(alert_key)
            except Exception as exc:
                errors.append(str(exc))

        if errors:
            messagebox.showerror("\u901a\u77e5\u5931\u8d25", f"ntfy \u53d1\u9001\u5931\u8d25\uff1a\n{errors[0]}")
        elif nav_errors:
            messagebox.showwarning("\u51c0\u503c\u66f4\u65b0\u63d0\u793a", "\u90e8\u5206\u57fa\u91d1\u51c0\u503c\u66f4\u65b0\u5931\u8d25\uff1a\n" + "\n".join(nav_errors[:3]))

    def add_fund(self) -> None:
        raw = simpledialog.askstring("\u65b0\u589e\u57fa\u91d1", "\u8f93\u51656\u4f4d\u57fa\u91d1\u4ee3\u7801:")
        if raw is None:
            return
        code = normalize_fund_code(raw)
        if not code:
            messagebox.showwarning("\u63d0\u793a", "\u57fa\u91d1\u4ee3\u7801\u65e0\u6548")
            return

        funds = set(self.config.get("funds", []))
        funds.add(code)
        self.config["funds"] = sorted(funds)
        if code not in self.fund_name_cache:
            try:
                self.fund_name_cache[code] = fetch_fund_name(code, self.session)
            except Exception:
                self.fund_name_cache[code] = ""

        self._persist_fund_changes()
        self.refresh_fund_list(keep_current=False)
        self._set_selected_fund_code(code)
        self.refresh_fund_view()

    @staticmethod
    def _validated_date(raw: str) -> Optional[str]:
        date_str = parse_date(raw)
        if not date_str:
            return None
        try:
            dt.date.fromisoformat(date_str)
            return date_str
        except ValueError:
            return None

    def _save_trade_doc(self) -> bool:
        try:
            current = self.vars["app.xlsx_path"].get().strip() or self.config.get("app", {}).get("xlsx_path", "data/jijin.xlsx")
            self.config.setdefault("app", {})
            self.config["app"]["xlsx_path"] = current
            latest_nav_map = {code: val[1] for code, val in self.latest_nav_cache.items() if val and len(val) >= 2}
            fund_name_map = {normalize_fund_code(code): name for code, name in self.fund_name_cache.items() if normalize_fund_code(code)}
            save_transactions(Path(current), self.transactions, latest_nav_map=latest_nav_map, fund_name_map=fund_name_map)
            self.sheet_fund_codes = sorted({normalize_fund_code(txn["fund_code"]) for txn in self.transactions if normalize_fund_code(txn["fund_code"])})
            return True
        except Exception as exc:
            messagebox.showerror("\u4fdd\u5b58\u5931\u8d25", f"\u4ea4\u6613\u8bb0\u5f55\u6587\u4ef6\u4fdd\u5b58\u5931\u8d25\uff1a\n{exc}")
            return False

    def _save_runtime_config(self) -> bool:
        try:
            self.config.setdefault("app", {})
            current = self.vars["app.xlsx_path"].get().strip() or self.config.get("app", {}).get("xlsx_path", "data/jijin.xlsx")
            self.config["app"]["xlsx_path"] = current
            self.config["funds"] = sorted(set(self.get_all_fund_codes()))
            save_config(self.config_path, self.config)
            self.saved_app_state = copy.deepcopy(self.config.get("app", {}))
            return True
        except Exception as exc:
            messagebox.showerror("\u4fdd\u5b58\u5931\u8d25", f"\u914d\u7f6e\u4fdd\u5b58\u5931\u8d25\uff1a\n{exc}")
            return False

    def _persist_fund_changes(self) -> bool:
        ok_xlsx = self._save_trade_doc()
        ok_cfg = self._save_runtime_config()
        return ok_xlsx and ok_cfg

    def _center_dialog(self, dialog: tk.Toplevel) -> None:
        self.update_idletasks()
        dialog.update_idletasks()

        parent_x = self.winfo_rootx()
        parent_y = self.winfo_rooty()
        parent_w = self.winfo_width()
        parent_h = self.winfo_height()
        dialog_w = dialog.winfo_reqwidth()
        dialog_h = dialog.winfo_reqheight()

        pos_x = parent_x + max((parent_w - dialog_w) // 2, 0)
        pos_y = parent_y + max((parent_h - dialog_h) // 2, 0)
        dialog.geometry(f"+{pos_x}+{pos_y}")

    def _prompt_buy_dialog(
        self,
        title: str = "\u65b0\u589e\u4e70\u5165",
        initial_date: Optional[str] = None,
        initial_shares: Optional[float] = None,
        initial_nav: Optional[float] = None,
    ) -> Optional[Dict]:
        dialog = tk.Toplevel(self)
        dialog.title(title)
        dialog.transient(self)
        dialog.grab_set()
        dialog.resizable(False, False)

        date_var = tk.StringVar(value=initial_date or dt.date.today().isoformat())
        shares_var = tk.StringVar(value="" if initial_shares is None else f"{initial_shares:.2f}")
        nav_var = tk.StringVar(value="" if initial_nav is None else f"{initial_nav:.4f}")

        body = ttk.Frame(dialog, padding=12)
        body.grid(row=0, column=0, sticky="nsew")

        ttk.Label(body, text="\u4e70\u5165\u65e5\u671f (YYYY-MM-DD)").grid(row=0, column=0, sticky="w")
        ttk.Entry(body, textvariable=date_var, width=24).grid(row=0, column=1, sticky="ew", padx=(8, 0))
        ttk.Label(body, text="\u4efd\u989d").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(body, textvariable=shares_var, width=24).grid(row=1, column=1, sticky="ew", padx=(8, 0), pady=(8, 0))
        ttk.Label(body, text="\u6210\u4ea4\u51c0\u503c").grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(body, textvariable=nav_var, width=24).grid(row=2, column=1, sticky="ew", padx=(8, 0), pady=(8, 0))

        result: Dict[str, float] = {}

        def on_ok() -> None:
            date_str = self._validated_date(date_var.get().strip())
            if not date_str:
                messagebox.showwarning("\u63d0\u793a", "\u65e5\u671f\u683c\u5f0f\u65e0\u6548")
                return

            shares = to_float(shares_var.get(), default=None)
            if shares is None or shares <= 0:
                messagebox.showwarning("\u63d0\u793a", "\u4efd\u989d\u5fc5\u987b\u5927\u4e8e 0")
                return

            nav = to_float(nav_var.get(), default=None)
            if nav is None or nav <= 0:
                messagebox.showwarning("\u63d0\u793a", "\u6210\u4ea4\u51c0\u503c\u5fc5\u987b\u5927\u4e8e 0")
                return

            result["date"] = date_str
            result["shares"] = float(shares)
            result["nav"] = float(nav)
            dialog.destroy()

        def on_cancel() -> None:
            dialog.destroy()

        btns = ttk.Frame(body)
        btns.grid(row=3, column=0, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(btns, text="\u786e\u5b9a", command=on_ok).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(btns, text="\u53d6\u6d88", command=on_cancel).grid(row=0, column=1)

        dialog.bind("<Return>", lambda _: on_ok())
        dialog.bind("<Escape>", lambda _: on_cancel())
        self._center_dialog(dialog)
        dialog.wait_window()
        return result or None

    def _find_txn_by_uid(self, uid: int) -> Optional[Dict]:
        for txn in self.transactions:
            if int(txn["uid"]) == uid:
                return txn
        return None

    def on_tree_right_click(self, event) -> None:
        row_id = self.tx_tree.identify_row(event.y)
        if not row_id:
            return
        if row_id not in self.tx_tree.selection():
            self.tx_tree.selection_set(row_id)
        self.tx_tree.focus(row_id)
        try:
            self.tx_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.tx_menu.grab_release()

    def add_buy(self) -> None:
        code = self._resolve_selected_fund_code()
        if not code:
            messagebox.showwarning("\u63d0\u793a", "\u8bf7\u5148\u9009\u62e9\u57fa\u91d1")
            return

        form = self._prompt_buy_dialog(title="\u65b0\u589e\u4e70\u5165")
        if form is None:
            return

        txn = {
            "uid": self.next_uid,
            "date": form["date"],
            "fund_code": code,
            "action": "BUY",
            "shares": form["shares"],
            "nav": form["nav"],
            "note": "",
        }
        self.next_uid += 1
        self.transactions.append(txn)
        self.transactions.sort(key=lambda x: (x["date"], x["uid"]))

        funds = set(self.config.get("funds", []))
        funds.add(txn["fund_code"])
        self.config["funds"] = sorted(funds)

        self._persist_fund_changes()
        self.refresh_fund_list(keep_current=True)
        self.refresh_fund_view()

    def edit_selected_record(self) -> None:
        selected = self.tx_tree.selection()
        if len(selected) != 1:
            messagebox.showwarning("\u63d0\u793a", "\u8bf7\u53ea\u9009\u62e9\u4e00\u6761\u8bb0\u5f55\u8fdb\u884c\u4fee\u6539")
            return

        uid = int(selected[0])
        txn = self._find_txn_by_uid(uid)
        if txn is None:
            messagebox.showwarning("\u63d0\u793a", "\u672a\u627e\u5230\u5bf9\u5e94\u8bb0\u5f55")
            return

        form = self._prompt_buy_dialog(
            title="\u4fee\u6539\u8bb0\u5f55",
            initial_date=txn.get("date", dt.date.today().isoformat()),
            initial_shares=float(txn.get("shares", 0.0)),
            initial_nav=to_float(txn.get("nav"), default=None),
        )
        if form is None:
            return

        txn["date"] = form["date"]
        txn["shares"] = form["shares"]
        txn["nav"] = form["nav"]
        self.transactions.sort(key=lambda x: (x["date"], x["uid"]))
        self._save_trade_doc()
        self.refresh_fund_view()

    def delete_selected_records(self) -> None:
        selected = self.tx_tree.selection()
        if not selected:
            messagebox.showwarning("\u63d0\u793a", "\u8bf7\u5148\u9009\u62e9\u8981\u5220\u9664\u7684\u8bb0\u5f55")
            return

        if not messagebox.askyesno("\u786e\u8ba4", f"\u786e\u5b9a\u5220\u9664\u9009\u4e2d\u7684 {len(selected)} \u6761\u8bb0\u5f55\u5417\uff1f"):
            return

        to_delete = {int(iid) for iid in selected}
        self.transactions = [x for x in self.transactions if x["uid"] not in to_delete]
        self._persist_fund_changes()
        self.refresh_fund_list(keep_current=True)
        self.refresh_fund_view()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/funds.yaml", help="config file path")
    args = parser.parse_args()

    app = JijinUI(config_path=Path(args.config))
    app.mainloop()


if __name__ == "__main__":
    main()
