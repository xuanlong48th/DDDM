#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quan ly Danh muc (Portfolio) - logic thuan, KHONG phu thuoc Streamlit.

- Luu tru: file portfolio.json canh app.py (danh sach cac vi the).
- Dinh gia: nhan price_fn(symbol) -> (gia_hien_tai, phien_du_lieu) tu ben ngoai
  (app.py truyen ham lay gia VCI truc tiep, co cache).
- Nguong chien luoc dong bo voi bo loc Breakout: chot loi +10%, cat lo -7%.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

ICT = timezone(timedelta(hours=7))
PORTFOLIO_FILE = Path(__file__).resolve().parent / "portfolio.json"

TAKE_PROFIT_PCT = 10.0  # >= +10%: goi y chot loi mot phan
STOP_LOSS_PCT = 7.0     # <= -7%: canh bao cat lo khan cap


# ----------------------------------------------------------------------------
# Luu tru
# ----------------------------------------------------------------------------
def load_portfolio(path: Path = PORTFOLIO_FILE) -> list:
    """Doc danh muc tu file JSON. Tra ve [] neu chua co hoac file loi."""
    try:
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
    except Exception:
        pass
    return []


def save_portfolio(items: list, path: Path = PORTFOLIO_FILE) -> None:
    path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def add_position(items: list, symbol: str, qty, buy_price, buy_date) -> tuple:
    """Them vi the sau khi validate. Tra ve (ok: bool, message: str)."""
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return False, "Vui lòng nhập mã cổ phiếu."
    try:
        qty = int(qty)
        buy_price = int(buy_price)
    except (TypeError, ValueError):
        return False, "Khối lượng và giá mua phải là số."
    if qty <= 0:
        return False, "Khối lượng phải lớn hơn 0."
    if buy_price <= 0:
        return False, "Giá mua phải lớn hơn 0."
    if hasattr(buy_date, "isoformat"):
        buy_date = buy_date.isoformat()
    items.append({
        "symbol": symbol,
        "qty": qty,
        "buy_price": buy_price,
        "buy_date": str(buy_date),
    })
    return True, f"Đã thêm {symbol} vào danh mục."


def remove_position(items: list, index: int) -> tuple:
    """Xoa vi the theo index. Tra ve (ok: bool, message: str)."""
    if 0 <= index < len(items):
        sym = items[index].get("symbol", "?")
        del items[index]
        return True, f"Đã xóa {sym} khỏi danh mục."
    return False, "Vị thế không tồn tại."


# ----------------------------------------------------------------------------
# T+2 & chien luoc
# ----------------------------------------------------------------------------
def t2_arrival(buy_date_iso: str):
    """Ngay hang ve = ngay mua + 2 ngay lam viec (bo T7/CN; ngay le chua xu ly)."""
    d = datetime.strptime(str(buy_date_iso), "%Y-%m-%d").date()
    added = 0
    while added < 2:
        d += timedelta(days=1)
        if d.weekday() < 5:
            added += 1
    return d


def suggest_strategy(pnl_pct: float) -> str:
    """De xuat chien luoc theo nguong +10% / -7%."""
    if pnl_pct >= TAKE_PROFIT_PCT:
        return f"🎯 Chốt lời một phần — đã đạt mục tiêu +{TAKE_PROFIT_PCT:.0f}%."
    if pnl_pct <= -STOP_LOSS_PCT:
        return f"🚨 Cắt lỗ khẩn cấp — đã thủng ngưỡng -{STOP_LOSS_PCT:.0f}%."
    if pnl_pct >= 0:
        return "✅ Giữ — canh chốt lời khi chạm mục tiêu."
    return "⚠️ Theo dõi sát — đang âm, chưa chạm ngưỡng cắt lỗ."


def value_positions(items: list, price_fn, today=None) -> tuple:
    """Dinh gia tung vi the.

    price_fn(symbol) -> (gia_hien_tai: float, phien_du_lieu: str); co the raise.
    Tra ve (rows: list[dict], totals: dict).
    """
    today = today or datetime.now(ICT).date()
    rows = []
    totals = {"invested": 0, "cur_value": 0, "pnl": 0}
    for i, p in enumerate(items):
        row = {
            "index": i,
            "symbol": p.get("symbol", "?"),
            "qty": int(p.get("qty", 0)),
            "buy_price": int(p.get("buy_price", 0)),
            "buy_date": str(p.get("buy_date", "")),
        }
        try:
            buy_d = datetime.strptime(row["buy_date"], "%Y-%m-%d").date()
        except ValueError:
            buy_d = today
        row["days_held"] = max((today - buy_d).days, 0)
        arrival = t2_arrival(row["buy_date"]) if row["buy_date"] else today
        row["t2_date"] = arrival.strftime("%d/%m")
        row["t2_done"] = today >= arrival

        invested = row["qty"] * row["buy_price"]
        row["invested"] = invested
        try:
            cur_price, sess = price_fn(row["symbol"])
            cur_price = float(cur_price)
            cur_value = row["qty"] * cur_price
            pnl = cur_value - invested
            pnl_pct = (pnl / invested * 100) if invested else 0.0
            row.update({
                "cur_price": cur_price,
                "price_session": sess,
                "cur_value": cur_value,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "suggestion": suggest_strategy(pnl_pct),
                "error": None,
            })
            totals["invested"] += invested
            totals["cur_value"] += cur_value
            totals["pnl"] += pnl
        except Exception as e:  # noqa: BLE001 - ma loi gia: giu vi the, bao loi rieng
            row.update({
                "cur_price": None,
                "price_session": None,
                "cur_value": None,
                "pnl": None,
                "pnl_pct": None,
                "suggestion": "⏸️ Không lấy được giá — thử lại sau.",
                "error": str(e)[:80],
            })
        rows.append(row)
    totals["pnl_pct"] = (totals["pnl"] / totals["invested"] * 100) if totals["invested"] else 0.0
    return rows, totals
