#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BUOC 4: Bo loc co phieu Viet Nam - Quet TOAN SAN HOSE + Scheduler.

- Tu dong lay danh sach toan bo ma co phieu san HOSE (khong dung list cung).
- Voi moi ma: tai du lieu lich su va ap dung bo loc tieu chuan:
    1. Thi gia (Close) >= 10.000 VND
    2. Khoi luong phien moi nhat >= 1.000.000 co phieu
    3. Khoi luong dot bien: >= 2.0 lan trung binh 20 phien (Vol_MA20)
    4. Gia dong cua vuot dinh cao nhat cua 20 phien truoc (Breakout)
- Moi ma duoc boc try-except rieng: ma loi tu dong bo qua, vong lap chay tiep.
- Gui canh bao qua Telegram Bot sau moi lan quet.
- Che do chay tu dong: lich T2-T6 luc 15:15 (gio dia phuong cua may).

Nguon du lieu: VCI API cong khai (https://trading.vietcap.com.vn)
  + Danh sach niem yet:  GET /api/price/symbols/getAll  (board='HSX' ~ san HOSE)
  + Du lieu nen ngay:    POST /api/chart/OHLCChart/gap-chart
  Khong can API key, khong phu thuoc thu vien vnstock.

Chay:
  python main.py                 # quet 1 lan roi thoat (chi danh gia phien da hoan tat)
  python main.py --live          # danh gia ca nen phien dang dien ra (neu chay trong gio GD)
  python main.py --schedule      # chay nen: lich T2-T6 15:15 + bot nghe lenh Telegram
  python main.py --schedule --now  # quet ngay 1 lan roi vao che do nen
  python main.py --listen        # chi chay bot Telegram (/scan), khong lap lich

Bot Telegram: nhan /scan de quet ngay (chi chap nhan tu dung chat_id da cau hinh).
"""

import argparse
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests
import schedule

# ----------------------------------------------------------------------------
# Cau hinh
# ----------------------------------------------------------------------------
# Danh sach du phong neu API danh sach niem yet gap su co
FALLBACK_SYMBOLS = [
    "VIC", "VHM", "SSI", "VIX", "STB", "VPB", "HPG", "MWG",
    "MBB", "TCB", "DIG", "NVL", "FPT", "ACB", "SHB", "CTG",
]

PRICE_MIN = 10_000        # VND
VOL_MIN = 1_000_000       # co phieu
VOL_SPIKE = 2.0           # lan so voi trung binh 20 phien
LOOKBACK = 20             # so phien tham chieu
COUNT_BACK = 45           # so nen lay ve (du phong ngay nghi/le)
MAX_WORKERS = 5           # so luong quet song song (lich su voi server)

# Quy tac vung mua T+1 (phien ngay mai):
#  - Gia tham chieu sang mai xem nhu bang gia breakout (P)
#  - Vung mua toi uu: tu P den toi da +2% (P * 1.02)
#  - Mo cua gap up > 3% so voi P -> canh bao "Khong mua duoi (Gap qua cao)"
BUY_REF = 1.00
BUY_MAX = 1.02
GAP_WARN = 1.03

# Chi danh gia phien DA HOAN TAT. Neu chay trong gio giao dich (HOSE 9:00-15:00),
# nen ngay moi nhat la phien dang dien ra (vol chua day du) -> tu dong bo qua
# va dung phien hoan tat gan nhat de danh gia. Dat False de giu hanh vi cu
# (danh gia ca nen dang dien ra), hoac dung flag CLI --live cho 1 lan chay.
COMPLETED_SESSION_ONLY = True

# ----------------------------------------------------------------------------
# Cau hinh Telegram Bot (dien Token va Chat ID that de nhan canh bao)
# Cach lay: chat voi @BotFather (/newbot) de lay Token,
#           chat voi @userinfobot de lay Chat ID cua ban.
# De trong ("") thi script van chay binh thuong, chi bo qua buoc gui tin nhan.
# ----------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = ""

BASE_URL = "https://trading.vietcap.com.vn/api"
URL_SYMBOLS = BASE_URL + "/price/symbols/getAll"
URL_OHLC = BASE_URL + "/chart/OHLCChart/gap-chart"
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://trading.vietcap.com.vn/",
    "Origin": "https://trading.vietcap.com.vn/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0 Safari/537.36"
    ),
}
ICT = timezone(timedelta(hours=7))

session = requests.Session()
session.headers.update(HEADERS)


# ----------------------------------------------------------------------------
# Buoc 1: Lay danh sach toan san HOSE
# ----------------------------------------------------------------------------
def get_hose_watchlist() -> list:
    """Tai danh sach niem yet va loc ra cac ma co phieu san HOSE (board='HSX')."""
    print("Dang tai danh sach toan bo doanh nghiep niem yet tu thi truong...")
    try:
        r = session.get(URL_SYMBOLS, timeout=25)
        r.raise_for_status()
        data = r.json()
        hose = [
            x["symbol"] for x in data
            if str(x.get("board", "")).upper() in ("HSX", "HOSE")
            and str(x.get("type", "")).upper() == "STOCK"
        ]
        # Loai bo trung lap, sap xep de ket qua on dinh
        hose = sorted(set(hose))
        if not hose:
            raise ValueError("API tra ve danh sach rong")
        print(f"Da lay thanh cong {len(hose)} ma co phieu tu san HOSE.")
        return hose
    except Exception as e:  # noqa: BLE001
        print(f"Loi khi tai danh sach san HOSE: {e}")
        print(f"Dung danh sach du phong {len(FALLBACK_SYMBOLS)} ma.")
        return list(FALLBACK_SYMBOLS)


# ----------------------------------------------------------------------------
# Buoc 2: Lay du lieu OHLCV tung ma
# ----------------------------------------------------------------------------
def fetch_ohlc(symbol: str, retries: int = 2) -> pd.DataFrame:
    """Lay nen ngay (D1) cua mot ma. Tra ve DataFrame [time, open, high, low, close, volume]."""
    payload = {
        "timeFrame": "ONE_DAY",
        "symbols": [symbol],
        "to": int(time.time()),
        "countBack": COUNT_BACK,
    }
    last_err = None
    for _ in range(retries + 1):
        try:
            r = session.post(URL_OHLC, json=payload, timeout=25)
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list) or not data:
                raise ValueError("API tra ve du lieu rong")
            s = data[0]
            df = pd.DataFrame({
                "time": pd.to_numeric(s["t"], errors="coerce").astype("int64"),
                "open": pd.to_numeric(s["o"], errors="coerce"),
                "high": pd.to_numeric(s["h"], errors="coerce"),
                "low": pd.to_numeric(s["l"], errors="coerce"),
                "close": pd.to_numeric(s["c"], errors="coerce"),
                "volume": pd.to_numeric(s["v"], errors="coerce"),
            }).dropna().sort_values("time").reset_index(drop=True)
            if len(df) < LOOKBACK + 1:
                raise ValueError(f"chi lay duoc {len(df)} phien (< {LOOKBACK + 1})")
            return df
        except Exception as e:  # noqa: BLE001 - muon bat moi loi mang/API
            last_err = e
            time.sleep(1.0)
    raise RuntimeError(f"khong lay duoc du lieu {symbol}: {last_err}")


# ----------------------------------------------------------------------------
# Buoc 3: Bo loc tieu chuan cho 1 ma
# ----------------------------------------------------------------------------
_intraday_notice_lock = threading.Lock()
_intraday_notice_shown = False


def _latest_candle_is_intraday(latest_time: int) -> tuple[bool, str]:
    """True neu nen moi nhat la phien dang dien ra (hom nay, truoc 15:05 ICT ngay trong tuan)."""
    now = datetime.now(ICT)
    candle_date = datetime.fromtimestamp(int(latest_time), ICT).date()
    if candle_date != now.date():
        return False, ""
    if now.weekday() >= 5:  # cuoi tuan: khong the co phien dang dien ra
        return False, ""
    cutoff = now.replace(hour=15, minute=5, second=0, microsecond=0)
    if now < cutoff:
        return True, candle_date.isoformat()
    return False, ""


def screen_symbol(symbol: str, completed_only: bool = COMPLETED_SESSION_ONLY) -> dict | None:
    """Ap dung 4 dieu kien loc. Tra ve dict ket qua neu DAT, None neu bi loai."""
    global _intraday_notice_shown
    df = fetch_ohlc(symbol)

    if completed_only:
        intraday, sess = _latest_candle_is_intraday(int(df.iloc[-1]["time"]))
        if intraday:
            with _intraday_notice_lock:
                if not _intraday_notice_shown:
                    prev_sess = datetime.fromtimestamp(int(df.iloc[-2]["time"]), ICT).date().isoformat()
                    print(f"Luu y: phien {sess} dang dien ra (vol chua day du) -> "
                          f"tu dong danh gia phien hoan tat gan nhat ({prev_sess}). "
                          f"Dung --live de ep danh gia nen dang dien ra.")
                    _intraday_notice_shown = True
            df = df.iloc[:-1].reset_index(drop=True)

    latest = df.iloc[-1]
    prev = df.iloc[-(LOOKBACK + 1):-1]  # 20 phien lien truoc (khong tinh phien moi nhat)

    close = float(latest["close"])
    low = float(latest["low"])
    volume = float(latest["volume"])
    vol_ma20 = float(prev["volume"].mean())
    vol_ratio = volume / vol_ma20 if vol_ma20 > 0 else 0.0
    high_20 = float(prev["high"].max())
    breakout_pct = (close - high_20) / high_20 * 100 if high_20 > 0 else 0.0

    if not (close >= PRICE_MIN):
        return None
    if not (volume >= VOL_MIN):
        return None
    if not (vol_ratio >= VOL_SPIKE):
        return None
    if not (close > high_20):
        return None

    # ---- Quan tri rui ro cho giao dich T+2 ----
    # Cat lo cung: khong bao gio de lo qua 7% (close * 0.93). Neu day phien
    # breakout nam gan hon (low * 0.99, dem 1%), lay muc chat hon de bao ve von.
    stop_loss = max(close * 0.93, low * 0.99)
    take_profit = close * 1.10  # ky vong chot loi 10%
    # Vung mua T+1 (thay the quy tac cu ±2%): tu gia tham chieu P (xem nhu bang
    # close phien breakout) den toi da +2%. Canh bao gap up > 3% ap dung sang
    # mai luc mo cua (khong danh gia duoc tai thoi diem quet) -> ghi chu tinh.
    buy_lo = int(round(close * BUY_REF))
    buy_hi = int(round(close * BUY_MAX))
    buy_zone_t1 = f"{fmt_int(buy_lo)}–{fmt_int(buy_hi)}"

    sess_date = datetime.fromtimestamp(int(latest["time"]), ICT).date().isoformat()
    return {
        "Ma": symbol,
        "Phien": sess_date,
        "Gia (VND)": int(round(close)),
        "Vùng mua T+1": buy_zone_t1,
        "Khoi luong": int(round(volume)),
        "KL TB 20P": int(round(vol_ma20)),
        "Vol/MA20": round(vol_ratio, 2),
        "Vuot dinh": f"{breakout_pct:+.2f}%",
        "Cắt lỗ": int(round(stop_loss)),
        "Mục tiêu": int(round(take_profit)),
    }


def t2_date(sess_date: str) -> str:
    """Ngay hang ve T+2 (cong 2 ngay lam viec, bo T7/CN; ngay le chua xu ly)."""
    d = datetime.strptime(sess_date, "%Y-%m-%d").date()
    added = 0
    while added < 2:
        d += timedelta(days=1)
        if d.weekday() < 5:
            added += 1
    return d.strftime("%d/%m")


def fmt_int(x: int) -> str:
    return f"{x:,}".replace(",", ".")


# ----------------------------------------------------------------------------
# Telegram Bot
# ----------------------------------------------------------------------------
def send_telegram_message(message: str, chat_id=None) -> bool:
    """Gui tin nhan qua Telegram Bot API. Tra ve True neu gui thanh cong.

    chat_id: neu de None thi dung TELEGRAM_CHAT_ID cau hinh san (kenh thong bao
    chung); truyen chat_id cu the de tra loi rieng cho nguoi goi lenh.
    """
    token_ok = bool(TELEGRAM_BOT_TOKEN) and "YOUR_" not in TELEGRAM_BOT_TOKEN
    target = chat_id or TELEGRAM_CHAT_ID
    if not token_ok or not target:
        print("Chua cau hinh Telegram Bot Token/Chat ID -> bo qua gui tin nhan.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": target, "text": message, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code == 200:
            print("Da gui canh bao ve Telegram thanh cong!")
            return True
        print(f"Loi gui Telegram (HTTP {r.status_code}): {r.text[:150]}")
        return False
    except Exception as e:  # noqa: BLE001
        print(f"Loi ket noi Telegram API: {e}")
        return False


def _tg_api(method: str, params: dict | None = None, timeout: int = 30):
    """Goi mot method cua Telegram Bot API. Tra ve result neu ok, None neu loi."""
    if not TELEGRAM_BOT_TOKEN or "YOUR_" in TELEGRAM_BOT_TOKEN:
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    try:
        r = requests.post(url, json=params or {}, timeout=timeout)
        data = r.json()
        if data.get("ok"):
            return data.get("result")
        print(f"Telegram API '{method}' loi: {str(data.get('description', ''))[:120]}")
        return None
    except Exception as e:  # noqa: BLE001
        print(f"Loi goi Telegram API '{method}': {e}")
        return None


def build_telegram_message(results: list, sess_date: str) -> str:
    """Dung noi dung tin nhan Markdown tu danh sach ma DAT."""
    if not results:
        return (f"🤖 Quét HOSE (phiên {sess_date}): "
                "hôm nay không có mã nào đạt chuẩn Breakout.")
    t2 = t2_date(sess_date)
    lines = [f"🚨 *PHÁT HIỆN BREAKOUT (HOSE)* 🚨",
             f"Phiên {sess_date} | {len(results)} mã đạt chuẩn | Hàng về T+2: {t2}", ""]
    for r in results:
        close_px = int(r["Gia (VND)"])
        buy_lo = fmt_int(int(round(close_px * BUY_REF)))
        buy_hi = fmt_int(int(round(close_px * BUY_MAX)))
        lines.append(
            f"• *{r['Ma']}* — Giá breakout: `{fmt_int(close_px)}` | "
            f"Vol: `{fmt_int(r['Khoi luong'])}` (`{r['Vol/MA20']}x` MA20)"
        )
        lines.append(
            f"  👉 Vùng mua khuyến nghị sáng mai: `{buy_lo}` đến `{buy_hi}` "
            f"(Lưu ý: Không mua nếu gap up > 3%)."
        )
        lines.append(f"  🎯 Chốt lời (+10%): `{fmt_int(r['Mục tiêu'])}`")
        lines.append(
            f"  ⚠️ Cắt lỗ nếu giá thủng: `{fmt_int(r['Cắt lỗ'])}` "
            f"khi hàng về T+2 ({t2}) để kiểm soát rủi ro."
        )
        lines.append("")
    return "\n".join(lines).rstrip()


# ----------------------------------------------------------------------------
# Chay quet toan san (1 job hoan chinh: quet + in bang + gui Telegram)
# ----------------------------------------------------------------------------
def _run_screener_job_inner(completed_only: bool = COMPLETED_SESSION_ONLY,
                            notify_chat_id=None) -> int:
    run_time = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")
    print("=" * 90)
    print("BO LOC CO PHIEU VIET NAM - QUET TOAN SAN HOSE: BREAKOUT + DOT BIEN KHOI LUONG")
    print(f"Thoi gian chay: {run_time} (ICT) | Nguon du lieu: VCI")
    print(f"Dieu kien: Close >= {fmt_int(PRICE_MIN)} | Vol >= {fmt_int(VOL_MIN)} | "
          f"Vol >= {VOL_SPIKE}x MA{LOOKBACK} | Close > dinh {LOOKBACK} phien")
    if completed_only:
        print("Che do: chi danh gia phien DA HOAN TAT (bo qua nen dang dien ra neu chay trong gio GD)")
    print("=" * 90)

    symbols = get_hose_watchlist()
    total = len(symbols)
    print(f"Bat dau quet thuat toan cho toan bo {total} ma (toi da {MAX_WORKERS} luong song song)...")

    results, errors = [], []
    done = 0
    lock = threading.Lock()

    def work(sym: str):
        nonlocal done
        try:
            res = screen_symbol(sym, completed_only=completed_only)
            if res:
                with lock:
                    results.append(res)
        except Exception as e:  # noqa: BLE001 - bo qua ma loi, chay tiep ma khac
            with lock:
                errors.append((sym, str(e)[:60]))
        finally:
            with lock:
                done += 1
                if done % 40 == 0 or done == total:
                    print(f"  ... da quet {done}/{total} ma ({len(results)} DAT)", flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        list(ex.map(work, symbols))

    # ---- Bang ket qua cuoi cung ----
    print()
    print("=" * 90)
    print("KET QUA QUET TOAN SAN HOSE - TIN HIEU BREAKOUT")
    print("=" * 90)
    if results:
        df = pd.DataFrame(results).sort_values("Vol/MA20", ascending=False).reset_index(drop=True)
        # Format so cho de doc
        df["Gia (VND)"] = df["Gia (VND)"].map(fmt_int)
        df["Khoi luong"] = df["Khoi luong"].map(fmt_int)
        df["KL TB 20P"] = df["KL TB 20P"].map(fmt_int)
        print(df.to_string(index=False))
    else:
        print("Hom nay khong co co phieu nao tren san HOSE thoa man du tieu chi ky thuat.")

    print("-" * 90)
    print(f"Tong ket: da quet {total} ma | {len(results)} ma DAT | {len(errors)} ma loi (tu dong bo qua)")
    if errors and len(errors) <= 10:
        for sym, msg in errors:
            print(f"  - {sym}: {msg}")

    # ---- Gui canh bao qua Telegram ----
    print()
    print("Dang gui thong bao qua Telegram...")
    sess_date = results[0]["Phien"] if results else datetime.now(ICT).date().isoformat()
    send_telegram_message(build_telegram_message(results, sess_date),
                          chat_id=notify_chat_id)
    return 0


# ----------------------------------------------------------------------------
# Dieu phoi quet (khoa chong chay chong cheo) + lang nghe lenh Telegram
# ----------------------------------------------------------------------------
_SCAN_LOCK = threading.Lock()


def run_screener_job(completed_only: bool = COMPLETED_SESSION_ONLY,
                     notify_chat_id=None) -> int:
    """Wrapper cong khai: chi cho 1 phien quet chay tai mot thoi diem.

    Neu co phien quet khac dang chay (vi du lich 15:15 trung lenh /scan),
    bao ban va tra ve 1 thay vi chay chong cheo.
    """
    if not _SCAN_LOCK.acquire(blocking=False):
        busy_msg = "⏳ Đang có một phiên quét khác chạy, vui lòng đợi hoàn tất rồi thử lại."
        print(busy_msg)
        send_telegram_message(busy_msg, chat_id=notify_chat_id)
        return 1
    try:
        return _run_screener_job_inner(completed_only, notify_chat_id)
    finally:
        _SCAN_LOCK.release()


def _scan_and_report(chat_id) -> None:
    """Chay quet theo lenh /scan (tren luong rieng) roi bao ket qua ve chat goi lenh."""
    try:
        run_screener_job(notify_chat_id=chat_id)
    except Exception as e:  # noqa: BLE001
        print(f"Loi khi quet theo lenh /scan: {e}")
        send_telegram_message(f"⚠️ Quét thất bại: {e}", chat_id=chat_id)


def listen_telegram_commands(poll_interval: int = 2) -> None:
    """Vong lap lang nghe lenh Telegram (getUpdates long-polling).

    - /scan (hoac /scan@tenbot): chay quet toan san ngay, ket qua gui ve dung
      chat da goi lenh (chay tren luong rieng de khong chan vong lap nghe).
    - /start, /help: huong dan su dung.
    - Chi chap nhan lenh tu dung chat_id da cau hinh (TELEGRAM_CHAT_ID) de tranh
      nguoi la kich quet tren may cua ban.
    """
    if not TELEGRAM_BOT_TOKEN or "YOUR_" in TELEGRAM_BOT_TOKEN:
        print("Chua cau hinh TELEGRAM_BOT_TOKEN -> khong the lang nghe lenh Telegram.")
        return
    print("Bot Telegram dang lang nghe lenh chat (nhan /scan de quet ngay)...")

    owner = str(TELEGRAM_CHAT_ID or "")

    # Xoa hang doi tin nhan ton dong tu cac lan chay truoc (lay update cuoi roi bo qua)
    try:
        old = _tg_api("getUpdates", {"timeout": 1}, timeout=10) or []
        offset = (old[-1]["update_id"] + 1) if old else 0
    except Exception:
        offset = 0

    while True:
        try:
            updates = _tg_api("getUpdates", {"offset": offset, "timeout": 25}, timeout=35)
            if not updates:
                time.sleep(poll_interval)
                continue
            for u in updates:
                offset = u.get("update_id", offset) + 1
                msg = u.get("message") or {}
                text = (msg.get("text") or "").strip()
                chat_id = (msg.get("chat") or {}).get("id")
                if not text or chat_id is None:
                    continue
                if owner and str(chat_id) != owner:
                    print(f"Bo qua lenh tu chat la {chat_id}: '{text[:30]}'")
                    continue
                cmd = text.split()[0].split("@")[0].lower()
                if cmd == "/scan":
                    send_telegram_message(
                        "⏳ Nhận lệnh! Hệ thống đang quét toàn sàn HOSE, vui lòng đợi ~1 phút...",
                        chat_id=chat_id,
                    )
                    threading.Thread(target=_scan_and_report, args=(chat_id,),
                                     daemon=True).start()
                elif cmd in ("/start", "/help"):
                    send_telegram_message(
                        "🤖 *Bot quét HOSE*\n"
                        "/scan — quét toàn sàn ngay, kết quả gửi về đây\n"
                        "Bot cũng tự quét lúc 15:15 các ngày T2–T6.",
                        chat_id=chat_id,
                    )
        except Exception as e:  # noqa: BLE001
            print(f"Loi vong lap lang nghe Telegram: {e}")
            time.sleep(5)


# ----------------------------------------------------------------------------
# Scheduler: tu dong chay T2-T6 luc 15:15 (gio dia phuong cua may)
# ----------------------------------------------------------------------------
SCHEDULE_TIME = "15:15"  # HOSE dong cua ~15:00, du lieu phien hoan tat sau do

def register_schedule() -> None:
    """Dang ky lich quet T2-T6 luc 15:15 (gio dia phuong cua may)."""
    # Thu vien `schedule` khong co monday_to_friday() -> dang ky rieng 5 ngay
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday"):
        getattr(schedule.every(), day).at(SCHEDULE_TIME).do(run_screener_job)
    for job in schedule.get_jobs():
        print(f"  - Job tiep theo: {job.next_run}")


def scheduler_loop() -> None:
    """Vong lap kiem tra lich (du kien chay tren luong rieng)."""
    while True:
        schedule.run_pending()
        time.sleep(30)


def start_scheduler(run_now: bool = False) -> None:
    """Khoi dong che do nen: 2 luong song song.

    - Luong 1: lich tu dong quet T2-T6 luc 15:15.
    - Luong 2: lang nghe lenh chat Telegram (/scan...). Tu tat neu chua cau
      hinh TELEGRAM_BOT_TOKEN.
    """
    print("=" * 90)
    print("HE THONG QUET CHUNG KHOAN TU DONG DA KHOI DONG")
    print(f"Lich chay: Thu Hai - Thu Sau, luc {SCHEDULE_TIME} (gio dia phuong cua may)")
    print("Lenh Telegram: /scan (quet ngay), /help (huong dan)")
    print("Nhan Ctrl+C de dung.")
    print("=" * 90)

    register_schedule()

    if run_now:
        run_screener_job()

    t_sched = threading.Thread(target=scheduler_loop, name="scheduler", daemon=True)
    t_tg = threading.Thread(target=listen_telegram_commands, name="telegram", daemon=True)
    t_sched.start()
    t_tg.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDa nhan lenh dung. Tam biet!")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Bo loc co phieu HOSE: Breakout + dot bien khoi luong."
    )
    parser.add_argument(
        "--schedule", action="store_true",
        help="Chay nen: tu dong quet T2-T6 luc 15:15 (gio dia phuong).",
    )
    parser.add_argument(
        "--now", action="store_true",
        help="Ket hop voi --schedule: quet ngay 1 lan truoc khi vao che do nen.",
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Danh gia ca nen phien dang dien ra (mac dinh: chi dung phien da hoan tat).",
    )
    parser.add_argument(
        "--listen", action="store_true",
        help="Chi chay bot lang nghe lenh Telegram (/scan), khong lap lich 15:15.",
    )
    args = parser.parse_args(argv)

    completed_only = COMPLETED_SESSION_ONLY and not args.live

    if args.schedule:
        start_scheduler(run_now=args.now)
        return 0
    if args.listen:
        listen_telegram_commands()
        return 0
    return run_screener_job(completed_only=completed_only)


if __name__ == "__main__":
    sys.exit(main())
