#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BUOC 5+7: Web Dashboard (Streamlit) - Bo loc co phieu HOSE + Quan ly Danh muc.

Tai su dung engine quet san co tu main.py (VCI API truc tiep, khong phu thuoc vnstock):
  get_hose_watchlist(), screen_symbol() + cac nguong loc chuan.
Danh muc: logic thuan trong portfolio.py, luu portfolio.json canh app.py.

Chay tren PC:
  streamlit run app.py
Xem tren dien thoai (cung mang WiFi):
  streamlit run app.py --server.address 0.0.0.0
  roi mo http://<dia-chi-IP-cua-PC>:8501 tren dien thoai
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import streamlit as st

from main import (
    fetch_ohlc,
    get_hose_watchlist,
    screen_symbol,
    fmt_int,
    PRICE_MIN,
    VOL_MIN,
    VOL_SPIKE,
    LOOKBACK,
    MAX_WORKERS,
    ICT,
)
from portfolio import (
    load_portfolio,
    save_portfolio,
    add_position,
    remove_position,
    value_positions,
    TAKE_PROFIT_PCT,
    STOP_LOSS_PCT,
)

# === CẤU HÌNH GIAO DIỆN TRANG WEB ===
st.set_page_config(
    page_title="Stock Screener - Săn Cổ Phiếu Breakout",
    page_icon="📈",
    layout="wide",
)


@st.cache_data(ttl=3600)  # luu cache danh sach HOSE 1 tieng cho web chay nhanh hon
def cached_watchlist() -> list:
    return get_hose_watchlist()


@st.cache_data(ttl=300)  # gia thi truong refresh moi 5 phut
def current_price(symbol: str) -> tuple:
    """Gia dong cua moi nhat hien co (gom ca phien live neu dang giao dich)."""
    df = fetch_ohlc(symbol)
    latest = df.iloc[-1]
    sess = datetime.fromtimestamp(int(latest["time"]), ICT).date().isoformat()
    return float(latest["close"]), sess


def run_scan(progress_cb) -> tuple:
    """Quet song song toan san HOSE, bao tien do ve UI qua progress_cb(done, total, symbol)."""
    symbols = cached_watchlist()
    total = len(symbols)
    results, errors = [], []
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(screen_symbol, s): s for s in symbols}
        for fut in as_completed(futures):  # chay tren main thread -> cap nhat UI an toan
            sym = futures[fut]
            try:
                res = fut.result()
                if res:
                    results.append(res)
            except Exception as e:  # noqa: BLE001 - ma loi tu bo qua, quet tiep
                errors.append((sym, str(e)[:60]))
            done += 1
            progress_cb(done, total, sym)
    return results, errors, total


def fmt_signed(x) -> str:
    """Dinh dang so co dau, ngan cach hang nghin kieu VN."""
    s = fmt_int(int(round(abs(x))))
    return ("+" if x >= 0 else "-") + s


# === TAB GIAO DIEN ===
tab_scan, tab_port = st.tabs(["🔍 Quét Breakout", "💼 Quản lý Danh mục (Portfolio)"])


# ----------------------------------------------------------------------------
# TAB 1: Quet Breakout
# ----------------------------------------------------------------------------
with tab_scan:
    st.title("🚀 Hệ thống Săn Cổ Phiếu Breakout (HOSE)")
    st.markdown(
        "Công cụ tự động quét dòng tiền lớn: "
        "tìm cổ phiếu **vượt đỉnh 20 phiên** kèm **khối lượng đột biến**."
    )

    with st.expander("⚙️ Tiêu chí bộ lọc & quản trị rủi ro đang áp dụng"):
        st.markdown(
            f"- Giá đóng cửa ≥ **{fmt_int(PRICE_MIN)} VND**\n"
            f"- Khối lượng phiên mới nhất ≥ **{fmt_int(VOL_MIN)}** cổ phiếu\n"
            f"- Khối lượng ≥ **{VOL_SPIKE}** lần trung bình {LOOKBACK} phiên (Vol/MA{LOOKBACK})\n"
            f"- Giá vượt đỉnh cao nhất {LOOKBACK} phiên trước\n"
            f"- 🎯 **Mục tiêu chốt lời:** giá mua × 1.10 (+10%)\n"
            f"- ⚠️ **Cắt lỗ:** mức cao hơn trong 2 mức — giá mua × 0.93 (cứng -7%) "
            f"hoặc đáy phiên breakout × 0.99 (đệm 1%); "
            f"thủng mức này khi hàng về T+2 thì cắt để kiểm soát rủi ro\n"
            f"- 💰 **Vùng mua T+1:** từ giá tham chiếu P (xem như bằng giá breakout) "
            f"đến tối đa +2% — sáng mai mở cửa **gap up > 3% thì không mua đuổi**"
        )

    if "scan" not in st.session_state:
        st.session_state.scan = None

    col1, col2 = st.columns([1, 4])
    with col1:
        scan_button = st.button(
            "🔍 Chạy quét toàn sàn HOSE ngay", type="primary", use_container_width=True
        )

    if scan_button:
        progress_bar = st.progress(0, text="Đang chuẩn bị...")
        t0 = time.time()

        def _cb(done, total, sym):
            progress_bar.progress(done / total, text=f"Đang quét ({done}/{total}): {sym}...")

        with st.spinner("Hệ thống đang tải dữ liệu và phân tích toàn bộ sàn HOSE..."):
            results, errors, total = run_scan(_cb)

        progress_bar.empty()
        elapsed = time.time() - t0
        sess_date = results[0]["Phien"] if results else datetime.now(ICT).date().isoformat()
        st.session_state.scan = {
            "results": results,
            "errors": errors,
            "total": total,
            "elapsed": elapsed,
            "sess_date": sess_date,
            "run_at": datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S"),
        }

    scan = st.session_state.scan
    if scan:
        results = scan["results"]
        st.caption(
            f"Lần quét: {scan['run_at']} (ICT) • Phiên dữ liệu: {scan['sess_date']} • "
            f"Đã quét {scan['total']} mã trong {scan['elapsed']:.0f}s • "
            f"{len(scan['errors'])} mã lỗi (tự động bỏ qua)"
        )
        if results:
            st.success(f"🎉 Đã tìm thấy **{len(results)}** cổ phiếu đạt chuẩn Breakout!")

            df = (
                pd.DataFrame(results)
                .sort_values("Vol/MA20", ascending=False)
                .reset_index(drop=True)
            )
            df_show = pd.DataFrame({
                "Mã CP": df["Ma"],
                "Giá Breakout": df["Gia (VND)"].map(fmt_int),
                "Vùng Mua T+1": df["Vùng mua T+1"],
                "Cắt Lỗ (-7%)": df["Cắt lỗ"].map(fmt_int),
                "Chốt Lời (+10%)": df["Mục tiêu"].map(fmt_int),
                "Tỷ lệ tăng Vol": df["Vol/MA20"].map(lambda x: f"{x}x"),
            })
            st.dataframe(df_show, use_container_width=True, hide_index=True)
        else:
            st.warning(
                "Hôm nay không có mã cổ phiếu nào trên sàn HOSE "
                "thỏa mãn đủ tiêu chí kỹ thuật."
            )


# ----------------------------------------------------------------------------
# TAB 2: Quan ly Danh muc (Portfolio)
# ----------------------------------------------------------------------------
with tab_port:
    st.title("💼 Quản lý Danh mục (Portfolio)")
    st.markdown(
        "Nhập vị thế thủ công, theo dõi lãi/lỗ theo **giá thị trường mới nhất** "
        f"và nhận đề xuất chiến lược theo ngưỡng **+{TAKE_PROFIT_PCT:.0f}% / -{STOP_LOSS_PCT:.0f}%**."
    )

    # ---- Form them vi the ----
    with st.form("add_position", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            in_sym = st.text_input("Mã CP", placeholder="VTO")
        with c2:
            in_qty = st.number_input("Khối lượng", min_value=0, step=100, value=0)
        with c3:
            in_price = st.number_input("Giá mua (VND)", min_value=0, step=100, value=0)
        with c4:
            in_date = st.date_input(
                "Ngày mua",
                value=datetime.now(ICT).date(),
                max_value=datetime.now(ICT).date(),
            )
        submitted = st.form_submit_button("➕ Thêm vào danh mục", type="primary")

    if submitted:
        items = load_portfolio()
        ok, msg = add_position(items, in_sym, in_qty, in_price, in_date)
        if ok:
            save_portfolio(items)
            st.success(msg)
            st.rerun()
        else:
            st.error(msg)

    items = load_portfolio()
    if not items:
        st.info("Danh mục đang trống. Thêm vị thế đầu tiên ở form bên trên.")
    else:
        rows, totals = value_positions(items, current_price)

        # ---- Thong ke tong ----
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("💰 Tổng đầu tư", f"{fmt_int(totals['invested'])} VND")
        with m2:
            st.metric("📊 Giá trị hiện tại", f"{fmt_int(int(totals['cur_value']))} VND")
        with m3:
            pnl, pct = totals["pnl"], totals["pnl_pct"]
            st.metric(
                "📈 Tổng Lãi/Lỗ",
                f"{fmt_signed(pnl)} VND",
                f"{pnl_pct:+.2f}%",
            )

        # ---- Bang chi tiet + de xuat ----
        df_pf = pd.DataFrame([{
            "Mã CP": r["symbol"],
            "KL": fmt_int(r["qty"]),
            "Giá mua": fmt_int(r["buy_price"]),
            "Giá hiện tại": fmt_int(int(r["cur_price"])) if r["cur_price"] else "—",
            "Giá trị HT": fmt_int(int(r["cur_value"])) if r["cur_value"] else "—",
            "Lãi/Lỗ (VND)": fmt_signed(r["pnl"]) if r["pnl"] is not None else "—",
            "Lãi/Lỗ (%)": f"{r['pnl_pct']:+.2f}%" if r["pnl_pct"] is not None else "—",
            "Ngày giữ": r["days_held"],
            "T+2": ("✅ Đã về" if r["t2_done"]
                    else f"⏳ Về {r['t2_date']}"),
            "Đề xuất chiến lược": r["suggestion"],
        } for r in rows])
        st.dataframe(df_pf, use_container_width=True, hide_index=True)
        st.caption(
            "Giá hiện tại = giá đóng cửa mới nhất có trên VCI "
            "(gồm phiên live nếu đang trong giờ giao dịch), refresh mỗi 5 phút."
        )

        # ---- Xoa vi the ----
        st.markdown("---")
        dc1, dc2 = st.columns([3, 1])
        with dc1:
            del_idx = st.selectbox(
                "Chọn vị thế cần xóa",
                options=list(range(len(rows))),
                format_func=lambda i: f"{rows[i]['symbol']} — "
                                      f"{fmt_int(rows[i]['qty'])} cp @ {fmt_int(rows[i]['buy_price'])}",
            )
        with dc2:
            st.write("")  # can dong voi selectbox
            if st.button("🗑️ Xóa vị thế", use_container_width=True):
                ok, msg = remove_position(items, del_idx)
                if ok:
                    save_portfolio(items)
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)


st.markdown("---")
st.caption(
    "Dữ liệu: VCI (VietCap) • Chạy: streamlit run app.py • "
    "Xem trên điện thoại cùng WiFi: http://<IP-của-PC>:8501"
)
