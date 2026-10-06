import time
import threading
from datetime import datetime
import schedule
import requests
import pandas as pd

# === CẤU HÌNH TELEGRAM BOT ===
# (Hãy thay Token và Chat ID thực tế của bạn vào đây khi chạy trên máy tính)
TELEGRAM_BOT_TOKEN = "8883836964:AAHM39eaMgm9m7VCTM21GRJJEV3RkbFub-8"
TELEGRAM_CHAT_ID = "6018000879"

# === THAM SỐ BỘ LỌC THỰC CHIẾN ===
PRICE_MIN = 10000       # Giá >= 10k
VOL_MIN = 1000000       # Khối lượng >= 1 triệu cổ
VOL_SPIKE = 2.0         # Vol >= 2.0 lần MA20
LOOKBACK = 20           # Đỉnh 20 phiên
MAX_WORKERS = 10        # Số luồng chạy song song

ICT = None # Mặc định xử lý múi giờ

def send_telegram_message(message):
    if "8883836964:" not in TELEGRAM_BOT_TOKEN and "YOUR_" in TELEGRAM_BOT_TOKEN:
        print("Chưa cấu hình Telegram Bot Token.")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            print(f"Lỗi gửi Telegram: {response.text}")
    except Exception as e:
        print(f"Lỗi kết nối Telegram API: {e}")

def get_hose_watchlist():
    try:
        url = "https://trading.vietcap.com.vn/api/market/stocks"
        res = requests.get(url, timeout=10)
        if res.status_code == 200:
            data = res.json()
            symbols = [item['symbol'] for item in data if item.get('exchange', '').upper() == 'HOSE']
            return symbols
    except Exception:
        pass
    # Fallback danh sách mẫu nếu gọi API lỗi
    return ['VIC', 'VHM', 'SSI', 'VIX', 'STB', 'VPB', 'HPG', 'MWG']

def fetch_ohlc(symbol: str) -> pd.DataFrame:
    # Lấy dữ liệu lịch sử giá từ VCI API
    url = f"https://trading.vietcap.com.vn/api/chart/historical?symbol={symbol}&resolution=1D&from=1704067200&to=9999999999"
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, headers=headers, timeout=10)
    data = res.json()
    df = pd.DataFrame(data)
    return df

def screen_symbol(symbol: str):
    try:
        df = fetch_ohlc(symbol)
        if df is None or len(df) < LOOKBACK + 5:
            return None
        
        df['Vol_MA20'] = df['volume'].rolling(window=LOOKBACK).mean()
        df['Price_Max20'] = df['close'].shift(1).rolling(window=LOOKBACK).max()
        
        latest = df.iloc[-1]
        prev_vol_ma20 = latest['Vol_MA20']
        max_price_20 = latest['Price_Max20']
        
        is_valid_price = latest['close'] >= PRICE_MIN
        is_valid_volume = latest['volume'] >= VOL_MIN
        is_breakout_vol = latest['volume'] >= VOL_SPIKE * prev_vol_ma20
        is_breakout_price = latest['close'] > max_price_20
        
        if is_valid_price and is_valid_volume and is_breakout_vol and is_breakout_price:
            latest_close = latest['close']
            buy_zone_max = round(latest_close * 1.02, 0)
            stop_loss = round(latest_close * 0.93, 0)
            target = round(latest_close * 1.10, 0)
            
            return {
                'Ma': symbol,
                'Gia (VND)': latest_close,
                'Vùng mua T+1': f"{latest_close:,.0f} - {buy_zone_max:,.0f}",
                'Cắt lỗ': stop_loss,
                'Mục tiêu': target,
                'Volume': int(latest['volume']),
                'Vol/MA20': round(latest['volume'] / prev_vol_ma20, 2),
                'Phien': datetime.fromtimestamp(int(latest['time'])).strftime('%Y-%m-%d')
            }
    except Exception:
        pass
    return None

def run_screener_job():
    print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Bắt đầu tiến hành quét toàn sàn HOSE...")
    symbols = get_hose_watchlist()
    results = []
    
    for symbol in symbols:
        res = screen_symbol(symbol)
        if res:
            results.append(res)
            
    if results:
        print(f"Tìm thấy {len(results)} mã đạt chuẩn!")
        msg = "🚨 *PHÁT HIỆN CỔ PHIẾU BREAKOUT (HOSE)* 🚨\n\n"
        for r in results:
            msg += f"• *Mã:* `{r['Ma']}`\n"
            msg += f"  - Giá Breakout: `{r['Gia (VND)']:,.0f} VND`\n"
            msg += f"  - Vùng mua T+1: `{r['Vùng mua T+1']}`\n"
            msg += f"  - Cắt lỗ (-7%): `{r['Cắt lỗ']:,.0f}`\n"
            msg += f"  - Chốt lời (+10%): `{r['Mục tiêu']:,.0f}`\n"
            msg += f"  - Đột biến Vol: `{r['Vol/MA20']}x` TB20\n\n"
        send_telegram_message(msg)
    else:
        print("Hôm nay không có cổ phiếu nào thỏa mãn điều kiện.")
        send_telegram_message("🤖 Hệ thống quét chứng khoán: Hôm nay không có mã nào đạt chuẩn Breakout.")

# === HỆ THỐNG LẮNG NGHE TIN NHẮN TỪ TELEGRAM ===
def listen_telegram_commands():
    offset = 0
    print("🤖 Bot Telegram đang ở trạng thái lắng nghe lệnh... (Hãy nhắn '/scan' vào bot để quét ngay)")
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=30"
            response = requests.get(url, timeout=35)
            data = response.json()
            
            if data.get("ok"):
                for result in data.get("result", []):
                    offset = result["update_id"] + 1
                    message = result.get("message", {})
                    text = message.get("text", "").strip()
                    
                    # Nếu bạn nhắn chữ /scan trên Telegram
                    if text.lower() == "/scan":
                        send_telegram_message("⏳ Nhận được yêu cầu! Hệ thống đang quét toàn sàn HOSE theo lệnh của bạn, vui lòng đợi...")
                        run_screener_job()
                        
        except Exception as e:
            time.sleep(5)
        time.sleep(2)

def run_scheduler():
    # Lịch tự động chạy lúc 15:15 các ngày từ Thứ Hai đến Thứ Sáu
    schedule.every().monday_to_friday().at("15:15").do(run_screener_job)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    print("=== HỆ THỐNG QUÉT CHỨNG KHOÁN TỰ ĐỘNG & TELEGRAM BOT ĐÃ KHỞI ĐỘNG ===")
    
    # Chạy lịch trình tự động bằng luồng phụ (Background Thread)
    t_schedule = threading.Thread(target=run_scheduler, daemon=True)
    t_schedule.start()
    
    # Chạy bộ lắng nghe tin nhắn Telegram ở luồng chính
    listen_telegram_commands()
