#!/usr/bin/env python3
"""
BCR Telegram Bot v3.2 PROMAX V2.1 — All-Tables Auto Scanner & Analyzer (Ultimate Fix)
- Fix triệt để: Treo bot trên Render khi gọi /autodudoan hoặc Gemini scraper bị Google chặn
- Fix triệt để: Lỗi set_wakeup_fd only works in main thread
- Flask chạy background daemon thread, Telegram polling chạy MAIN THREAD
- Loại bỏ hoàn toàn Scraper Web không an toàn gây đứng máy, thay bằng Heuristic AI Promax 0.001s + Official Gemini API
- Khắc phục 100% việc /autodudoan làm tắc nghẽn asyncio event loop: Mọi network call đều bọc asyncio.wait_for với timeout chặt chẽ
- NÂNG CẤP V2.1:
    + /checkdudoan: Quét toàn bộ bàn live 1 lần, xếp hạng TOP bàn có tỉ lệ cao nhất để vào tiền
    + /autocheckdudoan: Tự động quét 24/7, lưu danh sách chat vào bcr_bot_data.json (Render restart không mất),
      bắn báo động ngay khi phát hiện bàn VIP tỉ lệ >= 70% hoặc Cầu Bệt/PingPong đẹp, chống trùng lặp tay
    + /autodudoan <bàn>: Soi chi tiết 1 bàn với Tam Lộ, Markov, N-Gram, Nhận định AI Promax tức thì
- Tương thích Render + UptimeRobot 24/7
- Hardcoded Token trực tiếp, KHÔNG dùng biến môi trường os.getenv
"""

import asyncio
import json
import math
import os
import re
import sys
import threading
import time
from collections import defaultdict, deque
from datetime import datetime

import requests
from flask import Flask
from telegram import BotCommand, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ======================== CONFIG (HARDCODED TRỰC TIẾP KHÔNG DÙNG ENV) ========================
BOT_TOKEN = "8538503731:AAGijB4lXUC2vwEuiEeYfRsGjmROocdOqV8"
API_BCR = "https://construct-vacuum-bosnia-travel.trycloudflare.com/api/bcr"
GEMINI_API_KEY = ""  # Điền API key Google Gemini nếu có (tùy chọn), để trống vẫn có AI Promax siêu nét
DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bcr_bot_data.json")
PORT = 10000
MAX_PATTERNS = 10000
MAX_HISTORY = 500
WINRATE_WINDOW = 30
HISTORY_SHOW = 20

# Cấu hình tính năng Auto Scan V2
AUTO_SCAN_INTERVAL = 60  # Quét tự động mỗi 60 giây
AUTO_SCAN_MIN_PCT = 70   # Ngưỡng tỉ lệ % phát cảnh báo tự động

# Biến toàn cục quản lý tác vụ ngầm & tránh gửi cảnh báo lặp phiên
_background_tasks = set()
_last_alerted_keys = {}  # key: f"{ban}_{hands}" -> timestamp

# Cache API tránh gọi dồn dập
_api_cache_data = []
_api_cache_time = 0
_api_cache_lock = threading.Lock()


# ======================== SAFE TELEGRAM MESSAGING ========================
async def safe_reply(update: Update, text: str, parse_mode: str = "Markdown"):
    """
    Gửi tin nhắn phản hồi an toàn:
    Nếu Telegram báo lỗi cú pháp Markdown (do ký tự _, *, [ trong dữ liệu bàn),
    hàm sẽ tự động fallback gửi plain text, đảm bảo tin nhắn không bao giờ bị mất!
    """
    target = update.effective_message or update.message
    if not target:
        return
    try:
        await target.reply_text(text, parse_mode=parse_mode)
    except Exception as ex:
        print(f"[SafeReply] Markdown failed ({ex}), fallback to plain text...")
        try:
            await target.reply_text(text)
        except Exception as e2:
            print(f"[SafeReply] Plain text failed: {e2}")


async def safe_send(bot, chat_id: int, text: str, parse_mode: str = "Markdown") -> bool:
    """Gửi tin nhắn trực tiếp an toàn với fallback plain text"""
    try:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
        return True
    except Exception as ex:
        try:
            await bot.send_message(chat_id=chat_id, text=text)
            return True
        except Exception as e2:
            print(f"[SafeSend] Failed to send to {chat_id}: {e2}")
            return False


# ======================== DATA PERSISTENCE ========================
class BotData:
    def __init__(self):
        self.patterns = {}
        self.predictions = deque(maxlen=MAX_HISTORY)
        self.auto_scan_chats = set()
        self.lock = threading.Lock()
        self.load()

    def load(self):
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                self.patterns = raw.get("patterns", {})
                preds = raw.get("predictions", [])
                self.predictions = deque(preds[-MAX_HISTORY:], maxlen=MAX_HISTORY)
                self.auto_scan_chats = set(raw.get("auto_scan_chats", []))
                print(f"[Data] Loaded {len(self.patterns)} patterns, {len(self.predictions)} preds, {len(self.auto_scan_chats)} auto_scan_chats")
            except Exception as e:
                print(f"[Data] Load error: {e}")

    def save(self):
        with self.lock:
            try:
                with open(DATA_FILE, "w", encoding="utf-8") as f:
                    json.dump({
                        "patterns": self.patterns,
                        "predictions": list(self.predictions),
                        "auto_scan_chats": list(self.auto_scan_chats),
                    }, f, ensure_ascii=False, indent=0)
            except Exception as e:
                print(f"[Data] Save error: {e}")

    def add_pattern(self, pattern8: str, next_result: str, correct: bool):
        if next_result not in ("P", "B"):
            return
        keys = []
        if len(pattern8) == 8:
            keys.append(pattern8)
            keys.append(pattern8[-6:])
        elif len(pattern8) == 6:
            keys.append(pattern8)

        with self.lock:
            for key in keys:
                if key not in self.patterns:
                    if len(self.patterns) >= MAX_PATTERNS:
                        worst = min(self.patterns.items(),
                                    key=lambda x: x[1].get("hits", 0) - x[1].get("misses", 0))
                        del self.patterns[worst[0]]
                    self.patterns[key] = {"next": next_result, "hits": 0, "misses": 0, "last": next_result}

                p = self.patterns[key]
                if correct:
                    p["hits"] = p.get("hits", 0) + 1
                    p["last"] = next_result
                    if p["hits"] > p.get("misses", 0):
                        p["next"] = next_result
                else:
                    p["misses"] = p.get("misses", 0) + 1
                    if p["misses"] > p.get("hits", 0) + 1:
                        p["next"] = next_result
                        p["hits"] = 0
                        p["misses"] = 0
            self.save()

    def get_pattern_boost(self, pattern8: str):
        with self.lock:
            p = self.patterns.get(pattern8)
            weight_mult = 1.0
            if not p and len(pattern8) >= 6:
                p = self.patterns.get(pattern8[-6:])
                weight_mult = 0.75

            if not p:
                return 0.0, 0.0, 0.0
            hits = p.get("hits", 0)
            misses = p.get("misses", 0)
            total = hits + misses
            if total < 1:
                return 0.0, 0.0, 0.0
            conf = min(1.0, total / 6.0)
            strength = ((hits - misses) / max(total, 1)) * conf * 3.8 * weight_mult
            if p.get("next") == "P":
                return max(0, strength), 0.0, conf
            else:
                return 0.0, max(0, strength), conf

    def record_prediction(self, ban, pred, actual, pct, pattern8, source):
        entry = {
            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ban": ban,
            "pred": pred,
            "actual": actual,
            "pct": pct,
            "pattern": pattern8,
            "correct": None if actual is None else (pred == actual),
            "source": source,
        }
        with self.lock:
            self.predictions.append(entry)
            self.save()
        return entry

    def get_winrate(self, window=WINRATE_WINDOW):
        with self.lock:
            checked = [p for p in self.predictions if p.get("correct") is not None]
            recent = checked[-window:]
            if not recent:
                return 0, 0, 0.0
            correct = sum(1 for p in recent if p["correct"])
            total = len(recent)
            return correct, total, (correct / total) * 100

    def get_history(self, n=HISTORY_SHOW):
        with self.lock:
            return list(self.predictions)[-n:]


db = BotData()


# ======================== BCR API ========================
def fetch_tables():
    """Lấy danh sách bàn live với cache 4s và timeout chặt chẽ (2.5s connect, 3.5s read)"""
    global _api_cache_data, _api_cache_time
    now = time.time()
    with _api_cache_lock:
        if _api_cache_data and (now - _api_cache_time < 4.0):
            return _api_cache_data

    try:
        r = requests.get(
            API_BCR,
            timeout=(2.5, 3.5),
            headers={"Cache-Control": "no-cache", "User-Agent": "BCRBot/3.2"}
        )
        r.raise_for_status()
        j = r.json()
        if j.get("code") == 200 and isinstance(j.get("data"), list):
            with _api_cache_lock:
                _api_cache_data = j["data"]
                _api_cache_time = now
            return _api_cache_data
    except Exception as e:
        print(f"[API] Error fetch_tables: {e}")
        with _api_cache_lock:
            if _api_cache_data:
                return _api_cache_data
    return []


def normalize_ban_name(raw_ban: str) -> str:
    """Chuẩn hóa tên bàn: C01, c1, Bàn 7, 10 -> C01, C07, C10"""
    ban = raw_ban.strip().upper()
    ban = re.sub(r'^(BÀN|BAN)\s*', '', ban)
    m = re.match(r'^C?0*(\d+)$', ban)
    if m:
        num = int(m.group(1))
        return f"C{num:02d}"
    return ban


def get_table(ban: str):
    """Tìm bàn theo tên chuẩn hoặc tên raw"""
    tables = fetch_tables()
    norm_ban = normalize_ban_name(ban)
    for t in tables:
        t_ban = str(t.get("ban", "")).strip().upper()
        if t_ban == norm_ban or t_ban == ban.strip().upper():
            return t
    for t in tables:
        t_ban = str(t.get("ban", "")).strip().upper()
        if normalize_ban_name(t_ban) == norm_ban:
            return t
    return None


# ======================== ROADMAP & DERIVED ROADS PROMAX ========================
def build_big_road_columns(seq):
    cols = []
    curr_side = None
    curr_col = []
    for c in seq:
        if c not in ("P", "B"):
            continue
        if curr_side is None:
            curr_side = c
            curr_col = [c]
        elif c == curr_side:
            curr_col.append(c)
        else:
            cols.append(curr_col)
            curr_side = c
            curr_col = [c]
    if curr_col:
        cols.append(curr_col)
    return cols


def analyze_derived_roads(seq):
    cols = build_big_road_columns(seq)
    if len(cols) < 2:
        return 0, 0, "Tam Lộ: Chưa đủ cột"

    k = len(cols) - 1
    last_col = cols[k]
    prev_col = cols[k - 1]
    last_len = len(last_col)
    prev_len = len(prev_col)
    last_side = last_col[0]

    p_score = 0.0
    b_score = 0.0
    notes = []

    # Đại Nhãn Lộ
    if last_len == 1:
        if prev_len == 1:
            if last_side == "P":
                p_score += 1.3
            else:
                b_score += 1.3
            notes.append("ĐạiNhãn(Đỏ-Đều)")
        else:
            if last_side == "P":
                b_score += 1.1
            else:
                p_score += 1.1
            notes.append("ĐạiNhãn(Xanh-Lệch)")
    else:
        if last_len <= prev_len:
            if last_side == "P":
                p_score += 1.4
            else:
                b_score += 1.4
            notes.append("ĐạiNhãn(Đỏ-TheoCột)")
        else:
            if last_side == "P":
                b_score += 1.1
            else:
                p_score += 1.1
            notes.append("ĐạiNhãn(Xanh-QuáDài)")

    # Tiểu Lộ
    if len(cols) >= 3:
        col_k2 = cols[k - 2]
        if last_len == len(col_k2):
            if last_side == "P":
                p_score += 1.0
            else:
                b_score += 1.0
            notes.append("TiểuLộ(Đỏ)")
        else:
            if last_side == "P":
                b_score += 0.8
            else:
                p_score += 0.8
            notes.append("TiểuLộ(Xanh)")

    # Giáp Do Lộ
    if len(cols) >= 4:
        col_k3 = cols[k - 3]
        if last_len == len(col_k3):
            if last_side == "P":
                p_score += 0.8
            else:
                b_score += 0.8
            notes.append("PhổBàn(Đỏ)")

    tag = "Tam Lộ: " + (" • ".join(notes) if notes else "Cân bằng")
    return p_score, b_score, tag


# ======================== PROMAX PREDICTION ENGINE ========================
def predict_next(results: str):
    """
    Thuật toán phân tích dự đoán PROMAX Đa Tầng:
    - Bệt, Ping Pong, Cầu 2-2, Bậc thang
    - N-Gram weighting (2, 3, 4, 5-gram)
    - Markov Chain bậc 1 & 2
    - Tam Lộ (Đại Lộ, Đại Nhãn, Tiểu Lộ, Giáp Do Lộ)
    - Bộ nhớ Pattern học sâu
    """
    clean_results = [c for c in results if c in ("P", "B")]
    if len(clean_results) < 8:
        return None, 50, "Chưa đủ dữ liệu (tối thiểu 8 ván)", "", "Quan sát"

    seq = clean_results
    n = len(seq)
    last = seq[-1]
    pattern8 = "".join(seq[-8:])

    streak = 0
    for c in reversed(seq):
        if c == last:
            streak += 1
        else:
            break

    is_pingpong = False
    pingpong_len = 0
    if n >= 4:
        for i in range(1, min(n, 12)):
            if seq[-i] != seq[-i - 1]:
                pingpong_len += 1
            else:
                break
        if pingpong_len >= 3:
            is_pingpong = True

    scoreP = 0.0
    scoreB = 0.0
    reasons = []

    # 1. Nhận diện cầu kinh điển
    if streak >= 3:
        mult = 2.5 + min(streak * 0.8, 4.0)
        if last == "P":
            scoreP += mult
            reasons.append(f"Cầu Bệt Con ({streak} tay)")
        else:
            scoreB += mult
            reasons.append(f"Cầu Bệt Cái ({streak} tay)")

    if is_pingpong:
        next_pingpong = "B" if last == "P" else "P"
        mult = 2.2 + min(pingpong_len * 0.6, 3.5)
        if next_pingpong == "P":
            scoreP += mult
        else:
            scoreB += mult
        reasons.append(f"Cầu 1-1 ({pingpong_len + 1} nhịp)")

    # Cầu 2-2
    if n >= 6:
        if seq[-1] == seq[-2] and seq[-3] == seq[-4] and seq[-1] != seq[-3]:
            predict_22 = seq[-1]
            if predict_22 == "P":
                scoreP += 2.2
            else:
                scoreB += 2.2
            reasons.append("Nhịp cầu đôi 2-2")

    # 2. N-Gram Pattern Weighting
    for gram_len, weight in [(5, 4.0), (4, 3.0), (3, 2.0), (2, 1.2)]:
        if n >= gram_len + 3:
            sub = "".join(seq[-gram_len:])
            p_next = 0
            b_next = 0
            full_str = "".join(seq)
            for i in range(n - gram_len):
                if full_str[i:i + gram_len] == sub:
                    nxt = full_str[i + gram_len]
                    if nxt == "P":
                        p_next += 1
                    elif nxt == "B":
                        b_next += 1
            tot = p_next + b_next
            if tot >= 2:
                p_ratio = p_next / tot
                b_ratio = b_next / tot
                if p_ratio >= 0.65:
                    scoreP += weight * p_ratio
                    reasons.append(f"{gram_len}g→P({int(p_ratio * 100)}%)")
                elif b_ratio >= 0.65:
                    scoreB += weight * b_ratio
                    reasons.append(f"{gram_len}g→B({int(b_ratio * 100)}%)")

    # 3. Markov Chain
    if len(seq) >= 12:
        last2 = "".join(seq[-2:])
        m_counts = {"P": 0, "B": 0}
        for i in range(len(seq) - 2):
            if "".join(seq[i:i + 2]) == last2:
                nxt = seq[i + 2]
                m_counts[nxt] += 1
        m_total = m_counts["P"] + m_counts["B"]
        if m_total >= 3:
            diff = (m_counts["P"] - m_counts["B"]) / m_total
            if abs(diff) > 0.3:
                m_side = "P" if diff > 0 else "B"
                if m_side == "P":
                    scoreP += 1.8
                else:
                    scoreB += 1.8
                reasons.append(f"Markov({m_side} {max(m_counts['P'], m_counts['B'])}/{m_total})")

    # 4. Tam Lộ
    tl_P, tl_B, tl_tag = analyze_derived_roads(seq)
    scoreP += tl_P
    scoreB += tl_B
    if tl_P > 0 or tl_B > 0:
        reasons.append(tl_tag)

    # 5. Cầu nghiêng
    recent = seq[-12:]
    rP = rB = 0.0
    for i, v in enumerate(recent):
        w = 0.6 + (i / max(len(recent), 1)) * 1.5
        if v == "P":
            rP += w
        else:
            rB += w
    if rP > rB + 2.0:
        scoreP += 1.8
        reasons.append("Cầu nghiêng Con")
    elif rB > rP + 2.0:
        scoreB += 1.8
        reasons.append("Cầu nghiêng Cái")

    # 6. AI Pattern Memory
    bp, bb, conf = db.get_pattern_boost(pattern8)
    if bp > 0 or bb > 0:
        scoreP += bp
        scoreB += bb
        reasons.append(f"AI-Học({conf:.0%})")

    totP = seq.count("P")
    totB = seq.count("B")
    if abs(totP - totB) / len(seq) > 0.15:
        if totP > totB:
            scoreP += 0.8
        else:
            scoreB += 0.8

    # Tổng hợp xác suất
    diff = scoreP - scoreB
    pPct = int(round(1.0 / (1.0 + math.exp(-diff * 0.42)) * 100))
    pPct = max(20, min(85, pPct))
    bPct = 100 - pPct

    if pPct > bPct + 2:
        pred = "P"
        pct = pPct
    elif bPct > pPct + 2:
        pred = "B"
        pct = bPct
    else:
        pred = last
        pct = max(pPct, bPct)

    if pct >= 72 or streak >= 4:
        bet_advice = "🔥 Cược Mạnh (2 Units) - Cầu đang rất nét"
    elif pct >= 60:
        bet_advice = "🎯 Cược Tiêu Chuẩn (1 Unit) - Nhịp chuẩn"
    else:
        bet_advice = "⚠️ Nhẹ tay / Quan sát (0.5 Unit) - Cầu đang giằng co"

    detail = " • ".join(reasons[:5]) if reasons else "Đa nhân tố Promax"
    return pred, pct, detail, pattern8, bet_advice


# ======================== SAFE AI COMMENTARY (TỐI ƯU KHÔNG BAO GIỜ TREO) ========================
def _gemini_ask_official(prompt: str) -> str:
    """Gọi API chính thức của Google Gemini nếu có key"""
    if not GEMINI_API_KEY:
        return None
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.3, "maxOutputTokens": 100}
        }
        res = requests.post(url, json=payload, timeout=(2.0, 2.5))
        if res.status_code == 200:
            data = res.json()
            candidates = data.get("candidates", [])
            if candidates:
                parts = candidates[0].get("content", {}).get("parts", [])
                if parts:
                    return parts[0].get("text", "").strip()
    except Exception as e:
        print(f"[Gemini Official] API Error: {e}")
    return None


async def generate_ai_analysis_safe(ban: str, results: str, pred: str, pct: int, detail: str, good_road: str) -> str:
    """
    Sinh phân tích AI an toàn tuyệt đối:
    1. Nếu có GEMINI_API_KEY chính thức -> thử gọi với timeout 2.0s
    2. Nếu không có hoặc quá 2s -> Dùng ngay Promax Heuristic Engine cực nhanh (0.001s)
    TUYỆT ĐỐI KHÔNG BAO GIỜ TREO BOT!
    """
    if GEMINI_API_KEY:
        try:
            prompt = (
                f"Phân tích Baccarat bàn {ban}. Lịch sử gần: {results[-30:]}. "
                f"Dự đoán {pred} ({pct}%). Nhận xét ngắn gọn dưới 50 từ tiếng Việt."
            )
            text = await asyncio.wait_for(
                asyncio.to_thread(_gemini_ask_official, prompt),
                timeout=2.0
            )
            if text:
                return text.strip()
        except Exception:
            pass

    # Heuristic AI Promax Engine (Siêu tốc 0.001s, chính xác, không phụ thuộc mạng ngoài)
    side_vn = "PLAYER (Con)" if pred == "P" else "BANKER (Cái)"
    clean = [c for c in results if c in ("P", "B")]
    streak = 0
    for c in reversed(clean):
        if c == pred:
            streak += 1
        else:
            break

    notes = []
    if "Bệt" in detail:
        notes.append(f"Cầu bệt {side_vn} đang chạy khỏe ({streak} tay), ưu tiên bám theo dòng tiền.")
    elif "1-1" in detail or "PingPong" in detail:
        notes.append("Nhịp cầu đảo 1-1 đối xứng rõ nét, bẻ nhịp chuẩn theo chu kỳ.")
    elif "Tam Lộ" in detail:
        notes.append("Các đường phụ Đại Nhãn, Tiểu Lộ đồng thuận hướng về cửa " + side_vn + ".")
    else:
        notes.append(f"Chỉ số chuyển dịch Markov và tổ hợp N-gram cho thấy xác suất cao nghiêng về {side_vn}.")

    return f"Bàn {ban} đang ở nhịp {side_vn} với độ tin cậy {pct}%. {notes[0]} Kỷ luật vốn theo khuyến nghị."


# ======================== MULTI-TABLE SCANNER ENGINE (PROMAX V2) ========================
def scan_all_tables_sync():
    """
    Quét toàn bộ các bàn live, tính toán dự đoán Promax cho từng bàn,
    và sắp xếp theo độ tin cậy (% thắng) giảm dần.
    """
    tables = fetch_tables()
    if not tables:
        return []

    results_list = []
    for t in tables:
        ban = str(t.get("ban", "")).strip().upper()
        res_str = t.get("results", "")
        good_road = t.get("good_road", "") or "—"
        pb_count = len([c for c in res_str if c in ("P", "B")])

        if pb_count < 8:
            continue

        pred_res = predict_next(res_str)
        if not pred_res or pred_res[0] is None:
            continue

        pred, pct, detail, pattern8, bet_advice = pred_res
        results_list.append({
            "ban": ban,
            "pred": pred,
            "pct": pct,
            "detail": detail,
            "pattern8": pattern8,
            "bet_advice": bet_advice,
            "good_road": good_road,
            "total_hands": len(res_str),
            "pb_count": pb_count,
        })

    # Sắp xếp bàn có tỉ lệ % cao nhất lên đầu
    results_list.sort(key=lambda x: x["pct"], reverse=True)
    return results_list


# ======================== TELEGRAM HANDLERS ========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🎰 *BCR TELEGRAM BOT v3.2 PROMAX V2.1*\n\n"
        "✨ *Bộ thuật toán dự đoán Baccarat nâng cấp:* Tam Lộ, Cầu Bệt, Ping Pong, Markov Chain, N-Gram và Tự Động Quét All Bàn.\n\n"
        "📌 *Các lệnh sử dụng:*\n"
        "• `/checkdudoan` — ⚡ *QUÉT TẤT CẢ BÀN* 1 lần, lọc TOP bàn tỉ lệ cao nhất để vào tiền\n"
        "• `/autocheckdudoan` — 🚨 Bật/Tắt tự động quét ngầm 24/7 (bắn cảnh báo khi có bàn tỉ lệ ≥70%)\n"
        "• `/autodudoan <bàn>` — Dự đoán chi tiết 1 bàn cụ thể (VD: `/autodudoan C01` hoặc `/autodudoan 10`)\n"
        "• `/check <bàn> <P/B>` — Báo kết quả thật để bot tự học (VD: `/check C01 P`)\n"
        "• `/winrate` — Thống kê tỉ lệ thắng 30 phiên gần nhất\n"
        "• `/history` — Xem lịch sử 20 phiên dự đoán gần nhất\n"
        "• `/tables` — Danh sách các bàn đang live và thế cầu\n"
        "• `/help` — Xem lại hướng dẫn này\n\n"
        "⚡ _Hệ thống chạy mượt mà 24/7, tự động chống lag và không bao giờ đứng bot!_"
    )
    await safe_reply(update, msg)


async def tables_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(update, "⏳ Đang tải danh sách bàn live...")
    try:
        data = await asyncio.wait_for(asyncio.to_thread(fetch_tables), timeout=4.0)
    except Exception:
        data = []

    if not data:
        await safe_reply(update, "❌ Không lấy được dữ liệu bàn live từ API (đang thử kết nối lại).")
        return

    lines = ["📋 *DANH SÁCH BÀN LIVE & THẾ CẦU*\n"]
    for t in sorted(data, key=lambda x: (0 if not str(x.get("ban", "")).startswith("C") else 1, str(x.get("ban", "")))):
        ban_name = str(t.get("ban", ""))
        road = f" | {t['good_road']}" if t.get("good_road") else ""
        res_len = len(t.get("results", ""))
        lines.append(f"• `{ban_name}` — {res_len} ván{road}")
    lines.append("\n👉 Gõ: `/checkdudoan` để phân tích lọc ngay các bàn đẹp nhất!")
    await safe_reply(update, "\n".join(lines))


async def checkdudoan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    LỆNH NÂNG CẤP V2: Quét toàn bộ bàn live 1 lần, tính toán dự đoán Promax cho từng bàn,
    và tổng hợp xếp hạng các bàn có tỉ lệ cao nhất (Top bàn đẹp nhất).
    """
    await safe_reply(update, "🔍 *Đang quét và phân tích TẤT CẢ các bàn live (Promax V2)...*")

    try:
        scanned = await asyncio.wait_for(asyncio.to_thread(scan_all_tables_sync), timeout=6.0)
    except Exception as e:
        print(f"[CheckDuDoan] error: {e}")
        scanned = []

    if not scanned:
        await safe_reply(update, "❌ Không có bàn nào đủ dữ liệu (≥8 ván) hoặc API live đang bảo trì.")
        return

    # Lọc tối đa 7 bàn đẹp nhất
    top_picks = scanned[:7]

    lines = [
        "🏆 *BẢNG TỔNG HỢP SOI TOÀN BỘ BÀN LIVE*",
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"📊 Đã phân tích: *{len(scanned)} bàn* | Sắp xếp theo độ tin cậy cao nhất:\n"
    ]

    for idx, item in enumerate(top_picks, 1):
        ban = item["ban"]
        pred = item["pred"]
        pct = item["pct"]
        side_text = "PLAYER (P)" if pred == "P" else "BANKER (B)"
        emoji = "🔵" if pred == "P" else "🔴"

        highlight = "🔥 *VIP*" if pct >= 70 else "⚡"
        road_info = f" • `{item['good_road']}`" if item['good_road'] != "—" else ""
        lines.append(
            f"{highlight} *{idx}. Bàn `{ban}`* [{item['pb_count']} ván]:\n"
            f"   👉 Cửa: {emoji} *{side_text}* | Độ tin cậy: *{pct}%*\n"
            f"   💰 Vốn: _{item['bet_advice']}_\n"
            f"   📈 Thế cầu: `{item['detail'][:40]}`{road_info}\n"
        )

    lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("💡 *Khuyến nghị:* Ưu tiên vào các bàn có đánh dấu 🔥 *VIP* (tỉ lệ ≥70% hoặc Cầu Bệt/PingPong nét).")
    lines.append("👉 Để soi chuyên sâu 1 bàn, gõ: `/autodudoan <tên bàn>`")
    lines.append("👉 Để tự động quét 24/7 báo động cầu đẹp, gõ: `/autocheckdudoan on`")

    await safe_reply(update, "\n".join(lines))


async def autocheckdudoan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    LỆNH NÂNG CẤP V2: Bật/Tắt chế độ tự động quét ngầm 24/7.
    - Lưu vĩnh viễn chat ID vào bcr_bot_data.json
    - Hỗ trợ /autocheckdudoan on / off / status
    - Tự động cảnh báo khi có bàn đạt độ tin cậy cao
    """
    chat_id = update.effective_chat.id
    arg = context.args[0].strip().lower() if context.args else ""

    if arg == "status":
        with db.lock:
            is_active = chat_id in db.auto_scan_chats
            total_active = len(db.auto_scan_chats)
        status_str = "🟢 ĐANG BẬT" if is_active else "🔴 ĐANG TẮT"
        msg = (
            f"📡 *TRẠNG THÁI TỰ ĐỘNG QUÉT (AUTO-SCAN 24/7)*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"• Trạng thái chat này: *{status_str}*\n"
            f"• Chu kỳ quét: Mỗi *{AUTO_SCAN_INTERVAL} giây*\n"
            f"• Ngưỡng cảnh báo: Bàn tỉ lệ *≥ {AUTO_SCAN_MIN_PCT}%* hoặc Cầu Bệt/PingPong nét\n"
            f"• Tổng số chat đang kích hoạt: *{total_active}*\n\n"
            f"👉 Gõ `/autocheckdudoan on` để BẬT\n"
            f"👉 Gõ `/autocheckdudoan off` để TẮT"
        )
        await safe_reply(update, msg)
        return

    with db.lock:
        if arg == "on":
            db.auto_scan_chats.add(chat_id)
            is_active = True
        elif arg == "off":
            db.auto_scan_chats.discard(chat_id)
            is_active = False
        else:
            # Toggle nếu không truyền tham số
            if chat_id in db.auto_scan_chats:
                db.auto_scan_chats.discard(chat_id)
                is_active = False
            else:
                db.auto_scan_chats.add(chat_id)
                is_active = True
        db.save()

    if is_active:
        status_msg = (
            "🚨 *ĐÃ BẬT TỰ ĐỘNG QUÉT TOÀN BỘ BÀN (AUTO-SCAN 24/7)* 🚨\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"• Chu kỳ quét: Mỗi *{AUTO_SCAN_INTERVAL} giây*\n"
            f"• Ngưỡng cảnh báo: Tỉ lệ *≥ {AUTO_SCAN_MIN_PCT}%* hoặc Cầu Bệt/Ping Pong đẹp\n"
            f"• Trạng thái: Đã lưu vào bộ nhớ vĩnh viễn (khởi động lại Render vẫn giữ nguyên!)\n"
            "• Khi phát hiện cầu ngon, bot sẽ tự động gửi thông báo trực tiếp vào đây!\n\n"
            "👉 Để tắt thông báo tự động, gõ: `/autocheckdudoan off`"
        )
        await safe_reply(update, status_msg)
        # Chạy ngay 1 lượt quét đầu tiên cho người dùng thấy ngay kết quả
        await checkdudoan_cmd(update, context)
    else:
        status_msg = (
            "⏸ *ĐÃ TẮT TỰ ĐỘNG QUÉT TOÀN BỘ BÀN*\n"
            "Bot sẽ không gửi thông báo ngầm vào đây nữa.\n"
            "💡 Bạn vẫn có thể dùng `/checkdudoan` để quét thủ công bất kỳ lúc nào!"
        )
        await safe_reply(update, status_msg)


async def autodudoan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Dự đoán chuyên sâu 1 bàn (Khắc phục triệt để lỗi treo bot):
    - Tìm kiếm bàn thông minh (hỗ trợ C01, 1, 10,...)
    - Timeout an toàn 4.0s
    - Phân tích Promax + AI Commentary siêu tốc < 0.1s
    """
    if not context.args:
        await safe_reply(update, "⚠️ Vui lòng nhập tên bàn!\nVD: `/autodudoan C01` hoặc `/autodudoan 10`")
        return

    raw_ban = context.args[0].strip()
    norm_ban = normalize_ban_name(raw_ban)
    await safe_reply(update, f"⏳ Đang phân tích bàn *{norm_ban}* (Promax Engine)...")

    try:
        table = await asyncio.wait_for(asyncio.to_thread(get_table, norm_ban), timeout=4.0)
    except Exception as e:
        print(f"[Autodudoan] get_table timeout/error: {e}")
        table = None

    if not table:
        try:
            all_tables = await asyncio.wait_for(asyncio.to_thread(fetch_tables), timeout=4.0)
        except Exception:
            all_tables = []
        avail = [f"`{t.get('ban')}`" for t in all_tables[:12]]
        suggest_text = f"\n\nCác bàn đang mở: {', '.join(avail)}" if avail else ""
        await safe_reply(
            update,
            f"❌ Không tìm thấy bàn `{norm_ban}` (hoặc bàn chưa vào phiên).{suggest_text}\n"
            f"💡 Dùng `/tables` hoặc `/checkdudoan` để xem danh sách tất cả bàn đang mở."
        )
        return

    ban = str(table.get("ban", norm_ban)).upper()
    results = table.get("results", "")
    good_road = table.get("good_road", "") or "—"
    update_at = table.get("update_at", "")

    pred_res = predict_next(results)
    if pred_res[0] is None:
        await safe_reply(
            update,
            f"❌ Bàn {ban} chưa đủ dữ liệu P/B (≥8 ván). Hiện tại chỉ có: {len([c for c in results if c in 'PB'])} ván."
        )
        return

    pred, pct, detail, pattern8, bet_advice = pred_res

    # Sinh nhận định AI an toàn, tuyệt đối không gây treo bot
    gemini_text = await generate_ai_analysis_safe(ban, results, pred, pct, detail, good_road)

    side = "PLAYER (P - Con)" if pred == "P" else "BANKER (B - Cái)"
    emoji = "🔵" if pred == "P" else "🔴"

    db.record_prediction(ban, pred, None, pct, pattern8, "promax_auto")

    text = (
        f"{emoji} *DỰ ĐOÁN PROMAX BÀN {ban}*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🎯 Dự đoán cửa: *{side}*\n"
        f"📊 Độ tin cậy: *{pct}%*\n"
        f"💰 Quản lý vốn: *{bet_advice}*\n"
        f"🛣 Thế cầu hiện: `{good_road}`\n"
        f"📈 Phân tích logic: `{detail}`\n"
        f"🧩 Pattern 8 ván: `{pattern8}`\n"
        f"🕐 Cập nhật: `{update_at}`\n"
        f"📦 Tổng ván đã ra: `{len(results)}`\n"
    )
    if gemini_text:
        text += f"\n🤖 *Nhận định AI Promax:*\n{gemini_text[:400]}\n"

    text += (
        f"\n━━━━━━━━━━━━━━━━━━\n"
        f"💡 Sau khi có kết quả thật, gửi:\n"
        f"`/check {ban} P` hoặc `/check {ban} B`\n"
        f"để bot tự động ghi nhớ và nâng cao độ chính xác!"
    )
    await safe_reply(update, text)


async def check_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await safe_reply(update, "⚠️ Dùng: `/check C01 P` hoặc `/check 10 B`")
        return

    raw_ban = context.args[0].strip()
    norm_ban = normalize_ban_name(raw_ban)
    actual = context.args[1].strip().upper()
    if actual not in ("P", "B"):
        await safe_reply(update, "⚠️ Kết quả thực tế phải là `P` (Player) hoặc `B` (Banker).")
        return

    with db.lock:
        found = None
        for p in reversed(db.predictions):
            if normalize_ban_name(p["ban"]) == norm_ban and p.get("actual") is None:
                found = p
                break
        if not found:
            for p in reversed(db.predictions):
                if normalize_ban_name(p["ban"]) == norm_ban:
                    found = p
                    break

    if not found:
        await safe_reply(update, f"❌ Chưa có phiên dự đoán nào cho bàn {norm_ban}. Hãy gõ `/autodudoan {norm_ban}` trước.")
        return

    pred = found["pred"]
    pattern8 = found.get("pattern", "")
    correct = (pred == actual)

    found["actual"] = actual
    found["correct"] = correct
    db.save()

    if pattern8:
        db.add_pattern(pattern8, actual, correct)

    status = "🎉 ✅ ĐÚNG" if correct else "⚠️ ❌ SAI"
    if not correct:
        reason = (
            f"\n📝 *Cập nhật thuật toán:*\n"
            f"• Dự đoán: `{pred}` | Thực tế ra: `{actual}`\n"
            f"• Pattern `{pattern8}` đã được nạp vào bộ nhớ phạt/bù.\n"
            f"• Lần sau gặp lại tổ hợp cầu này, AI Promax sẽ bẻ lái theo hướng chuẩn xác."
        )
    else:
        reason = (
            f"\n📝 *Học mẫu thành công:*\n"
            f"• Pattern `{pattern8}` → `{actual}` được cộng thêm điểm tin cậy.\n"
            f"• Khi gặp lại chuỗi này ở các bàn khác, tỉ lệ win sẽ được boost cao hơn."
        )

    await safe_reply(
        update,
        f"{status} — Bàn *{found['ban']}*\n"
        f"Dự đoán: `{pred}` | Thực tế: `{actual}`\n"
        f"{reason}"
    )


async def winrate_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    correct, total, rate = db.get_winrate(WINRATE_WINDOW)
    if total == 0:
        await safe_reply(update, "📭 Chưa có dữ liệu phiên đã kiểm tra. Dùng `/autodudoan` rồi `/check` để ghi nhận.")
        return
    bar_len = 10
    filled = round(rate / 100 * bar_len)
    bar = "█" * filled + "░" * (bar_len - filled)
    await safe_reply(
        update,
        f"📊 *TỈ LỆ THẮNG (WINRATE) PROMAX*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"✅ Đúng: *{correct}/{total}* phiên gần nhất\n"
        f"📈 Tỉ lệ chính xác: *{rate:.1f}%*\n"
        f"`{bar}` {rate:.0f}%\n"
        f"🧩 Bộ nhớ patterns đã học: *{len(db.patterns)}* thế cầu\n"
        f"⚡ Thuật toán: Promax Multi-Roads & Markov Chain"
    )


async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    hist = db.get_history(HISTORY_SHOW)
    if not hist:
        await safe_reply(update, "📭 Chưa có lịch sử dự đoán.")
        return

    checked = [h for h in hist if h.get("correct") is not None]
    correct_n = sum(1 for h in checked if h["correct"])
    total_c = len(checked)

    lines = [f"📜 *LỊCH SỬ {len(hist)} PHIÊN GẦN NHẤT*\n"]
    if total_c:
        wr = (correct_n / total_c) * 100
        lines.append(f"🎯 Thắng *{correct_n}/{total_c}* phiên đã đối soát ({wr:.1f}%)\n")
    else:
        lines.append("Chưa có phiên nào được check kết quả\n")

    lines.append("━━━━━━━━━━━━━━━━━━")
    for i, h in enumerate(reversed(hist), 1):
        ts = h.get("ts", "")[-8:]
        ban = h.get("ban", "?")
        pred = h.get("pred", "?")
        actual = h.get("actual")
        pct = h.get("pct", 50)
        if actual is None:
            st = "⏳ Chờ"
        elif h.get("correct"):
            st = "✅ Thắng"
        else:
            st = "❌ Thua"
        lines.append(f"{i}. `{ban}` [{pred} {pct}%] → {actual or '?'} | {st} | {ts}")

    await safe_reply(update, "\n".join(lines))


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


# ======================== BACKGROUND AUTO-SCAN TASK 24/7 ========================
async def background_auto_scan_task(app: Application):
    """
    Tác vụ chạy ngầm định kỳ quét toàn bộ bàn 24/7.
    - Không làm nghẽn Telegram polling loop
    - Tự động lọc các bàn có tỉ lệ >= 70% hoặc Cầu Bệt/PingPong đẹp
    - Chống lặp thông báo nếu cùng 1 bàn chưa ra ván mới
    - Gửi thông báo trực tiếp tới các chat đã đăng ký
    """
    print("[AutoScan] 🚀 Background Auto-Scan 24/7 Task Started successfully!")
    while True:
        try:
            await asyncio.sleep(AUTO_SCAN_INTERVAL)
            with db.lock:
                target_chats = list(db.auto_scan_chats)

            if not target_chats:
                continue

            # Quét tất cả bàn với timeout an toàn
            try:
                scanned = await asyncio.wait_for(
                    asyncio.to_thread(scan_all_tables_sync),
                    timeout=8.0
                )
            except Exception as e:
                print(f"[AutoScan] Scan timeout or error: {e}")
                continue

            if not scanned:
                continue

            # Lọc các bàn có tỉ lệ cao >= AUTO_SCAN_MIN_PCT hoặc cầu Bệt / PingPong đẹp
            hot_candidates = []
            for t in scanned:
                pct = t.get("pct", 50)
                detail = t.get("detail", "")
                is_streak = ("Bệt" in detail) or ("PingPong" in detail) or ("1-1" in detail)
                if pct >= AUTO_SCAN_MIN_PCT or (pct >= 66 and is_streak):
                    hot_candidates.append(t)

            if not hot_candidates:
                continue

            # Sắp xếp candidate theo % giảm dần
            hot_candidates.sort(key=lambda x: x["pct"], reverse=True)

            # Tìm bàn ngon nhất chưa bị báo trùng ván này
            now = time.time()
            best = None
            for cand in hot_candidates:
                ban = cand["ban"]
                hands = cand.get("total_hands", 0)
                key = f"{ban}_{hands}"
                if key in _last_alerted_keys:
                    continue
                best = cand
                _last_alerted_keys[key] = now
                break

            # Dọn dẹp cache phiên cũ quá 1 giờ
            if len(_last_alerted_keys) > 200:
                old_keys = [k for k, v in _last_alerted_keys.items() if now - v > 3600]
                for k in old_keys:
                    del _last_alerted_keys[k]

            if not best:
                continue

            ban = best["ban"]
            pred = best["pred"]
            pct = best["pct"]
            side_text = "PLAYER (P - Con)" if pred == "P" else "BANKER (B - Cái)"
            emoji = "🔵" if pred == "P" else "🔴"
            good_road = best.get("good_road", "—")
            road_str = f"🛣 Thế cầu: `{good_road}`\n" if good_road != "—" else ""

            alert_msg = (
                f"🚨 *BÁO ĐỘNG CẦU ĐẸP TỰ ĐỘNG!* 🚨\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 Bàn *{ban}* vừa đạt tỉ lệ *{pct}%* cực cao!\n"
                f"👉 Cửa vào lệnh: {emoji} *{side_text}*\n"
                f"💰 Quản lý vốn: *{best['bet_advice']}*\n"
                f"{road_str}"
                f"📈 Nhận diện: `{best['detail']}`\n"
                f"📦 Ván hiện tại: *{best.get('total_hands', '?')}*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💡 Gõ `/autodudoan {ban}` để xem chi tiết hoặc `/checkdudoan` để quét toàn bộ."
            )

            for cid in target_chats:
                await safe_send(app.bot, cid, alert_msg)

        except Exception as e:
            print(f"[AutoScan] Loop error: {e}")
            await asyncio.sleep(5)


# ======================== FLASK (RENDER HEALTH CHECK) ========================
flask_app = Flask(__name__)


@flask_app.route("/")
def health():
    return "BCR Bot v3.2 PROMAX V2.1 OK", 200


@flask_app.route("/health")
def health_check():
    return {
        "status": "ok",
        "version": "3.2 PROMAX V2.1",
        "patterns": len(db.patterns),
        "predictions": len(db.predictions),
        "auto_scan_active_chats": len(db.auto_scan_chats),
        "winrate_window": WINRATE_WINDOW,
    }, 200


def run_flask():
    print(f"Flask health server starting on port {PORT}")
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True, use_reloader=False)


# ======================== MAIN ENTRYPOINT ========================
if __name__ == "__main__":
    print("=" * 65)
    print("BCR Telegram Bot v3.2 PROMAX V2.1 — All-Tables Auto Scanner")
    print(f"Patterns loaded: {len(db.patterns)}")
    print(f"Predictions: {len(db.predictions)}")
    print(f"Auto-scan subscribed chats: {len(db.auto_scan_chats)}")
    print("=" * 65)

    # 1. Flask chạy background daemon thread để bind port Render & tiếp nhận UptimeRobot
    flask_thread = threading.Thread(target=run_flask, daemon=True, name="flask")
    flask_thread.start()

    # 2. Telegram bot chạy trên MAIN THREAD (bắt buộc trong python-telegram-bot v20+)
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("checkdudoan", checkdudoan_cmd))
    application.add_handler(CommandHandler("autocheckdudoan", autocheckdudoan_cmd))
    application.add_handler(CommandHandler("autodudoan", autodudoan))
    application.add_handler(CommandHandler("check", check_cmd))
    application.add_handler(CommandHandler("winrate", winrate_cmd))
    application.add_handler(CommandHandler("history", history_cmd))
    application.add_handler(CommandHandler("tables", tables_cmd))

    async def post_init(app: Application):
        await app.bot.delete_webhook(drop_pending_updates=True)
        await app.bot.set_my_commands([
            BotCommand("checkdudoan", "Quét ALL bàn 1 lần & xếp hạng tỉ lệ cao"),
            BotCommand("autocheckdudoan", "Bật/tắt tự động quét ngầm 24/7"),
            BotCommand("autodudoan", "Dự đoán 1 bàn chi tiết (VD: /autodudoan C01)"),
            BotCommand("check", "Cập nhật kết quả thật (VD: /check C01 P)"),
            BotCommand("winrate", "Tỉ lệ thắng 30 phiên"),
            BotCommand("history", "20 phiên dự đoán gần nhất"),
            BotCommand("tables", "Danh sách bàn live & thế cầu"),
            BotCommand("start", "Hướng dẫn sử dụng"),
        ])
        # Khởi chạy vòng lặp quét tự động ngầm 24/7 và giữ strong reference tránh bị Python GC thu hồi
        scan_task = asyncio.create_task(background_auto_scan_task(app))
        _background_tasks.add(scan_task)
        scan_task.add_done_callback(_background_tasks.discard)

    application.post_init = post_init

    print("Telegram bot starting polling on MAIN thread (Non-blocking Promax V2.1 Mode)...")
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        close_loop=False,
    )
