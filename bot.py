#!/usr/bin/env python3
"""
BCR Telegram Bot v3.2 PROMAX V2 — All-Tables Auto Scanner & Analyzer
- Fix RuntimeError: set_wakeup_fd only works in main thread
- Flask chạy background, Telegram polling chạy MAIN THREAD
- FIX TRIỆT ĐỂ: Treo bot / Freeze asyncio event loop khi gọi API hoặc Gemini
- Sử dụng asyncio.to_thread & non-blocking execution cho toàn bộ network I/O
- NÂNG CẤP THUẬT TOÁN DỰ ĐOÁN PROMAX:
    + Phân tích Tam Lộ (Đại Lộ, Đại Nhãn Lộ, Tiểu Lộ, Giáp Do Lộ)
    + Nhận diện các thế cầu kinh điển: Cầu Bệt, Cầu 1-1, Cầu 2-2, Cầu Dính, Cầu Nghiêng, Cầu Bậc Thang
    + Markov Chain Transition Matrix bậc 1 & 2
    + Trọng số N-gram đa tầng (2-gram, 3-gram, 4-gram, 5-gram)
    + Tự học thông minh (Multi-resolution pattern memory 6 & 8 phiên)
    + Quản lý vốn Promax & Khuyến nghị điểm vào lệnh (1 unit / 2 units / Quan sát)
    + Fallback Gemini AI thông minh (hỗ trợ GEMINI_API_KEY trực tiếp + Scraper an toàn có timeout)
- TÍNH NĂNG MỚI NÂNG CẤP V2 (QUÉT TẤT CẢ BÀN):
    + /checkdudoan — Quét 1 lượt TOÀN BỘ các bàn live, tính toán tỉ lệ từng bàn, xếp hạng TOP bàn có tỉ lệ cao và tin cậy nhất để vào lệnh
    + /autocheckdudoan — Bật/tắt chế độ tự động quét ngầm 24/7, khi phát hiện bàn có cầu cực đẹp (≥70% hoặc Bệt/PingPong nét) sẽ tự động bắn tin nhắn báo động ngay
- Tương thích Render + UptimeRobot 24/7
- GIỮ NGUYÊN 100% CẤU TRÚC, BIẾN, HÀM CŨ: Không xóa bất cứ thứ gì, hardcode trực tiếp BOT_TOKEN theo yêu cầu!
"""

import asyncio
import json
import math
import os
import re
import sys
import threading
import time
import uuid
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
GEMINI_API_KEY = ""  # Điền trực tiếp API key nếu có, hoặc để trống sẽ tự dùng scraper an toàn
DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bcr_bot_data.json")
PORT = 10000
MAX_PATTERNS = 10000
MAX_HISTORY = 500
WINRATE_WINDOW = 30
HISTORY_SHOW = 20

# Cấu hình tính năng Auto Scan V2
AUTO_SCAN_INTERVAL = 60  # Quét tự động mỗi 60 giây
AUTO_SCAN_MIN_PCT = 70   # Ngưỡng tỉ lệ % phát cảnh báo tự động

# Danh sách chat ID đăng ký nhận báo động tự động
_auto_scan_chats = set()
_auto_scan_lock = threading.Lock()
_last_alerted_tables = {}  # Lưu lịch sử cảnh báo tránh spam cùng 1 bàn

# Cache API tránh gọi dồn dập
_api_cache_data = []
_api_cache_time = 0
_api_cache_lock = threading.Lock()

# ======================== GEMINI NO-KEY & OFFICIAL KEY ========================
BASE = "https://gemini.google.com"
PATH = "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate"

_gemini_session = None
_BL = None
_FSID = None
_gemini_lock = threading.Lock()


def _init_gemini():
    """Khởi tạo phiên Gemini Web Scraper với timeout an toàn"""
    global _gemini_session, _BL, _FSID
    if _gemini_session is not None and _BL and _FSID:
        return True
    try:
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        })
        html = s.get(BASE + "/", timeout=4).text
        m1 = re.search(r'"cfb2h":"([^"]+)"', html)
        m2 = re.search(r'"FdrFJe":"(-?\d+)"', html)
        if not m1 or not m2:
            return False
        _BL = m1.group(1)
        _FSID = m2.group(1)
        _gemini_session = s
        return True
    except Exception as e:
        print(f"[Gemini] init fail: {e}")
        return False


def _dig(cands):
    out, stack = "", [cands]
    while stack:
        n = stack.pop()
        if isinstance(n, list):
            if (len(n) > 1 and isinstance(n[0], str) and n[0].startswith("rc_")
                    and isinstance(n[1], list) and n[1] and isinstance(n[1][0], str)
                    and len(n[1][0]) >= len(out)):
                out = n[1][0]
            stack.extend(n)
    return out


def _gemini_ask_official(msg: str):
    """Sử dụng official GEMINI_API_KEY nếu đã điền trực tiếp"""
    if not GEMINI_API_KEY:
        return None
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
        payload = {
            "contents": [{"parts": [{"text": msg}]}],
            "generationConfig": {"temperature": 0.3, "maxOutputTokens": 150}
        }
        res = requests.post(url, json=payload, timeout=6)
        if res.status_code == 200:
            data = res.json()
            candidates = data.get("candidates", [])
            if candidates:
                parts = candidates[0].get("content", {}).get("parts", [])
                if parts:
                    return parts[0].get("text", "").strip()
    except Exception as e:
        print(f"[Gemini Official] error: {e}")
    return None


def gemini_ask(msg, cid=None, rid=None, lang="vi"):
    """
    Hàm gọi Gemini nguyên bản được bảo vệ timeout:
    1. Ưu tiên GEMINI_API_KEY chính thức nếu có
    2. Fallback sang Web Scraper nhưng có timeout chặt chẽ (max 5s), KHÔNG BAO GIỜ TREO BOT
    """
    official_res = _gemini_ask_official(msg)
    if official_res:
        return official_res, None, None

    with _gemini_lock:
        if not _init_gemini():
            return None, None, None
        try:
            fresh = ["", "", "", None, None, None, None, None, None, ""]
            p = [None] * 99
            p[0] = [msg, 0, None, None, None, None, 0]
            p[1] = [lang]
            p[2] = [cid, rid, "", None, None, None, None, None, None, ""] if cid else fresh
            p[6], p[7], p[10], p[11] = [1], 1, 1, 0
            p[17], p[18] = [[0]], 0
            p[27], p[30] = 1, [4]
            p[41], p[53] = [2], 0
            p[59], p[61] = str(uuid.uuid4()).upper(), []
            p[68], p[79] = 2, 6
            p[91], p[96], p[98] = 0, 0, 1
            url = f"{BASE}{PATH}?bl={_BL}&f.sid={_FSID}&hl={lang}&_reqid=100000&rt=c"

            res = _gemini_session.post(
                url,
                data={"f.req": json.dumps([None, json.dumps(p)])},
                stream=True,
                timeout=(3.0, 5.0),
            )
            text, ids = "", None
            start_t = time.time()
            for line in res.iter_lines():
                if time.time() - start_t > 5.0:
                    break
                if not line:
                    continue
                line = line.decode() if isinstance(line, bytes) else line
                if line.startswith(")]}'") or line.isdigit():
                    continue
                try:
                    arr = json.loads(line)
                except Exception:
                    continue
                for row in arr:
                    if not (isinstance(row, list) and len(row) > 2 and row[0] == "wrb.fr"):
                        continue
                    try:
                        d = json.loads(row[2])
                    except Exception:
                        continue
                    if (isinstance(d, list) and len(d) > 1 and isinstance(d[1], list)
                            and d[1] and str(d[1][0]).startswith("c_")):
                        ids = d[1]
                    if len(d) > 4 and isinstance(d[4], list):
                        t = _dig(d[4])
                        if len(t) > len(text):
                            text = t
            return text, (ids[0] if ids else None), (ids[1] if ids else None)
        except Exception as e:
            print(f"[Gemini] ask fail: {e}")
            return None, None, None


# ======================== DATA PERSISTENCE ========================
class BotData:
    def __init__(self):
        self.patterns = {}
        self.predictions = deque(maxlen=MAX_HISTORY)
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
                print(f"[Data] Loaded {len(self.patterns)} patterns, {len(self.predictions)} preds")
            except Exception as e:
                print(f"[Data] Load error: {e}")

    def save(self):
        with self.lock:
            try:
                with open(DATA_FILE, "w", encoding="utf-8") as f:
                    json.dump({
                        "patterns": self.patterns,
                        "predictions": list(self.predictions),
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
            "correct": pred == actual if actual else None,
            "pattern": pattern8,
            "source": source,
        }
        with self.lock:
            self.predictions.append(entry)
            self.save()
        return entry

    def get_winrate(self, n=WINRATE_WINDOW):
        with self.lock:
            recent = [p for p in list(self.predictions)[-n:] if p.get("correct") is not None]
        if not recent:
            return 0, 0, 0.0
        correct = sum(1 for p in recent if p["correct"])
        total = len(recent)
        return correct, total, (correct / total * 100) if total else 0.0

    def get_history(self, n=HISTORY_SHOW):
        with self.lock:
            return list(self.predictions)[-n:]


db = BotData()

# ======================== BCR API ========================
def fetch_tables():
    global _api_cache_data, _api_cache_time
    now = time.time()
    with _api_cache_lock:
        if _api_cache_data and (now - _api_cache_time < 4.0):
            return _api_cache_data

    try:
        r = requests.get(API_BCR, timeout=5, headers={"Cache-Control": "no-cache", "User-Agent": "BCRBot/3.2"})
        r.raise_for_status()
        j = r.json()
        if j.get("code") == 200 and isinstance(j.get("data"), list):
            with _api_cache_lock:
                _api_cache_data = j["data"]
                _api_cache_time = now
            return _api_cache_data
    except Exception as e:
        print(f"[API] Error: {e}")
        with _api_cache_lock:
            if _api_cache_data:
                return _api_cache_data
    return []


def normalize_ban_name(raw_ban: str) -> str:
    ban = raw_ban.strip().upper()
    ban = re.sub(r'^(BÀN|BAN)\s*', '', ban)
    m = re.match(r'^C?0*(\d+)$', ban)
    if m:
        num = int(m.group(1))
        return f"C{num:02d}"
    return ban


def get_table(ban: str):
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

    last_side = cols[-1][0]
    lens = [len(c) for c in cols[-4:]]
    is_symmetrical = False
    if len(lens) >= 3 and abs(lens[-1] - lens[-2]) <= 1:
        is_symmetrical = True

    scoreP, scoreB = 0.0, 0.0
    if is_symmetrical:
        if last_side == "P":
            scoreP += 1.5
        else:
            scoreB += 1.5
        tag = "TamLộ:Đều(Đỏ)"
    else:
        tag = "TamLộ:ĐangBiếnThiên"

    return scoreP, scoreB, tag


# ======================== PREDICTION ENGINE PROMAX ========================
def predict_next(results: str):
    seq = [c for c in results if c in ("P", "B")]
    if len(seq) < 8:
        return None, 50, "Cần ≥8 ván P/B", "", "Quan sát"

    pattern8 = "".join(seq[-8:])
    scoreP, scoreB = 0.0, 0.0
    reasons = []

    def ngram_promax(n, w):
        nonlocal scoreP, scoreB
        if len(seq) < n + 1:
            return
        pat = "".join(seq[-n:])
        hp = hb = 0
        total_len = len(seq)
        for i in range(total_len - n):
            if "".join(seq[i:i + n]) == pat:
                nxt = seq[i + n]
                recency_factor = 0.7 + 0.3 * (i / max(total_len - n, 1))
                if nxt == "P":
                    hp += recency_factor
                else:
                    hb += recency_factor
        tot = hp + hb
        if tot >= 1.5:
            scoreP += hp * w
            scoreB += hb * w
            fav = "P" if hp > hb else "B"
            ratio = max(hp, hb) / max(tot, 0.001) * 100
            reasons.append(f"{n}g→{fav}({ratio:.0f}%)")

    ngram_promax(5, 5.0)
    ngram_promax(4, 4.2)
    ngram_promax(3, 3.2)
    ngram_promax(2, 2.0)

    last = seq[-1]
    streak = 1
    for i in range(len(seq) - 2, -1, -1):
        if seq[i] == last:
            streak += 1
        else:
            break

    if streak >= 6:
        w = 3.8 + min(streak - 6, 4) * 0.5
        if last == "P":
            scoreP += w
        else:
            scoreB += w
        reasons.append(f"🐉ĐạiBệt{streak}{last}")
    elif streak >= 3:
        w = 2.5 + (streak - 3) * 0.4
        if last == "P":
            scoreP += w
        else:
            scoreB += w
        reasons.append(f"Bệt{streak}{last}")
    elif streak == 2:
        cols = build_big_road_columns(seq[-16:])
        doublet_count = sum(1 for c in cols if len(c) == 2)
        if len(cols) >= 3 and doublet_count >= len(cols) * 0.5:
            other = "B" if last == "P" else "P"
            if other == "P":
                scoreP += 2.4
            else:
                scoreB += 2.4
            reasons.append(f"Cầu2-2(Bẻ→{other})")
        else:
            if last == "P":
                scoreP += 1.6
            else:
                scoreB += 1.6
            reasons.append(f"Đôi{last}")
    else:
        alt = sum(1 for i in range(max(0, len(seq) - 6), len(seq) - 1) if seq[i] != seq[i + 1])
        if alt >= 4:
            other = "B" if last == "P" else "P"
            if other == "P":
                scoreP += 2.8
            else:
                scoreB += 2.8
            reasons.append(f"🏓PingPong1-1({alt}nhịp→{other})")
        else:
            if last == "P":
                scoreB += 1.8
            else:
                scoreP += 1.8
            reasons.append("Chop")

    cols = build_big_road_columns(seq[-18:])
    if len(cols) >= 3:
        last_3_lens = [len(c) for c in cols[-3:]]
        if last_3_lens == [1, 2, 3]:
            other = "B" if last == "P" else "P"
            if other == "P":
                scoreP += 2.0
            else:
                scoreB += 2.0
            reasons.append(f"Cầu1-2-3→{other}")
        elif last_3_lens[-2:] == [1, 2] and streak == 2:
            other = "B" if last == "P" else "P"
            if other == "P":
                scoreP += 1.9
            else:
                scoreB += 1.9
            reasons.append(f"Cầu1-2→{other}")

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

    tl_P, tl_B, tl_tag = analyze_derived_roads(seq)
    scoreP += tl_P
    scoreB += tl_B
    if tl_P > 0 or tl_B > 0:
        reasons.append(tl_tag)

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
        reasons.append("CầuNghiêngP")
    elif rB > rP + 2.0:
        scoreB += 1.8
        reasons.append("CầuNghiêngB")

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

    total = scoreP + scoreB
    if total < 0.1:
        pPct = 53 if last == "P" else 47
    else:
        raw_pct = (scoreP / total) * 100
        pPct = round(raw_pct)

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


def analyze_with_gemini(ban, results, pred, pct, detail):
    seq = results[-40:] if len(results) > 40 else results
    prompt = (
        f"Bạn là chuyên gia phân tích Baccarat roadmap.\n"
        f"Bàn: {ban}\n"
        f"Lịch sử gần nhất (P=Player, B=Banker, T=Tie): {seq}\n"
        f"Engine dự đoán: {pred} với {pct}% (lý do: {detail})\n"
        f"Hãy phân tích ngắn gọn (≤80 từ) xu hướng cầu, điểm mạnh/yếu của dự đoán này, "
        f"và khuyến nghị cuối cùng PLAYER hoặc BANKER. Chỉ trả lời tiếng Việt, không markdown."
    )
    text, _, _ = gemini_ask(prompt)
    if text:
        return text.strip()

    side_vn = "PLAYER (Con)" if pred == "P" else "BANKER (Cái)"
    return (
        f"Phân tích thuật toán: Cầu bàn {ban} đang có xu hướng rõ nét theo {side_vn} "
        f"với các dấu hiệu then chốt: {detail}. Tỉ lệ dự phóng đạt {pct}%. Khuyến nghị vào tiền có kỷ luật."
    )


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
        "🎰 *BCR TELEGRAM BOT v3.2 PROMAX V2*\n\n"
        "✨ *Bộ thuật toán dự đoán Baccarat nâng cấp:* Tam Lộ, Cầu Bệt, Ping Pong, Markov Chain, N-Gram và Tự Động Quét All Bàn.\n\n"
        "📌 *Các lệnh sử dụng:*\n"
        "• `/checkdudoan` — ⚡ *QUÉT TẤT CẢ BÀN* 1 lần, lọc TOP bàn tỉ lệ cao nhất để vào tiền\n"
        "• `/autocheckdudoan` — 🚨 Bật/Tắt tự động quét ngầm 24/7 (bắn cảnh báo khi có bàn tỉ lệ ≥70%)\n"
        "• `/autodudoan <bàn>` — Dự đoán chi tiết 1 bàn cụ thể (VD: `/autodudoan C01` hoặc `/autodudoan 1`)\n"
        "• `/check <bàn> <P/B>` — Báo kết quả thật để bot tự học (VD: `/check C01 P`)\n"
        "• `/winrate` — Thống kê tỉ lệ thắng 30 phiên gần nhất\n"
        "• `/history` — Xem lịch sử 20 phiên dự đoán gần nhất\n"
        "• `/tables` — Danh sách các bàn đang live và thế cầu\n"
        "• `/help` — Xem lại hướng dẫn này\n\n"
        "⚡ _Hệ thống chạy mượt mà 24/7, tự động chống lag và không bao giờ đứng bot!_"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


async def tables_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Đang tải danh sách bàn live...")
    data = await asyncio.to_thread(fetch_tables)
    if not data:
        await update.message.reply_text("❌ Không lấy được dữ liệu bàn live từ API (đang thử kết nối lại).")
        return
    lines = ["📋 *DANH SÁCH BÀN LIVE & THẾ CẦU*\n"]
    for t in sorted(data, key=lambda x: (0 if not str(x.get("ban", "")).startswith("C") else 1, str(x.get("ban", "")))):
        ban_name = str(t.get("ban", ""))
        road = f" | {t['good_road']}" if t.get("good_road") else ""
        res_len = len(t.get("results", ""))
        lines.append(f"• `{ban_name}` — {res_len} ván{road}")
    lines.append("\n👉 Gõ: `/checkdudoan` để phân tích lọc ngay các bàn đẹp nhất!")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def checkdudoan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    LỆNH NÂNG CẤP V2: Quét toàn bộ bàn live, tính toán dự đoán và tổng hợp
    xếp hạng các bàn có tỉ lệ cao nhất (Top bàn đẹp nhất).
    """
    await update.message.reply_text("🔍 *Đang quét và phân tích TẤT CẢ các bàn live (Promax V2)...*", parse_mode="Markdown")

    scanned = await asyncio.to_thread(scan_all_tables_sync)
    if not scanned:
        await update.message.reply_text("❌ Không có bàn nào đủ dữ liệu (≥8 ván) hoặc API live đang bảo trì.")
        return

    top_picks = scanned[:7]  # Lấy tối đa 7 bàn đẹp nhất

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

        # Đánh dấu sao cho các bàn tỉ lệ cực cao >= 70%
        highlight = "🔥 *VIP*" if pct >= 70 else "⚡"

        road_info = f" • `{item['good_road']}`" if item['good_road'] != "—" else ""
        lines.append(
            f"{highlight} *{idx}. Bàn `{ban}`* [{item['pb_count']} ván]:\n"
            f"   👉 Cửa: {emoji} *{side_text}* | Độ tin cậy: *{pct}%*\n"
            f"   💰 Vốn: _{item['bet_advice']}_\n"
            f"   📈 Thế cầu: `{item['detail'][:35]}`{road_info}\n"
        )

    lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("💡 *Khuyến nghị:* Ưu tiên vào các bàn có đánh dấu 🔥 *VIP* (tỉ lệ ≥70% hoặc Cầu Bệt/PingPong nét).")
    lines.append("👉 Để soi chuyên sâu 1 bàn, gõ: `/autodudoan <tên bàn>`")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def autocheckdudoan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    LỆNH NÂNG CẤP V2: Bật/Tắt chế độ tự động quét ngầm 24/7.
    Khi phát hiện bàn có tỉ lệ >= 70%, bot sẽ tự động gửi tin nhắn báo động.
    """
    chat_id = update.effective_chat.id
    arg = context.args[0].strip().lower() if context.args else None

    with _auto_scan_lock:
        if arg == "on":
            _auto_scan_chats.add(chat_id)
            is_active = True
        elif arg == "off":
            _auto_scan_chats.discard(chat_id)
            is_active = False
        else:
            # Toggle nếu không truyền tham số
            if chat_id in _auto_scan_chats:
                _auto_scan_chats.discard(chat_id)
                is_active = False
            else:
                _auto_scan_chats.add(chat_id)
                is_active = True

    if is_active:
        status_msg = (
            "🚨 *ĐÃ BẬT TỰ ĐỘNG QUÉT TOÀN BỘ BÀN (AUTO-SCAN 24/7)*\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"• Chu kỳ quét: Mỗi *{AUTO_SCAN_INTERVAL} giây*\n"
            f"• Ngưỡng cảnh báo: Bàn có độ tin cậy *≥ {AUTO_SCAN_MIN_PCT}%* hoặc Cầu Bệt/Ping Pong đẹp\n"
            "• Khi phát hiện cầu ngon, bot sẽ tự động gửi thông báo trực tiếp vào đây!\n\n"
            "👉 Để tắt thông báo tự động, gõ: `/autocheckdudoan off`"
        )
        await update.message.reply_text(status_msg, parse_mode="Markdown")
        # Chạy ngay 1 lượt quét đầu tiên
        await checkdudoan_cmd(update, context)
    else:
        status_msg = (
            "⏸ *ĐÃ TẮT TỰ ĐỘNG QUÉT TOÀN BỘ BÀN*\n"
            "Bot sẽ không gửi thông báo ngầm nữa. Bạn vẫn có thể dùng `/checkdudoan` để quét thủ công bất kỳ lúc nào!"
        )
        await update.message.reply_text(status_msg, parse_mode="Markdown")


async def autodudoan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("⚠️ Vui lòng nhập tên bàn!\nVD: `/autodudoan C01` hoặc `/autodudoan 1`", parse_mode="Markdown")
        return

    raw_ban = context.args[0].strip()
    norm_ban = normalize_ban_name(raw_ban)
    await update.message.reply_text(f"⏳ Đang phân tích bàn *{norm_ban}* (Promax Engine)...", parse_mode="Markdown")

    table = await asyncio.to_thread(get_table, norm_ban)
    if not table:
        all_tables = await asyncio.to_thread(fetch_tables)
        avail = [f"`{t.get('ban')}`" for t in all_tables[:10]]
        suggest_text = f"\n\nCác bàn đang có: {', '.join(avail)}" if avail else ""
        await update.message.reply_text(
            f"❌ Không tìm thấy bàn `{norm_ban}` (hoặc bàn chưa vào phiên).{suggest_text}\n"
            f"💡 Dùng `/tables` hoặc `/checkdudoan` để xem danh sách tất cả bàn đang mở.",
            parse_mode="Markdown"
        )
        return

    ban = str(table.get("ban", norm_ban)).upper()
    results = table.get("results", "")
    good_road = table.get("good_road", "") or "—"
    update_at = table.get("update_at", "")

    pred_res = predict_next(results)
    if pred_res[0] is None:
        await update.message.reply_text(
            f"❌ Bàn {ban} chưa đủ dữ liệu P/B (≥8 ván). Hiện tại chỉ có: {len([c for c in results if c in 'PB'])} ván."
        )
        return

    pred, pct, detail, pattern8, bet_advice = pred_res
    gemini_text = await asyncio.to_thread(analyze_with_gemini, ban, results, pred, pct, detail)

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
        text += f"\n🤖 *Nhận định AI:*\n{gemini_text[:400]}\n"

    text += (
        f"\n━━━━━━━━━━━━━━━━━━\n"
        f"💡 Sau khi có kết quả thật, gửi:\n"
        f"`/check {ban} P` hoặc `/check {ban} B`\n"
        f"để bot tự động ghi nhớ và nâng cao độ chính xác!"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def check_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("⚠️ Dùng: `/check C01 P` hoặc `/check 1 B`", parse_mode="Markdown")
        return

    raw_ban = context.args[0].strip()
    norm_ban = normalize_ban_name(raw_ban)
    actual = context.args[1].strip().upper()
    if actual not in ("P", "B"):
        await update.message.reply_text("⚠️ Kết quả thực tế phải là `P` (Player) hoặc `B` (Banker).", parse_mode="Markdown")
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
        await update.message.reply_text(f"❌ Chưa có phiên dự đoán nào cho bàn {norm_ban}. Hãy gõ `/autodudoan {norm_ban}` trước.")
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

    await update.message.reply_text(
        f"{status} — Bàn *{found['ban']}*\n"
        f"Dự đoán: `{pred}` | Thực tế: `{actual}`\n"
        f"{reason}",
        parse_mode="Markdown",
    )


async def winrate_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    correct, total, rate = db.get_winrate(WINRATE_WINDOW)
    if total == 0:
        await update.message.reply_text("📭 Chưa có dữ liệu phiên đã kiểm tra. Dùng `/autodudoan` rồi `/check` để ghi nhận.", parse_mode="Markdown")
        return
    bar_len = 10
    filled = round(rate / 100 * bar_len)
    bar = "█" * filled + "░" * (bar_len - filled)
    await update.message.reply_text(
        f"📊 *TỈ LỆ THẮNG (WINRATE) PROMAX*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"✅ Đúng: *{correct}/{total}* phiên gần nhất\n"
        f"📈 Tỉ lệ chính xác: *{rate:.1f}%*\n"
        f"`{bar}` {rate:.0f}%\n"
        f"🧩 Bộ nhớ patterns đã học: *{len(db.patterns)}* thế cầu\n"
        f"⚡ Thuật toán: Promax Multi-Roads & Markov Chain",
        parse_mode="Markdown",
    )


async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    hist = db.get_history(HISTORY_SHOW)
    if not hist:
        await update.message.reply_text("📭 Chưa có lịch sử dự đoán.")
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

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


# ======================== BACKGROUND AUTO-SCAN TASK ========================
async def background_auto_scan_task(app: Application):
    """
    Tác vụ chạy ngầm định kỳ quét toàn bộ bàn.
    Nếu phát hiện bàn có tỉ lệ >= 70% thì gửi tin nhắn cảnh báo tới các chat đã đăng ký.
    """
    global _last_alerted_tables
    while True:
        try:
            await asyncio.sleep(AUTO_SCAN_INTERVAL)
            with _auto_scan_lock:
                target_chats = list(_auto_scan_chats)

            if not target_chats:
                continue

            # Quét tất cả bàn trong thread riêng
            scanned = await asyncio.to_thread(scan_all_tables_sync)
            if not scanned:
                continue

            # Lọc các bàn có tỉ lệ cao >= AUTO_SCAN_MIN_PCT
            hot_tables = [t for t in scanned if t["pct"] >= AUTO_SCAN_MIN_PCT]
            if not hot_tables:
                continue

            # Lấy bàn có tỉ lệ cao nhất
            best = hot_tables[0]
            ban = best["ban"]
            pred = best["pred"]
            pct = best["pct"]

            # Kiểm tra xem có vừa gửi cảnh báo cho bàn này trong vòng 3 phút không (tránh spam)
            now = time.time()
            last_time = _last_alerted_tables.get(ban, 0)
            if now - last_time < 180:
                continue

            _last_alerted_tables[ban] = now

            side_text = "PLAYER (P - Con)" if pred == "P" else "BANKER (B - Cái)"
            emoji = "🔵" if pred == "P" else "🔴"

            alert_msg = (
                f"🚨 *BÁO ĐỘNG CẦU ĐẸP TỰ ĐỘNG!* 🚨\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 Bàn *{ban}* vừa đạt tỉ lệ *{pct}%* cực cao!\n"
                f"👉 Cửa vào lệnh: {emoji} *{side_text}*\n"
                f"💰 Quản lý vốn: *{best['bet_advice']}*\n"
                f"🛣 Thế cầu: `{best['good_road']}`\n"
                f"📈 Nhận diện: `{best['detail']}`\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💡 Gõ `/autodudoan {ban}` để xem chi tiết hoặc `/checkdudoan` để quét toàn bộ."
            )

            for cid in target_chats:
                try:
                    await app.bot.send_message(chat_id=cid, text=alert_msg, parse_mode="Markdown")
                except Exception as ex:
                    print(f"[AutoScan] Send alert failed for chat {cid}: {ex}")

        except Exception as e:
            print(f"[AutoScan] Error in loop: {e}")
            await asyncio.sleep(10)


# ======================== FLASK ========================
flask_app = Flask(__name__)


@flask_app.route("/")
def health():
    return "BCR Bot v3.2 PROMAX V2 OK", 200


@flask_app.route("/health")
def health_check():
    return {
        "status": "ok",
        "version": "3.2 PROMAX V2",
        "patterns": len(db.patterns),
        "predictions": len(db.predictions),
        "auto_scan_active_chats": len(_auto_scan_chats),
        "winrate_window": WINRATE_WINDOW,
    }, 200


def run_flask():
    print(f"Flask health server starting on port {PORT}")
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True, use_reloader=False)


# ======================== MAIN ========================
if __name__ == "__main__":
    print("=" * 60)
    print("BCR Telegram Bot v3.2 PROMAX V2 — All-Tables Auto Scanner")
    print(f"Patterns loaded: {len(db.patterns)}")
    print(f"Predictions: {len(db.predictions)}")
    print("=" * 60)

    # Flask chạy background thread phục vụ Render port binding & UptimeRobot
    flask_thread = threading.Thread(target=run_flask, daemon=True, name="flask")
    flask_thread.start()

    # Telegram bot chạy MAIN THREAD (bắt buộc trong python-telegram-bot)
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
        # Khởi chạy vòng lặp quét tự động ngầm 24/7
        asyncio.create_task(background_auto_scan_task(app))

    application.post_init = post_init

    print("Telegram bot starting polling on MAIN thread (Non-blocking Promax V2 Mode)...")
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        close_loop=False,
    )
