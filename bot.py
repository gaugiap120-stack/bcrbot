#!/usr/bin/env python3
"""
BCR Telegram Bot v3.2 — Render Fixed
- Fix RuntimeError: set_wakeup_fd only works in main thread
- Flask chạy background, Telegram polling chạy MAIN THREAD
- Tương thích Render + UptimeRobot 24/7
"""

import json
import re
import uuid
import sys
import time
import os
import hashlib
import threading
from datetime import datetime
from collections import defaultdict, deque

import requests
from flask import Flask
from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ======================== CONFIG ========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8538503731:AAGijB4lXUC2vwEuiEeYfRsGjmROocdOqV8")
API_BCR = "https://construct-vacuum-bosnia-travel.trycloudflare.com/api/bcr"
DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bcr_bot_data.json")
MAX_PATTERNS = 10000
MAX_HISTORY = 500
WINRATE_WINDOW = 30
HISTORY_SHOW = 20

# ======================== GEMINI NO-KEY ========================
BASE = "https://gemini.google.com"
PATH = "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate"

_gemini_session = None
_BL = None
_FSID = None
_gemini_lock = threading.Lock()


def _init_gemini():
    global _gemini_session, _BL, _FSID
    if _gemini_session is not None:
        return True
    try:
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        })
        html = s.get(BASE + "/", timeout=20).text
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


def gemini_ask(msg, cid=None, rid=None, lang="vi"):
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
                timeout=60,
            )
            text, ids = "", None
            for line in res.iter_lines():
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
        if len(pattern8) != 8 or next_result not in ("P", "B"):
            return
        key = pattern8
        with self.lock:
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
                if p["misses"] > p.get("hits", 0) + 2:
                    p["next"] = next_result
                    p["hits"] = 0
                    p["misses"] = 0
            self.save()

    def get_pattern_boost(self, pattern8: str):
        with self.lock:
            p = self.patterns.get(pattern8)
            if not p:
                return 0.0, 0.0, 0.0
            hits = p.get("hits", 0)
            misses = p.get("misses", 0)
            total = hits + misses
            if total < 1:
                return 0.0, 0.0, 0.0
            conf = min(1.0, total / 8.0)
            strength = (hits - misses) / max(total, 1) * conf * 3.5
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
    try:
        r = requests.get(API_BCR, timeout=15, headers={"Cache-Control": "no-cache"})
        r.raise_for_status()
        j = r.json()
        if j.get("code") == 200 and isinstance(j.get("data"), list):
            return j["data"]
    except Exception as e:
        print(f"[API] Error: {e}")
    return []


def get_table(ban: str):
    tables = fetch_tables()
    ban = ban.strip().upper()
    for t in tables:
        if str(t.get("ban", "")).upper() == ban:
            return t
    return None


# ======================== PREDICTION ENGINE ========================
def predict_next(results: str):
    seq = [c for c in results if c in ("P", "B")]
    if len(seq) < 8:
        return None, 50, "Cần ≥8 ván P/B", ""

    pattern8 = "".join(seq[-8:])
    scoreP, scoreB = 0.0, 0.0
    reasons = []

    def ngram(n, w):
        nonlocal scoreP, scoreB
        if len(seq) < n + 1:
            return
        pat = "".join(seq[-n:])
        hp = hb = 0
        for i in range(len(seq) - n):
            if "".join(seq[i:i + n]) == pat:
                nxt = seq[i + n]
                if nxt == "P":
                    hp += 1
                else:
                    hb += 1
        tot = hp + hb
        if tot >= 2:
            scoreP += hp * w
            scoreB += hb * w
            if hp != hb:
                reasons.append(f"{n}g→{'P' if hp > hb else 'B'}({max(hp,hb)}/{tot})")

    ngram(4, 4.0)
    ngram(3, 3.0)
    ngram(2, 1.8)

    last = seq[-1]
    streak = 1
    for i in range(len(seq) - 2, -1, -1):
        if seq[i] == last:
            streak += 1
        else:
            break

    if streak >= 3:
        w = 2.2 + min(streak - 3, 3) * 0.4
        if last == "P":
            scoreP += w
        else:
            scoreB += w
        reasons.append(f"Streak{streak}{last}")
    elif streak == 2:
        if last == "P":
            scoreP += 1.6
        else:
            scoreB += 1.6
        reasons.append(f"Đôi{last}")
    else:
        if last == "P":
            scoreB += 1.8
        else:
            scoreP += 1.8
        reasons.append("Chop")

    recent = seq[-10:]
    rP = rB = 0.0
    for i, v in enumerate(recent):
        w = 0.6 + (i / max(len(recent), 1)) * 1.4
        if v == "P":
            rP += w
        else:
            rB += w
    if rP > rB + 1.5:
        scoreP += 1.5
        reasons.append("BiasP")
    elif rB > rP + 1.5:
        scoreB += 1.5
        reasons.append("BiasB")

    bp, bb, conf = db.get_pattern_boost(pattern8)
    if bp > 0 or bb > 0:
        scoreP += bp
        scoreB += bb
        reasons.append(f"Learned({conf:.0%})")

    totP = seq.count("P")
    totB = seq.count("B")
    if abs(totP - totB) / len(seq) > 0.12:
        if totP > totB:
            scoreP += 0.7
        else:
            scoreB += 0.7

    alt = sum(1 for i in range(max(0, len(seq) - 6), len(seq) - 1) if seq[i] != seq[i + 1])
    if alt >= 4:
        if last == "P":
            scoreB += 1.4
        else:
            scoreP += 1.4
        reasons.append("PingPong")

    total = scoreP + scoreB
    if total < 0.1:
        pPct = 53 if last == "P" else 47
    else:
        pPct = round(scoreP / total * 100)
    pPct = max(18, min(82, pPct))
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

    detail = " • ".join(reasons[:4]) if reasons else "Multi-factor"
    return pred, pct, detail, pattern8


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
    return text.strip() if text else None


# ======================== TELEGRAM HANDLERS ========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🎰 *BCR PRO BOT v3.2*\n\n"
        "Lệnh:\n"
        "`/autodudoan <bàn>` — Dự đoán phiên tiếp theo\n"
        "VD: `/autodudoan C01` hoặc `/autodudoan 7`\n\n"
        "`/winrate` — Tỉ lệ đúng 30 phiên gần nhất\n"
        "`/history` — 20 phiên dự đoán gần nhất\n"
        "`/tables` — Danh sách bàn đang live\n"
        "`/check <bàn>` — Kiểm tra kết quả thực tế + cập nhật học\n\n"
        "Bot tự học pattern 8 phiên (tối đa 10.000 cầu)."
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


async def tables_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Đang lấy danh sách bàn...")
    data = fetch_tables()
    if not data:
        await update.message.reply_text("❌ Không lấy được API.")
        return
    lines = ["📋 *DANH SÁCH BÀN LIVE*\n"]
    for t in sorted(data, key=lambda x: (0 if not str(x["ban"]).startswith("C") else 1, x["ban"])):
        road = f" | {t['good_road']}" if t.get("good_road") else ""
        lines.append(f"• `{t['ban']}` — {len(t.get('results',''))} ván{road}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def autodudoan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("⚠️ Dùng: `/autodudoan C01` hoặc `/autodudoan 7`", parse_mode="Markdown")
        return

    ban = context.args[0].strip().upper()
    await update.message.reply_text(f"⏳ Đang phân tích bàn *{ban}*...", parse_mode="Markdown")

    table = get_table(ban)
    if not table:
        await update.message.reply_text(f"❌ Không tìm thấy bàn `{ban}`. Dùng /tables để xem danh sách.", parse_mode="Markdown")
        return

    results = table.get("results", "")
    good_road = table.get("good_road", "") or "—"
    update_at = table.get("update_at", "")

    pred, pct, detail, pattern8 = predict_next(results)
    if pred is None:
        await update.message.reply_text(f"❌ Bàn {ban} chưa đủ dữ liệu P/B (≥8 ván). Hiện: {len([c for c in results if c in 'PB'])} ván.")
        return

    gemini_text = None
    try:
        gemini_text = analyze_with_gemini(ban, results, pred, pct, detail)
    except Exception:
        pass

    side = "PLAYER (P)" if pred == "P" else "BANKER (B)"
    emoji = "🔵" if pred == "P" else "🔴"

    db.record_prediction(ban, pred, None, pct, pattern8, "auto")

    text = (
        f"{emoji} *DỰ ĐOÁN BÀN {ban}*\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🎯 Kết quả: *{side}*\n"
        f"📊 Độ tin cậy: *{pct}%*\n"
        f"🛣 Good Road: `{good_road}`\n"
        f"📈 Phân tích: `{detail}`\n"
        f"🧩 Pattern 8: `{pattern8}`\n"
        f"🕐 Cập nhật: `{update_at}`\n"
        f"📦 Tổng ván: `{len(results)}`\n"
    )
    if gemini_text:
        text += f"\n🤖 *Gemini:*\n{gemini_text[:400]}\n"

    text += (
        f"\n💡 Sau khi có kết quả thật, gửi:\n"
        f"`/check {ban} P` hoặc `/check {ban} B`\n"
        f"để bot học pattern."
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def check_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("⚠️ Dùng: `/check C01 P` hoặc `/check 7 B`", parse_mode="Markdown")
        return

    ban = context.args[0].strip().upper()
    actual = context.args[1].strip().upper()
    if actual not in ("P", "B"):
        await update.message.reply_text("⚠️ Kết quả phải là P hoặc B.")
        return

    with db.lock:
        found = None
        for p in reversed(db.predictions):
            if p["ban"] == ban and p.get("actual") is None:
                found = p
                break
        if not found:
            for p in reversed(db.predictions):
                if p["ban"] == ban:
                    found = p
                    break

    if not found:
        await update.message.reply_text(f"❌ Chưa có dự đoán nào cho bàn {ban}. Hãy /autodudoan trước.")
        return

    pred = found["pred"]
    pattern8 = found.get("pattern", "")
    correct = (pred == actual)

    found["actual"] = actual
    found["correct"] = correct
    db.save()

    if pattern8 and len(pattern8) == 8:
        db.add_pattern(pattern8, actual, correct)

    status = "✅ ĐÚNG" if correct else "❌ SAI"
    if not correct:
        reason = (
            f"\n📝 Phân tích sai:\n"
            f"• Dự đoán: {pred} | Thực tế: {actual}\n"
            f"• Pattern `{pattern8}` đã được cập nhật theo kết quả thật.\n"
            f"• Lần sau gặp lại 8 phiên này sẽ nghiêng về hướng đúng hơn."
        )
    else:
        reason = (
            f"\n📝 Học thành công:\n"
            f"• Pattern `{pattern8}` → {actual} được tăng điểm.\n"
            f"• Lần sau xuất hiện 8 phiên này sẽ cộng % vào kết quả đúng."
        )

    await update.message.reply_text(
        f"{status} — Bàn *{ban}*\n"
        f"Dự đoán: `{pred}` | Thực tế: `{actual}`\n"
        f"{reason}",
        parse_mode="Markdown",
    )


async def winrate_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    correct, total, rate = db.get_winrate(WINRATE_WINDOW)
    if total == 0:
        await update.message.reply_text("📭 Chưa có dữ liệu đã kiểm tra. Dùng /autodudoan rồi /check.")
        return
    bar_len = 10
    filled = round(rate / 100 * bar_len)
    bar = "█" * filled + "░" * (bar_len - filled)
    await update.message.reply_text(
        f"📊 *WINRATE {total} PHIÊN GẦN NHẤT*\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"✅ Đúng: *{correct}/{total}*\n"
        f"📈 Tỉ lệ: *{rate:.1f}%*\n"
        f"`{bar}` {rate:.0f}%\n"
        f"🧩 Patterns đã học: *{len(db.patterns)}*",
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
        lines.append(f"Đúng *{correct_n}/{total_c}* phiên đã check\n")
    else:
        lines.append("Chưa có phiên nào được /check\n")

    lines.append("━━━━━━━━━━━━━━━━")
    for i, h in enumerate(reversed(hist), 1):
        ts = h.get("ts", "")[-8:]
        ban = h.get("ban", "?")
        pred = h.get("pred", "?")
        actual = h.get("actual")
        if actual is None:
            st = "⏳ Chờ"
        elif h.get("correct"):
            st = "✅ Đúng"
        else:
            st = "❌ Sai"
        lines.append(f"{i}. `{ban}` {pred}→{actual or '?'} | {st} | {ts}")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


# ======================== FLASK ========================
flask_app = Flask(__name__)

@flask_app.route("/")
def health():
    return "BCR Bot v3.2 OK", 200

@flask_app.route("/health")
def health_check():
    return {"status": "ok", "patterns": len(db.patterns), "predictions": len(db.predictions)}, 200


def run_flask():
    port = int(os.environ.get("PORT", 10000))
    print(f"Flask health server starting on port {port}")
    flask_app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)


# ======================== MAIN ========================
if __name__ == "__main__":
    print("=" * 50)
    print("BCR Telegram Bot v3.2 — Render Fixed")
    print(f"Patterns loaded: {len(db.patterns)}")
    print(f"Predictions: {len(db.predictions)}")
    print("=" * 50)

    # Flask chạy background thread
    flask_thread = threading.Thread(target=run_flask, daemon=True, name="flask")
    flask_thread.start()

    # Telegram bot chạy MAIN THREAD (bắt buộc)
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("autodudoan", autodudoan))
    application.add_handler(CommandHandler("check", check_cmd))
    application.add_handler(CommandHandler("winrate", winrate_cmd))
    application.add_handler(CommandHandler("history", history_cmd))
    application.add_handler(CommandHandler("tables", tables_cmd))

    async def post_init(app):
        await app.bot.delete_webhook(drop_pending_updates=True)
        await app.bot.set_my_commands([
            BotCommand("autodudoan", "Dự đoán bàn (VD: /autodudoan C01)"),
            BotCommand("check", "Cập nhật kết quả thật (VD: /check C01 P)"),
            BotCommand("winrate", "Tỉ lệ đúng 30 phiên"),
            BotCommand("history", "20 phiên gần nhất"),
            BotCommand("tables", "Danh sách bàn live"),
            BotCommand("start", "Hướng dẫn"),
        ])
    application.post_init = post_init

    print("Telegram bot starting polling on MAIN thread...")
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        close_loop=False,
    )
