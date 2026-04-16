import hashlib
import hmac
import html
import json
import os
import re
import sqlite3
import threading
import unicodedata
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from datetime import datetime
from io import BytesIO

import pandas as pd
import qrcode
import requests
from dotenv import load_dotenv
from openai import OpenAI
from telegram.error import BadRequest
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

MENU_PATH = "data/Menu.csv"
DB_PATH = "orders.db"
APP_REF = None


def get_env(key: str, default: str = "") -> str:
    value = os.getenv(key, default)
    if isinstance(value, str):
        return value.strip().strip('"').strip("'")
    return default


def normalize_text(text: str) -> str:
    text = text.replace("đ", "d").replace("Đ", "D")
    text = unicodedata.normalize("NFD", text)
    text = "".join(char for char in text if unicodedata.category(char) != "Mn")
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()


def parse_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def format_money(amount: int) -> str:
    return f"{amount:,}đ".replace(",", ".")


def load_menu() -> list[dict]:
    frame = pd.read_csv(MENU_PATH)
    records = []
    for row in frame.to_dict("records"):
        record = {
            "category": str(row["category"]).strip(),
            "item_id": str(row["item_id"]).strip(),
            "name": str(row["name"]).strip(),
            "description": str(row["description"]).strip(),
            "price_m": int(row["price_m"]),
            "price_l": int(row["price_l"]),
            "available": parse_bool(row.get("available", True)),
        }
        record["name_norm"] = normalize_text(record["name"])
        record["category_norm"] = normalize_text(record["category"])
        record["aliases"] = build_aliases(record)
        records.append(record)

    alias_counts = {}
    for record in records:
        for alias in record["aliases"]:
            alias_counts[alias] = alias_counts.get(alias, 0) + 1

    for record in records:
        record["aliases"] = [
            alias
            for alias in record["aliases"]
            if alias_counts[alias] == 1
            or alias == record["name_norm"]
            or alias == normalize_text(record["item_id"])
        ]
    return records


def build_aliases(record: dict) -> list[str]:
    aliases = {record["name_norm"], normalize_text(record["item_id"])}
    short_forms = [
        "tra sua ",
        "tra trai cay ",
        "ca phe ",
        "da xay ",
    ]
    for prefix in short_forms:
        if record["name_norm"].startswith(prefix):
            aliases.add(record["name_norm"].removeprefix(prefix).strip())
    return sorted((alias for alias in aliases if alias), key=len, reverse=True)


MENU_ITEMS = load_menu()
MENU_BY_ID = {item["item_id"]: item for item in MENU_ITEMS}
CATEGORIES = list(dict.fromkeys(item["category"] for item in MENU_ITEMS if item["available"]))
CATEGORY_INDEX = {str(index): category for index, category in enumerate(CATEGORIES, start=1)}
MENU_SEARCH_ENTRIES = sorted(
    [
        {
            "item_id": item["item_id"],
            "alias": alias,
            "is_topping": item["category_norm"] == "topping",
        }
        for item in MENU_ITEMS
        for alias in item["aliases"]
    ],
    key=lambda entry: len(entry["alias"]),
    reverse=True,
)

TOKEN = get_env("TELEGRAM_BOT_TOKEN")
OPENAI_API_KEY = get_env("OPENAI_API_KEY")
OPENAI_MODEL = get_env("OPENAI_MODEL", "gpt-4o-mini")

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

if not TOKEN or TOKEN == "YOUR_BOT_TOKEN_HERE":
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured in .env.")

MOMO_ENABLED = get_env("MOMO_ENABLED", "false").lower() == "true"
MOMO_API_ENDPOINT = get_env("MOMO_API_ENDPOINT")
MOMO_PAYMENT_URL = get_env("MOMO_PAYMENT_URL")
MOMO_PARTNER_CODE = get_env("MOMO_PARTNER_CODE")
MOMO_ACCESS_KEY = get_env("MOMO_ACCESS_KEY")
MOMO_SECRET_KEY = get_env("MOMO_SECRET_KEY")
PUBLIC_BASE_URL = get_env("PUBLIC_BASE_URL")
MOMO_RETURN_URL = get_env("MOMO_RETURN_URL") or (f"{PUBLIC_BASE_URL}/momo-return" if PUBLIC_BASE_URL else "")
MOMO_NOTIFY_URL = get_env("MOMO_NOTIFY_URL") or (f"{PUBLIC_BASE_URL}/momo-ipn" if PUBLIC_BASE_URL else "")
MOMO_ORDER_INFO = get_env("MOMO_ORDER_INFO", "Thanh toan don tra sua")
MOMO_REQUEST_TYPE = get_env("MOMO_REQUEST_TYPE", "payWithMethod")
MOMO_PARTNER_NAME = get_env("MOMO_PARTNER_NAME", "Casso Milk Tea Bot")
MOMO_STORE_ID = get_env("MOMO_STORE_ID", "CASSO_STORE")
WEBHOOK_HOST = get_env("WEBHOOK_HOST", "0.0.0.0")
WEBHOOK_PORT = int(get_env("WEBHOOK_PORT", "8000") or "8000")
OWNER_NAME = get_env("OWNER_NAME", "Chu quan")
OWNER_PHONE = get_env("OWNER_PHONE")
OWNER_ADDRESS = get_env("OWNER_ADDRESS")
OWNER_TELEGRAM_CHAT_ID = get_env("OWNER_TELEGRAM_CHAT_ID")

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row


def ensure_column(cursor: sqlite3.Cursor, name: str, definition: str) -> None:
    cursor.execute("PRAGMA table_info(orders)")
    columns = {row[1] for row in cursor.fetchall()}
    if name not in columns:
        cursor.execute(f"ALTER TABLE orders ADD COLUMN {name} {definition}")


def init_db() -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer TEXT,
            items TEXT,
            total INTEGER,
            status TEXT,
            momo_order_id TEXT
        )
        """
    )
    ensure_column(cursor, "customer_name", "TEXT")
    ensure_column(cursor, "customer_username", "TEXT")
    ensure_column(cursor, "phone", "TEXT")
    ensure_column(cursor, "address", "TEXT")
    ensure_column(cursor, "note", "TEXT")
    ensure_column(cursor, "payment_status", "TEXT")
    ensure_column(cursor, "payment_method", "TEXT")
    ensure_column(cursor, "payment_url", "TEXT")
    ensure_column(cursor, "created_at", "TEXT")
    ensure_column(cursor, "owner_notified", "INTEGER DEFAULT 0")
    conn.commit()


init_db()


def row_to_dict(row: sqlite3.Row | None) -> dict | None:
    return dict(row) if row is not None else None


def get_order_by_momo_order_id(momo_order_id: str) -> dict | None:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM orders WHERE momo_order_id = ?", (momo_order_id,))
    return row_to_dict(cursor.fetchone())

CHECKOUT_FIELDS = ["customer_name", "phone", "address", "note"]
FIELD_PROMPTS = {
    "customer_name": "Mình cần tên người nhận để chốt đơn. Bạn nhắn tên giúp mình nhé.",
    "phone": "Cho mình xin số điện thoại người nhận.",
    "address": "Cho mình xin địa chỉ giao hàng chi tiết.",
    "note": "Bạn có ghi chú gì thêm không? Nếu không có, nhắn `không` là được.",
}
FIELD_LABELS = {
    "customer_name": "Người nhận",
    "phone": "Số điện thoại",
    "address": "Địa chỉ",
    "note": "Ghi chú",
}
SKIP_NOTE_KEYWORDS = {"khong", "không", "ko", "skip", "bo qua", "bỏ qua", "none"}
NUMBER_WORDS = {
    "mot": 1,
    "hai": 2,
    "ba": 3,
    "bon": 4,
    "tu": 4,
    "nam": 5,
    "sau": 6,
}


def get_cart(context: ContextTypes.DEFAULT_TYPE) -> list[dict]:
    return context.user_data.setdefault("cart", [])


def get_checkout_info(context: ContextTypes.DEFAULT_TYPE) -> dict:
    return context.user_data.setdefault("checkout_info", {})


def clear_session(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["cart"] = []
    context.user_data.pop("checkout_info", None)
    context.user_data.pop("awaiting_field", None)
    context.user_data.pop("awaiting_compact_info", None)
    context.user_data.pop("pending_payment", None)
    context.user_data.pop("pending_order_db_id", None)


def cart_total(cart: list[dict]) -> int:
    return sum(item["line_total"] for item in cart)


def add_to_cart(context: ContextTypes.DEFAULT_TYPE, item_id: str, size: str, quantity: int) -> dict:
    item = MENU_BY_ID[item_id]
    unit_price = item["price_l"] if size == "L" else item["price_m"]
    cart = get_cart(context)
    for line in cart:
        if line["item_id"] == item_id and line["size"] == size:
            line["quantity"] += quantity
            line["line_total"] = line["quantity"] * line["unit_price"]
            return line

    line = {
        "item_id": item_id,
        "name": item["name"],
        "size": size,
        "quantity": quantity,
        "unit_price": unit_price,
        "line_total": unit_price * quantity,
        "category": item["category"],
    }
    cart.append(line)
    return line


def find_next_missing_field(context: ContextTypes.DEFAULT_TYPE) -> str | None:
    info = get_checkout_info(context)
    for field in CHECKOUT_FIELDS:
        if field == "note":
            if field not in info:
                return field
            continue
        if not str(info.get(field, "")).strip():
            return field
    return None


def build_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Xem menu", callback_data="menu_root"),
                InlineKeyboardButton("Xem gio", callback_data="cart_view"),
            ],
            [
                InlineKeyboardButton("Thanh toan", callback_data="checkout_start"),
                InlineKeyboardButton("Xoa gio", callback_data="cart_clear"),
            ],
        ]
    )


def build_payment_keyboard(payment_url: str | None) -> InlineKeyboardMarkup:
    rows = []
    if payment_url:
        rows.append([InlineKeyboardButton("Mo trang thanh toan", url=payment_url)])
    rows.append(
        [
            InlineKeyboardButton("Da thanh toan", callback_data="payment_done"),
            InlineKeyboardButton("Xem gio", callback_data="cart_view"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def build_category_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(category, callback_data=f"cat:{index}")]
        for index, category in CATEGORY_INDEX.items()
    ]
    keyboard.append(
        [
            InlineKeyboardButton("Xem gio", callback_data="cart_view"),
            InlineKeyboardButton("Thanh toan", callback_data="checkout_start"),
        ]
    )
    return InlineKeyboardMarkup(keyboard)


def build_category_items_keyboard(category: str) -> InlineKeyboardMarkup:
    keyboard = []
    for item in MENU_ITEMS:
        if item["category"] != category or not item["available"]:
            continue
        keyboard.append(
            [
                InlineKeyboardButton(
                    f"{item['name']} M - {format_money(item['price_m'])}",
                    callback_data=f"add:{item['item_id']}:M",
                )
            ]
        )
        keyboard.append(
            [
                InlineKeyboardButton(
                    f"{item['name']} L - {format_money(item['price_l'])}",
                    callback_data=f"add:{item['item_id']}:L",
                )
            ]
        )
    keyboard.append(
        [
            InlineKeyboardButton("Quay lai", callback_data="menu_root"),
            InlineKeyboardButton("Xem gio", callback_data="cart_view"),
        ]
    )
    return InlineKeyboardMarkup(keyboard)


def build_cart_keyboard(context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    cart = get_cart(context)
    keyboard = []
    for index, item in enumerate(cart):
        keyboard.append(
            [
                InlineKeyboardButton("-1", callback_data=f"qty:dec:{index}"),
                InlineKeyboardButton(
                    f"{item['name']} {item['size']} x{item['quantity']}",
                    callback_data="noop",
                ),
                InlineKeyboardButton("+1", callback_data=f"qty:inc:{index}"),
                InlineKeyboardButton("Xoa", callback_data=f"qty:remove:{index}"),
            ]
        )
    keyboard.append(
        [
            InlineKeyboardButton("Them mon", callback_data="menu_root"),
            InlineKeyboardButton("Xac nhan don", callback_data="checkout_start"),
        ]
    )
    keyboard.append([InlineKeyboardButton("Xoa gio", callback_data="cart_clear")])
    return InlineKeyboardMarkup(keyboard)


def build_menu_text() -> str:
    parts = ["Menu hom nay:"]
    for category in CATEGORIES:
        parts.append("")
        parts.append(category)
        for item in MENU_ITEMS:
            if item["category"] != category or not item["available"]:
                continue
            parts.append(
                f"- {item['name']}: M {format_money(item['price_m'])}, L {format_money(item['price_l'])}"
            )
    return "\n".join(parts)


def build_menu_html() -> str:
    sections = ["<b>Menu hom nay</b>"]
    for category in CATEGORIES:
        rows = []
        # Keep each row <= ~50 chars to avoid Telegram wrapping on mobile.
        code_w = 5
        name_w = 24
        price_w = 9

        def truncate(text: str, width: int) -> str:
            text = text.strip()
            if len(text) <= width:
                return text
            if width <= 1:
                return text[:width]
            return text[: max(0, width - 1)].rstrip() + "…"

        rows.append(f"{'Ma':<{code_w}} {'Ten mon':<{name_w}} {'M':>{price_w}} {'L':>{price_w}}")
        rows.append("-" * (code_w + 1 + name_w + 1 + price_w + 1 + price_w))
        for item in MENU_ITEMS:
            if item["category"] != category or not item["available"]:
                continue
            name = truncate(item["name"], name_w)
            rows.append(
                f"{item['item_id']:<{code_w}} {name:<{name_w}} {format_money(item['price_m']):>{price_w}} {format_money(item['price_l']):>{price_w}}"
            )
        sections.append(f"\n<b>{html.escape(category)}</b>\n<pre>{html.escape(chr(10).join(rows))}</pre>")
    return "\n".join(sections)


def build_cart_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    cart = get_cart(context)
    if not cart:
        return "Gio hang dang trong. Ban co the nhan /menu hoac nhan tin tu nhien nhu `2 tra sua truyen thong size L`."

    lines = ["Gio hang hien tai:"]
    for index, item in enumerate(cart, start=1):
        lines.append(
            f"{index}. {item['name']} size {item['size']} x{item['quantity']} - {format_money(item['line_total'])}"
        )
    lines.append(f"Tong tam tinh: {format_money(cart_total(cart))}")

    info = context.user_data.get("checkout_info")
    if info:
        lines.append("")
        lines.append("Thong tin giao hang da co:")
        for field in CHECKOUT_FIELDS:
            value = info.get(field, "")
            if field == "note" and value == "":
                value = "Khong co"
            if value:
                lines.append(f"- {FIELD_LABELS[field]}: {value}")
    return "\n".join(lines)


def menu_prompt_payload() -> str:
    payload = [
        {
            "item_id": item["item_id"],
            "name": item["name"],
            "category": item["category"],
            "price_m": item["price_m"],
            "price_l": item["price_l"],
        }
        for item in MENU_ITEMS
        if item["available"]
    ]
    return json.dumps(payload, ensure_ascii=False)


def detect_quantity(message_norm: str, start: int) -> int:
    before = message_norm[max(0, start - 25) : start].strip()
    match = re.search(r"(\d+)\s*$", before)
    if match:
        return max(1, int(match.group(1)))
    words = before.split()
    if words and words[-1] in NUMBER_WORDS:
        return NUMBER_WORDS[words[-1]]
    return 1


def detect_size(message_norm: str, start: int, end: int, is_topping: bool) -> str:
    if is_topping:
        return "M"
    window = message_norm[max(0, start - 18) : min(len(message_norm), end + 18)]
    if re.search(r"\b(size\s*l|ly\s*l|l\b|large|lon)\b", window):
        return "L"
    return "M"


def extract_items_rule_based(message: str) -> dict:
    message_norm = normalize_text(message)
    updates = []
    occupied_spans = []

    for entry in MENU_SEARCH_ENTRIES:
        pattern = rf"(?<!\w){re.escape(entry['alias'])}(?!\w)"
        for match in re.finditer(pattern, message_norm):
            span = (match.start(), match.end())
            if any(not (span[1] <= used[0] or span[0] >= used[1]) for used in occupied_spans):
                continue
            quantity = detect_quantity(message_norm, match.start())
            size = detect_size(message_norm, match.start(), match.end(), entry["is_topping"])
            updates.append(
                {
                    "item_id": entry["item_id"],
                    "quantity": quantity,
                    "size": size,
                }
            )
            occupied_spans.append(span)
            break

    if not updates:
        return {"items": [], "reply": ""}

    names = []
    for update in updates:
        item = MENU_BY_ID[update["item_id"]]
        names.append(f"{update['quantity']} {item['name']} size {update['size']}")

    return {
        "items": updates,
        "reply": "Mình đã hiểu đơn gồm " + ", ".join(names) + ".",
    }


def extract_items_with_ai(message: str) -> dict | None:
    if not client:
        return None

    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Bạn là trợ lý bán trà sữa. "
                        "Hãy trích xuất món khách muốn đặt từ menu bên dưới. "
                        "Chỉ trả về JSON có dạng "
                        '{"reply":"...", "items":[{"item_id":"TS01","size":"M","quantity":2}]}. '
                        "Nếu không có món hợp lệ thì để items là mảng rỗng và reply là câu trả lời hỗ trợ ngắn. "
                        "Size chỉ được là M hoặc L. Topping mặc định size M. "
                        f"Menu: {menu_prompt_payload()}"
                    ),
                },
                {"role": "user", "content": message},
            ],
        )
        content = response.choices[0].message.content or "{}"
        data = json.loads(content)
        items = []
        for raw in data.get("items", []):
            item_id = str(raw.get("item_id", "")).strip()
            size = str(raw.get("size", "M")).upper()
            quantity = int(raw.get("quantity", 1))
            if item_id in MENU_BY_ID and size in {"M", "L"} and quantity > 0:
                items.append({"item_id": item_id, "size": size, "quantity": quantity})
        return {"items": items, "reply": str(data.get("reply", "")).strip()}
    except Exception:
        return None


def answer_with_ai(message: str, context: ContextTypes.DEFAULT_TYPE) -> str | None:
    if not client:
        return None

    cart = get_cart(context)
    cart_snapshot = [
        {
            "name": item["name"],
            "size": item["size"],
            "quantity": item["quantity"],
        }
        for item in cart
    ]

    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0.5,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Bạn là phiên bản AI của chủ quán trà sữa. "
                        "Trả lời ngắn gọn, thân thiện, tập trung hỗ trợ khách đặt món, xem menu, chọn size, topping, "
                        "thanh toán và giao hàng. Không bịa thông tin ngoài menu. "
                        f"Menu: {menu_prompt_payload()} "
                        f"Gio hang hien tai: {json.dumps(cart_snapshot, ensure_ascii=False)}"
                    ),
                },
                {"role": "user", "content": message},
            ],
        )
        return response.choices[0].message.content
    except Exception:
        return None


def generate_momo_payment_url(total: int, order_id: str) -> str:
    if MOMO_PAYMENT_URL:
        return f"{MOMO_PAYMENT_URL}?amount={total}&orderId={order_id}"

    def is_placeholder(value: str) -> bool:
        normalized = (value or "").strip().lower()
        return not normalized or "your_momo_" in normalized or normalized in {"changeme", "change_me"}

    ready_for_momo = all(
        [
            MOMO_ENABLED,
            MOMO_API_ENDPOINT,
            MOMO_PARTNER_CODE and not is_placeholder(MOMO_PARTNER_CODE),
            MOMO_ACCESS_KEY and not is_placeholder(MOMO_ACCESS_KEY),
            MOMO_SECRET_KEY and not is_placeholder(MOMO_SECRET_KEY),
            MOMO_RETURN_URL,
            MOMO_NOTIFY_URL,
        ]
    )
    if not ready_for_momo:
        raise ValueError(
            "MoMo chua duoc cau hinh day du. Can MOMO_API_ENDPOINT, PARTNER_CODE, ACCESS_KEY, "
            "SECRET_KEY, MOMO_RETURN_URL va MOMO_NOTIFY_URL. "
            "Luu y: MOMO_RETURN_URL nen ket thuc bang /momo-return va MOMO_NOTIFY_URL nen ket thuc bang /momo-ipn."
        )

    request_id = str(uuid.uuid4())
    amount = str(total)
    extra_data = ""
    request_type = MOMO_REQUEST_TYPE or "captureWallet"
    raw_signature = (
        f"accessKey={MOMO_ACCESS_KEY}&amount={amount}&extraData={extra_data}&ipnUrl={MOMO_NOTIFY_URL}"
        f"&orderId={order_id}&orderInfo={MOMO_ORDER_INFO}&partnerCode={MOMO_PARTNER_CODE}"
        f"&redirectUrl={MOMO_RETURN_URL}&requestId={request_id}&requestType={request_type}"
    )
    signature = hmac.new(
        MOMO_SECRET_KEY.encode("utf-8"),
        raw_signature.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    payload = {
        "partnerCode": MOMO_PARTNER_CODE,
        "accessKey": MOMO_ACCESS_KEY,
        "partnerName": MOMO_PARTNER_NAME,
        "storeId": MOMO_STORE_ID,
        "requestId": request_id,
        "amount": amount,
        "orderId": order_id,
        "orderInfo": MOMO_ORDER_INFO,
        "redirectUrl": MOMO_RETURN_URL,
        "ipnUrl": MOMO_NOTIFY_URL,
        "lang": "vi",
        "extraData": extra_data,
        "requestType": request_type,
        "signature": signature,
    }
    response = requests.post(MOMO_API_ENDPOINT, json=payload, timeout=15)
    if response.status_code >= 400:
        raise ValueError(f"MoMo HTTP {response.status_code}: {response.text}")
    data = response.json()
    if data.get("resultCode") not in {0, None}:
        raise ValueError(f"MoMo create payment loi: {data}")
    if data.get("payUrl"):
        return data["payUrl"]
    if data.get("deeplink"):
        return data["deeplink"]
    raise ValueError(f"MoMo response missing payUrl: {data}")


def is_demo_payment_url(payment_url: str) -> bool:
    normalized = payment_url.lower()
    return "example.com" in normalized or normalized.startswith("https://momo.vn/?amount=")


async def send_payment_qr(chat_id: int, application: Application, payment_url: str) -> None:
    qr_image = qrcode.make(payment_url)
    buffer = BytesIO()
    buffer.name = "payment_qr.png"
    qr_image.save(buffer, format="PNG")
    buffer.seek(0)
    await application.bot.send_photo(chat_id=chat_id, photo=buffer, caption="QR thanh toan")


async def safe_edit_message(query, text: str, reply_markup: InlineKeyboardMarkup | None = None) -> None:
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except BadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            raise


def build_checkout_summary(context: ContextTypes.DEFAULT_TYPE) -> str:
    cart = get_cart(context)
    info = get_checkout_info(context)
    lines = ["Xac nhan don hang:"]
    for index, item in enumerate(cart, start=1):
        lines.append(
            f"{index}. {item['name']} size {item['size']} x{item['quantity']} - {format_money(item['line_total'])}"
        )
    lines.append(f"Tong cong: {format_money(cart_total(cart))}")
    lines.append("")
    lines.append("Thong tin giao hang:")
    for field in CHECKOUT_FIELDS:
        value = info.get(field, "")
        if field == "note" and not value:
            value = "Khong co"
        lines.append(f"- {FIELD_LABELS[field]}: {value}")
    return "\n".join(lines)


def create_or_update_pending_order(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    momo_order_id: str,
    payment_url: str,
) -> int:
    cart = get_cart(context)
    info = get_checkout_info(context)
    username = update.effective_user.username or ""
    summary_items = json.dumps(cart, ensure_ascii=False)
    customer_label = info.get("customer_name") or username or "Anonymous"
    payment_method = "momo"
    existing_db_id = context.user_data.get("pending_order_db_id")
    cursor = conn.cursor()

    if existing_db_id:
        cursor.execute(
            """
            UPDATE orders
            SET customer = ?, customer_name = ?, customer_username = ?, phone = ?, address = ?, note = ?,
                items = ?, total = ?, status = ?, momo_order_id = ?, payment_status = ?, payment_method = ?,
                payment_url = ?
            WHERE id = ?
            """,
            (
                customer_label,
                info.get("customer_name", ""),
                username,
                info.get("phone", ""),
                info.get("address", ""),
                info.get("note", ""),
                summary_items,
                cart_total(cart),
                "pending_payment",
                momo_order_id,
                "pending",
                payment_method,
                payment_url,
                existing_db_id,
            ),
        )
        conn.commit()
        return int(existing_db_id)

    cursor.execute(
        """
        INSERT INTO orders (
            customer,
            customer_name,
            customer_username,
            phone,
            address,
            note,
            items,
            total,
            status,
            momo_order_id,
            payment_status,
            payment_method,
            payment_url,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            customer_label,
            info.get("customer_name", ""),
            username,
            info.get("phone", ""),
            info.get("address", ""),
            info.get("note", ""),
            summary_items,
            cart_total(cart),
            "pending_payment",
            momo_order_id,
            "pending",
            payment_method,
            payment_url,
            datetime.now().isoformat(timespec="seconds"),
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def mark_order_paid(momo_order_id: str, payment_url: str = "") -> dict | None:
    order = get_order_by_momo_order_id(momo_order_id)
    if not order:
        return None

    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE orders
        SET status = ?, payment_status = ?, payment_url = COALESCE(NULLIF(?, ''), payment_url)
        WHERE momo_order_id = ?
        """,
        ("confirmed", "paid", payment_url, momo_order_id),
    )
    conn.commit()
    return get_order_by_momo_order_id(momo_order_id)


def build_kitchen_ticket(context: ContextTypes.DEFAULT_TYPE, order_id: int) -> str:
    cart = get_cart(context)
    info = get_checkout_info(context)
    lines = [f"Don #{order_id} da chot", "", "Pha che:"]
    for item in cart:
        lines.append(f"- {item['name']} size {item['size']} x{item['quantity']}")
    lines.append("")
    lines.append("Giao hang:")
    lines.append(f"- Nguoi nhan: {info.get('customer_name', '')}")
    lines.append(f"- So dien thoai: {info.get('phone', '')}")
    lines.append(f"- Dia chi: {info.get('address', '')}")
    note = info.get("note", "")
    if note:
        lines.append(f"- Ghi chu: {note}")
    lines.append(f"- Thu ho: {format_money(cart_total(cart))}")
    return "\n".join(lines)


def build_kitchen_ticket_from_order(order: dict) -> str:
    items = json.loads(order["items"]) if order.get("items") else []
    lines = [f"Don #{order['id']} da thanh toan", ""]
    if OWNER_NAME or OWNER_PHONE or OWNER_ADDRESS:
        lines.append("Thong tin chu quan:")
        lines.append(f"- Chu quan: {OWNER_NAME}")
        if OWNER_PHONE:
            lines.append(f"- SDT chu quan: {OWNER_PHONE}")
        if OWNER_ADDRESS:
            lines.append(f"- Dia chi quan: {OWNER_ADDRESS}")
        lines.append("")
    lines.append("Pha che:")
    for item in items:
        lines.append(f"- {item['name']} size {item['size']} x{item['quantity']}")
    lines.append("")
    lines.append("Giao hang:")
    lines.append(f"- Nguoi nhan: {order.get('customer_name', '')}")
    lines.append(f"- So dien thoai: {order.get('phone', '')}")
    lines.append(f"- Dia chi: {order.get('address', '')}")
    if order.get("note"):
        lines.append(f"- Ghi chu: {order['note']}")
    lines.append(f"- Thu ho: {format_money(int(order.get('total', 0) or 0))}")
    return "\n".join(lines)


def notify_owner(text: str) -> None:
    if not OWNER_TELEGRAM_CHAT_ID or not TOKEN:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": OWNER_TELEGRAM_CHAT_ID, "text": text},
            timeout=10,
        )
    except Exception:
        return


def notify_owner_once(order: dict | None) -> None:
    if not order:
        return
    if int(order.get("owner_notified", 0) or 0) == 1:
        return

    notify_owner(build_kitchen_ticket_from_order(order))
    cursor = conn.cursor()
    cursor.execute("UPDATE orders SET owner_notified = 1 WHERE id = ?", (order["id"],))
    conn.commit()


def build_momo_ipn_signature(payload: dict) -> str:
    raw_signature = (
        f"accessKey={MOMO_ACCESS_KEY}&amount={payload.get('amount', '')}&extraData={payload.get('extraData', '')}"
        f"&message={payload.get('message', '')}&orderId={payload.get('orderId', '')}"
        f"&orderInfo={payload.get('orderInfo', '')}&orderType={payload.get('orderType', '')}"
        f"&partnerCode={payload.get('partnerCode', '')}&payType={payload.get('payType', '')}"
        f"&requestId={payload.get('requestId', '')}&responseTime={payload.get('responseTime', '')}"
        f"&resultCode={payload.get('resultCode', '')}&transId={payload.get('transId', '')}"
    )
    return hmac.new(
        MOMO_SECRET_KEY.encode("utf-8"),
        raw_signature.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def process_momo_ipn(payload: dict) -> tuple[int, str]:
    signature = str(payload.get("signature", ""))
    if not signature or not MOMO_SECRET_KEY or not MOMO_ACCESS_KEY:
        return 400, "Missing MoMo signature config"

    expected_signature = build_momo_ipn_signature(payload)
    if signature != expected_signature:
        return 400, "Invalid signature"

    if int(payload.get("resultCode", -1)) != 0:
        return 200, "Payment not successful"

    order = mark_order_paid(str(payload.get("orderId", "")), str(payload.get("payUrl", "")))
    if not order:
        return 404, "Order not found"

    notify_owner_once(order)
    return 200, "OK"


class MoMoCallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/momo-return":
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found")
            return

        params = parse_qs(parsed.query)
        order_id = params.get("orderId", [""])[0]
        result_code = params.get("resultCode", [""])[0]
        ok = result_code == "0"
        html = (
            "<html><body><h1>Thanh toan thanh cong</h1><p>Ban co the quay lai Telegram.</p></body></html>"
            if ok
            else "<html><body><h1>Thanh toan chua thanh cong</h1><p>Vui long quay lai Telegram de thu lai.</p></body></html>"
        )
        if ok and order_id:
            order = mark_order_paid(order_id)
            if order:
                notify_owner_once(order)
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/momo-ipn":
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found")
            return

        length = int(self.headers.get("Content-Length", "0") or 0)
        raw_body = self.rfile.read(length)
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Invalid JSON")
            return

        status, message = process_momo_ipn(payload)
        body = json.dumps({"message": message}).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        return


def start_momo_callback_server() -> None:
    if not MOMO_NOTIFY_URL and not MOMO_RETURN_URL:
        return

    server = ThreadingHTTPServer((WEBHOOK_HOST, WEBHOOK_PORT), MoMoCallbackHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.setdefault("cart", [])
    await update.message.reply_text(
        "Xin chao, minh la bot dat mon cho quan tra sua.\n"
        "Ban co the bam /menu de xem mon, hoac nhan tin tu nhien nhu:\n"
        "`2 tra sua truyen thong size L va 1 tran chau den`",
        reply_markup=build_main_keyboard(),
    )


async def show_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"Chat ID cua ban la: {update.effective_chat.id}")


async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        build_menu_html(),
        reply_markup=build_category_keyboard(),
        parse_mode="HTML",
    )


async def view_cart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        build_cart_text(context),
        reply_markup=build_cart_keyboard(context) if get_cart(context) else build_main_keyboard(),
    )


async def clear_cart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    clear_session(context)
    await update.message.reply_text("Mình đã xóa giỏ hàng hiện tại.", reply_markup=build_main_keyboard())


async def begin_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not get_cart(context):
        await update.message.reply_text("Giỏ hàng đang trống, mình chưa thể thanh toán.")
        return

    info = get_checkout_info(context)
    has_required_info = bool(info.get("customer_name") and info.get("phone") and info.get("address"))
    if has_required_info:
        await update.message.reply_text("Mình chốt lại đơn và chuyển sang bước thanh toán nhé.")
        await show_checkout_summary(update, context)
        return

    next_field = find_next_missing_field(context)
    context.user_data["awaiting_field"] = next_field
    await update.message.reply_text(
        build_cart_text(context) + "\n\n" + FIELD_PROMPTS[next_field],
        reply_markup=build_cart_keyboard(context),
    )


async def show_checkout_summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    order_id = str(uuid.uuid4())
    total = cart_total(get_cart(context))
    try:
        payment_url = generate_momo_payment_url(total, order_id)
    except Exception as exc:
        await message.reply_text(
            "Khong tao duoc link thanh toan MoMo.\n"
            f"Chi tiet: {exc}\n"
            "Ban can cau hinh lai thong tin MoMo trong .env."
        )
        return

    pending_db_id = create_or_update_pending_order(update, context, order_id, payment_url)
    context.user_data["pending_payment"] = {
        "order_id": order_id,
        "payment_url": payment_url,
    }
    context.user_data["pending_order_db_id"] = pending_db_id
    context.user_data["awaiting_field"] = None

    payment_note = "Mo link ben duoi de thanh toan. Sau khi thanh toan thanh cong, bot se nhan IPN tu MoMo va cap nhat don."

    await message.reply_text(
        build_checkout_summary(context)
        + f"\n\n{payment_note}\n{payment_url}",
        reply_markup=build_payment_keyboard(payment_url),
    )
    await send_payment_qr(update.effective_chat.id, context.application, payment_url)


async def confirm_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not get_cart(context):
        await update.message.reply_text("Hiện chưa có đơn để xác nhận.")
        return
    info = get_checkout_info(context)
    if not (info.get("customer_name") and info.get("phone") and info.get("address")):
        next_field = find_next_missing_field(context)
        context.user_data["awaiting_field"] = next_field
        await update.message.reply_text(FIELD_PROMPTS[next_field])
        return
    if "pending_payment" not in context.user_data:
        await show_checkout_summary(update, context)
        return

    pending_payment = context.user_data.get("pending_payment", {})
    order = get_order_by_momo_order_id(pending_payment.get("order_id", ""))
    if order and order.get("payment_status") == "paid":
        ticket = build_kitchen_ticket_from_order(order)
        await update.message.reply_text(
            "Thanh toán đã được ghi nhận. Quán bắt đầu làm món cho bạn nhé.\n\n" + ticket
        )
        clear_session(context)
        return

    await update.message.reply_text(
        "MoMo chưa gửi xác nhận thanh toán về bot. Nếu bạn vừa thanh toán xong, đợi vài giây rồi thử lại."
    )


async def handle_checkout_field(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    field = context.user_data.get("awaiting_field")
    if not field:
        return

    text = update.message.text.strip()
    info = get_checkout_info(context)

    if field == "phone":
        digits = re.sub(r"\D", "", text)
        if len(digits) < 9:
            await update.message.reply_text("Số điện thoại có vẻ chưa đúng, bạn nhập lại giúp mình nhé.")
            return
        info[field] = digits
    elif field == "note":
        info[field] = "" if normalize_text(text) in SKIP_NOTE_KEYWORDS else text
    else:
        info[field] = text

    next_field = find_next_missing_field(context)
    if next_field:
        context.user_data["awaiting_field"] = next_field
        await update.message.reply_text(FIELD_PROMPTS[next_field])
        return

    context.user_data["awaiting_field"] = None
    await show_checkout_summary(update, context)


def looks_like_intent(message: str, keywords: set[str]) -> bool:
    normalized = normalize_text(message)
    return any(normalize_text(keyword) in normalized for keyword in keywords)


async def handle_free_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text.strip()

    if context.user_data.get("awaiting_field"):
        await handle_checkout_field(update, context)
        return

    if looks_like_intent(text, {"menu", "thuc don", "thực đơn"}):
        await show_menu(update, context)
        return
    if looks_like_intent(text, {"gio hang", "giỏ hàng", "cart", "don hien tai"}):
        await view_cart(update, context)
        return
    if looks_like_intent(text, {"thanh toan", "checkout", "chot don", "chốt đơn"}):
        await begin_checkout(update, context)
        return
    if looks_like_intent(text, {"xac nhan", "confirm", "da chuyen khoan", "đã chuyển khoản"}):
        await confirm_payment(update, context)
        return
    if looks_like_intent(text, {"xoa gio", "clear cart", "huy don", "hủy đơn"}):
        await clear_cart(update, context)
        return

    parsed = extract_items_with_ai(text) or extract_items_rule_based(text)
    if parsed["items"]:
        added_lines = []
        for item in parsed["items"]:
            line = add_to_cart(context, item["item_id"], item["size"], item["quantity"])
            added_lines.append(
                f"- {line['name']} size {line['size']} x{item['quantity']} ({format_money(line['unit_price'])}/ly)"
            )

        reply = parsed["reply"] or "Mình đã thêm món vào giỏ hàng."
        await update.message.reply_text(
            reply
            + "\n"
            + "\n".join(added_lines)
            + f"\nTong tam tinh: {format_money(cart_total(get_cart(context)))}\n"
            "Ban co the nhan /cart de xem gio hoac /checkout de chot don.",
            reply_markup=build_main_keyboard(),
        )
        return

    ai_reply = answer_with_ai(text, context)
    if ai_reply:
        await update.message.reply_text(ai_reply, reply_markup=build_main_keyboard())
        return

    await update.message.reply_text(
        "Mình chưa hiểu rõ ý đó. Bạn có thể nhắn theo mẫu như `1 trà dâu tây size M` hoặc dùng /menu để xem món.",
        reply_markup=build_main_keyboard(),
    )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "menu_root":
        try:
            await query.edit_message_text(build_menu_html(), reply_markup=build_category_keyboard(), parse_mode="HTML")
        except BadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                raise
        return

    if data == "noop":
        return

    if data == "cart_view":
        await safe_edit_message(
            query,
            build_cart_text(context),
            reply_markup=build_cart_keyboard(context) if get_cart(context) else build_main_keyboard(),
        )
        return

    if data == "cart_clear":
        clear_session(context)
        await safe_edit_message(query, "Mình đã xóa giỏ hàng hiện tại.", reply_markup=build_main_keyboard())
        return

    if data == "checkout_start":
        if not get_cart(context):
            await safe_edit_message(query, "Giỏ hàng đang trống, mình chưa thể thanh toán.", reply_markup=build_main_keyboard())
            return
        info = get_checkout_info(context)
        has_required_info = bool(info.get("customer_name") and info.get("phone") and info.get("address"))
        if has_required_info:
            await query.message.reply_text("Mình sẽ chốt lại đơn trước, rồi chuyển sang bước thanh toán.")
            await show_checkout_summary(update, context)
            return

        next_field = find_next_missing_field(context)
        context.user_data["awaiting_field"] = next_field
        await query.message.reply_text(FIELD_PROMPTS[next_field])
        return

    if data == "payment_done":
        if not get_cart(context):
            await safe_edit_message(query, "Giỏ hàng đang trống.", reply_markup=build_main_keyboard())
            return
        pending_payment = context.user_data.get("pending_payment", {})
        order = get_order_by_momo_order_id(pending_payment.get("order_id", ""))
        if order and order.get("payment_status") == "paid":
            ticket = build_kitchen_ticket_from_order(order)
            await query.message.reply_text("Thanh toan da duoc MoMo xac nhan.\n\n" + ticket)
            clear_session(context)
            return
        await query.message.reply_text(
            "MoMo chua gui xac nhan thanh toan ve bot. Neu ban vua thanh toan xong, doi them vai giay roi bam lai."
        )
        return

    if data.startswith("qty:"):
        _, action, raw_index = data.split(":")
        index = int(raw_index)
        cart = get_cart(context)
        if index >= len(cart):
            await safe_edit_message(query, "Giỏ hàng đã thay đổi, bạn mở lại giúp mình nhé.", reply_markup=build_main_keyboard())
            return

        if action == "inc":
            cart[index]["quantity"] += 1
            cart[index]["line_total"] = cart[index]["quantity"] * cart[index]["unit_price"]
        elif action == "dec":
            cart[index]["quantity"] -= 1
            if cart[index]["quantity"] <= 0:
                cart.pop(index)
            else:
                cart[index]["line_total"] = cart[index]["quantity"] * cart[index]["unit_price"]
        elif action == "remove":
            cart.pop(index)

        await safe_edit_message(
            query,
            build_cart_text(context),
            reply_markup=build_cart_keyboard(context) if get_cart(context) else build_main_keyboard(),
        )
        return

    if data.startswith("cat:"):
        category = CATEGORY_INDEX.get(data.split(":", maxsplit=1)[1])
        if not category:
            await safe_edit_message(query, "Không tìm thấy danh mục.")
            return
        await safe_edit_message(
            query,
            f"Menu {category}:",
            reply_markup=build_category_items_keyboard(category),
        )
        return

    if data.startswith("add:"):
        _, item_id, size = data.split(":")
        line = add_to_cart(context, item_id, size, 1)
        await safe_edit_message(
            query,
            f"Đã thêm {line['name']} size {line['size']} vào giỏ.\n"
            f"Tổng tạm tính: {format_money(cart_total(get_cart(context)))}",
            reply_markup=build_cart_keyboard(context),
        )
        return


def main() -> None:
    global APP_REF
    application = Application.builder().token(TOKEN).build()
    APP_REF = application
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("chatid", show_chat_id))
    application.add_handler(CommandHandler("menu", show_menu))
    application.add_handler(CommandHandler("cart", view_cart))
    application.add_handler(CommandHandler("order", view_cart))
    application.add_handler(CommandHandler("clear", clear_cart))
    application.add_handler(CommandHandler("checkout", begin_checkout))
    application.add_handler(CommandHandler("confirm", confirm_payment))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_free_text))

    start_momo_callback_server()
    print("Bot dang chay. Mo Telegram va gui /start de thu.")
    application.run_polling()


if __name__ == "__main__":
    main()
