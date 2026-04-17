import streamlit as st
import sqlite3
import json
import pandas as pd

DB_PATH = "orders.db"

def get_orders():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM orders ORDER BY id DESC")
    rows = cursor.fetchall()
    conn.close()

    orders = []
    for row in rows:
        order = dict(row)
        try:
            order["items"] = json.loads(order["items"])
        except:
            order["items"] = []
        orders.append(order)

    return orders


st.set_page_config(page_title="Admin Dashboard", layout="wide")

st.title("📦 Admin - Quản lý đơn hàng")

orders = get_orders()

if not orders:
    st.warning("Chưa có đơn hàng")
    st.stop()

# Convert sang DataFrame
df = pd.DataFrame(orders)

# Filter
status_filter = st.selectbox(
    "Lọc theo trạng thái",
    ["Tất cả", "paid", "pending"]
)

if status_filter != "Tất cả":
    df = df[df["payment_status"] == status_filter]

st.dataframe(df)

st.markdown("---")

# Chi tiết từng đơn
for order in orders:
    with st.expander(f"Đơn #{order['id']} - {order['payment_status']}"):
        st.write("👤 Khách:", order.get("customer_name"))
        st.write("📞 SĐT:", order.get("phone"))
        st.write("📍 Địa chỉ:", order.get("address"))
        st.write("💰 Tổng tiền:", order.get("total"))

        st.write("🛒 Món:")
        for item in order["items"]:
            st.write(f"- {item['name']} ({item['size']}) x{item['quantity']}")