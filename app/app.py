import os
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify, render_template_string
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

DB = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     os.getenv("DB_PORT",     5432),
    "dbname":   os.getenv("DB_NAME",     "roseamor"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

# La tabla de pedidos se encuentra en otro schema para evitar conflictos
INIT_SQL = """
CREATE SCHEMA IF NOT EXISTS app;
CREATE TABLE IF NOT EXISTS app.orders (
    order_id    TEXT        PRIMARY KEY,
    customer_id TEXT        NOT NULL,
    sku         TEXT        NOT NULL,
    quantity    INTEGER     NOT NULL CHECK (quantity > 0),
    unit_price  NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    order_date  DATE        NOT NULL,
    channel     TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def get_conn():
    return psycopg2.connect(**DB)


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(INIT_SQL)

with open(os.path.join(os.path.dirname(__file__), "index.html"), encoding="utf-8") as f:
    HTML = f.read()


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/orders", methods=["POST"])
def create_order():
    body = request.get_json(silent=True) or {}

    required = ["order_id", "customer_id", "sku", "quantity", "unit_price", "order_date", "channel"]
    missing  = [f for f in required if not str(body.get(f, "")).strip()]
    if missing:
        return jsonify({"error": f"Campos obligatorios faltantes: {', '.join(missing)}"}), 400

    try:
        qty   = int(body["quantity"])
        price = float(body["unit_price"])
        if qty <= 0:
            return jsonify({"error": "quantity debe ser mayor a 0"}), 400
        if price < 0:
            return jsonify({"error": "unit_price no puede ser negativo"}), 400
        datetime.strptime(body["order_date"], "%Y-%m-%d")
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    valid_channels = {"retail", "ecommerce", "wholesale", "export"}
    if body["channel"].lower() not in valid_channels:
        return jsonify({"error": f"channel inválido. Opciones: {valid_channels}"}), 400

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO app.orders
                       (order_id, customer_id, sku, quantity, unit_price, order_date, channel)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (body["order_id"], body["customer_id"], body["sku"],
                     qty, price, body["order_date"], body["channel"].lower()),
                )
    except psycopg2.errors.UniqueViolation:
        return jsonify({"error": f"order_id '{body['order_id']}' ya existe"}), 409
    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"message": f"Pedido {body['order_id']} registrado correctamente"}), 201


@app.route("/orders", methods=["GET"])
def list_orders():
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM app.orders ORDER BY created_at DESC LIMIT 100")
            rows = cur.fetchall()
    return jsonify([dict(r) for r in rows])


if __name__ == "__main__":
    init_db()
    app.run(debug=True, port=5000)
