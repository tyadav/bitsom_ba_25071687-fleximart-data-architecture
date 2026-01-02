#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL pipeline for FlexiMart:
- Extract: Read customers_raw.csv, products_raw.csv, sales_raw.csv
- Transform:
    * Remove duplicates
    * Handle missing values (drop, fill, default)
    * Standardize phone formats to +91-XXXXXXXXXX
    * Standardize category names to Title case
    * Convert date formats to YYYY-MM-DD
    * Add surrogate keys (via DB auto-increments)
- Load: Insert cleaned data into MySQL/PostgreSQL using SQLAlchemy
- Report: Generate data_quality_report.txt
"""

import os
import re
import sys
import logging
from datetime import datetime
from typing import Dict, Any, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
load_dotenv()


# -----------------------
# Logging configuration
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("fleximart_etl")


def check_runtime_deps():
    """Log which Python executable is running and whether key DB auth packages are available."""
    import sys
    cryptography_ok = True
    pymysql_ok = True
    try:
        import cryptography  # noqa: F401
    except Exception:
        cryptography_ok = False
    try:
        import pymysql  # noqa: F401
    except Exception:
        pymysql_ok = False
    logger.info(f"Runtime python: {sys.executable}")
    logger.info(f"cryptography installed: {cryptography_ok}; pymysql installed: {pymysql_ok}")
    if not cryptography_ok:
        logger.warning("cryptography not available: install with `pip install cryptography` in the interpreter used to run ETL.")
    if not pymysql_ok:
        logger.warning("pymysql not available: install with `pip install pymysql` in the interpreter used to run ETL.")

# -----------------------
# Constants & Config
# -----------------------

CUSTOMERS_CSV = "customers_raw.csv"
PRODUCTS_CSV = "products_raw.csv"
SALES_CSV = "sales_raw.csv"

# Expected columns in raw files.
# If your CSVs use different headers, update these mappings.
CUSTOMERS_COLUMNS = {
    "first_name": "first_name",
    "last_name": "last_name",
    "email": "email",
    "phone": "phone",
    "city": "city",
    "registration_date": "registration_date",
}
PRODUCTS_COLUMNS = {
    "product_name": "product_name",
    "category": "category",
    "price": "price",
    "stock_quantity": "stock_quantity",
}
# Sales raw expected columns:
# The script maps sales into orders and order_items.
# If your sales file differs, update these mappings accordingly.
SALES_COLUMNS = {
    "order_id": "transaction_id", # maps to transaction_id
    "customer_id": "customer_id", # use customer_id instead of email
    "product_id": "product_id", # use product_id instead of product_name
    "quantity": "quantity",               # optional; default 'Pending'
    "unit_price": "unit_price",
    "order_date": "transaction_date", # maps to transaction_date
    "status": "status"
}

# Default values
DEFAULT_ORDER_STATUS = "Pending"
DEFAULT_STOCK = 0

# -----------------------
# Utility functions
# -----------------------

def get_db_engine() -> Engine:
    """Create SQLAlchemy engine for MySQL or PostgreSQL based on env vars."""
    db_type = os.getenv("DB_TYPE", "mysql").strip().lower()
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "3306" if db_type == "mysql" else "5432")
    name = os.getenv("DB_NAME", "fleximart")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")

    # Fail fast with clear message if credentials missing
    if not user or str(user).strip() == "":
        logger.error("DB_USER not set. Set DB_USER in .env or environment before running.")
        raise RuntimeError("DB_USER not set. Please set DB_USER in your environment or .env file.")
    if not password or str(password).strip() == "":
        logger.error("DB_PASSWORD not set. Set DB_PASSWORD in .env or environment before running.")
        raise RuntimeError("DB_PASSWORD not set. Please set DB_PASSWORD in your environment or .env file.")

    if db_type == "mysql":
        # Requires: pip install pymysql
        url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"
    elif db_type == "postgres":
        # Requires: pip install psycopg2-binary
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    else:
        raise ValueError("DB_TYPE must be 'mysql' or 'postgres'")

    logger.info(f"Connecting to {db_type} database {name} at {host}:{port}")
    return create_engine(url, future=True)


def read_csv_safe(path: str) -> pd.DataFrame:
    """Read CSV with basic error handling."""
    try:
        df = pd.read_csv(path, dtype=str)  # read all as str to avoid parsing issues
        logger.info(f"Read {len(df)} records from {path}")
        return df
    except FileNotFoundError:
        logger.error(f"CSV not found: {path}")
        raise
    except Exception as e:
        logger.error(f"Failed to read CSV {path}: {e}")
        raise


def normalize_phone_to_india(phone_raw: str) -> str:
    """
    Standardize to +91-XXXXXXXXXX
    Strategy:
    - Extract digits
    - If 10 digits, format +91-##########
    - If starts with 91 and length 12, take last 10
    - Else return empty string (will treat as missing)
    """
    if pd.isna(phone_raw):
        return ""
    digits = re.sub(r"\D", "", str(phone_raw))
    if len(digits) == 10:
        return f"+91-{digits}"
    if len(digits) == 12 and digits.startswith("91"):
        return f"+91-{digits[-10:]}"
    # Handle common cases with leading zeros or country codes
    if len(digits) > 10:
        # try last 10 digits
        candidate = digits[-10:]
        if len(candidate) == 10:
            return f"+91-{candidate}"
    return ""  # mark missing


def to_title_case(s: Any) -> str:
    """Standardize category names to Title case (e.g., 'Electronics')."""
    if pd.isna(s):
        return ""
    return str(s).strip().lower().title()


def parse_date_to_iso(date_raw: Any) -> str:
    if pd.isna(date_raw):
        return ""
    s = str(date_raw).strip()
    if not s:
        return ""
    # Try explicit, common formats first to avoid ambiguous parsing and warnings
    fmts = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m-%d-%Y", "%m/%d/%Y"]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue
    # Fallback to pandas parsing (set dayfirst=False to avoid the warning)
    try:
        dt = pd.to_datetime(s, errors="raise", dayfirst=False)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


def safe_to_decimal(s: Any) -> Any:
    """Convert to decimal-friendly string (for DB), return None if invalid."""
    if pd.isna(s):
        return None
    try:
        val = float(str(s).strip())
        return f"{val:.2f}"
    except Exception:
        return None


def safe_to_int(s: Any, default: int = None) -> Any:
    """Convert to int, returning default if invalid."""
    if pd.isna(s) or str(s).strip() == "":
        return default
    try:
        return int(float(str(s).strip()))
    except Exception:
        return default


def drop_duplicates(df: pd.DataFrame, subset=None) -> Tuple[pd.DataFrame, int]:
    """Drop duplicates and return count removed."""
    before = len(df)
    df2 = df.drop_duplicates(subset=subset, keep="first")
    removed = before - len(df2)
    return df2, removed


# -----------------------
# Extract
# -----------------------

def extract() -> Dict[str, pd.DataFrame]:
    customers_df = read_csv_safe(CUSTOMERS_CSV)
    products_df = read_csv_safe(PRODUCTS_CSV)
    sales_df = read_csv_safe(SALES_CSV)
    return {
        "customers": customers_df,
        "products": products_df,
        "sales": sales_df
    }


# -----------------------
# Transform
# -----------------------

def transform_customers(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    stats = {"processed": len(df), "duplicates_removed": 0, "missing_handled": 0}

    # Rename columns to expected canonical
    df = df.rename(columns={v: k for k, v in CUSTOMERS_COLUMNS.items()})

    # Remove duplicates based on email (critical unique)
    df, dup_removed = drop_duplicates(df, subset=["email"])
    stats["duplicates_removed"] += dup_removed

    # Handle missing email: drop rows without email
    before = len(df)
    df = df[~df["email"].isna() & (df["email"].str.strip() != "")]
    stats["missing_handled"] += (before - len(df))

    # Standardize phone
    df["phone"] = df["phone"].apply(normalize_phone_to_india)
    # If phone empty after normalization, treat as missing but keep record (phone is nullable)
    # Count missing phones handled (informational only)
    stats["missing_handled"] += int(df["phone"].eq("").sum())

    # Standardize registration_date
    df["registration_date"] = df["registration_date"].apply(parse_date_to_iso)
    # If date still empty, set to today's date as a reasonable default
    empty_dates = df["registration_date"].eq("").sum()
    if empty_dates:
        df.loc[df["registration_date"] == "", "registration_date"] = datetime.today().strftime("%Y-%m-%d")
        stats["missing_handled"] += empty_dates

    # Validate required fields not blank
    required = ["first_name", "last_name", "email"]
    for col in required:
        before = len(df)
        df = df[~df[col].isna() & (df[col].str.strip() != "")]
        stats["missing_handled"] += (before - len(df))

    # Trim strings
    for col in ["first_name", "last_name", "email", "city", "phone"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    return df, stats


def transform_products(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    stats = {"processed": len(df), "duplicates_removed": 0, "missing_handled": 0}

    df = df.rename(columns={v: k for k, v in PRODUCTS_COLUMNS.items()})

    # Remove duplicates by product_name + category
    df, dup_removed = drop_duplicates(df, subset=["product_name", "category"])
    stats["duplicates_removed"] += dup_removed

    # Standardize category names
    df["category"] = df["category"].apply(to_title_case)

    # Handle missing price: drop rows with invalid price
    df["price"] = df["price"].apply(safe_to_decimal)
    before = len(df)
    df = df[~df["price"].isna()]
    stats["missing_handled"] += (before - len(df))

    # Handle null stock: fill with 0
    df["stock_quantity"] = df["stock_quantity"].apply(lambda x: safe_to_int(x, default=DEFAULT_STOCK))
    # Count filled
    stats["missing_handled"] += int(df["stock_quantity"].isna().sum())
    df["stock_quantity"] = df["stock_quantity"].fillna(DEFAULT_STOCK)

    # Clean strings
    for col in ["product_name", "category"]:
        df[col] = df[col].fillna("").astype(str).str.strip()

    return df, stats


def transform_sales(df: pd.DataFrame, customers_df: pd.DataFrame, products_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    """
    Transform sales_raw into orders and order_items.
    Assumptions:
    - Each row is one item in an order.
    - order_id may be missing; we generate grouping by (customer_email, order_date) to create new order IDs.
    - unit_price missing -> fallback to product price from products_df
    - Missing customer/product or date -> drop row
    """
    stats = {"processed": len(df), "duplicates_removed": 0, "missing_handled": 0}

    df = df.rename(columns={v: k for k, v in SALES_COLUMNS.items()})

    # Determine if sales file uses (customer_email + product_name) or (customer_id + product_id)
    has_name_cols = {"customer_email", "product_name"}.issubset(df.columns)
    has_id_cols = {"customer_id", "product_id"}.issubset(df.columns)
    if not (has_name_cols or has_id_cols):
        raise KeyError(f"Sales file must contain either (customer_email & product_name) or (customer_id & product_id). Found columns: {list(df.columns)}. Update SALES_COLUMNS or CSV headers.")
    by_id = has_id_cols

    # Remove duplicates using the relevant keys
    if by_id:
        subset_cols = ["customer_id", "product_id", "order_date", "quantity", "unit_price"]
    else:
        subset_cols = ["customer_email", "product_name", "order_date", "quantity", "unit_price"]
    df, dup_removed = drop_duplicates(df, subset=subset_cols)
    stats["duplicates_removed"] += dup_removed

    # Normalize date
    df["order_date"] = df["order_date"].apply(parse_date_to_iso)
    # Drop rows with missing date
    before = len(df)
    df = df[df["order_date"].str.strip() != ""]
    stats["missing_handled"] += (before - len(df))
    logger.info(f"after date normalization: rows={len(df)} (removed {before - len(df)})")

    if by_id:
        # Determine whether IDs are numeric (true DB ids) or codes (e.g., 'C001', 'P001')
        def is_series_numeric(s):
            if s.dropna().empty:
                return False
            return s.dropna().apply(lambda x: safe_to_int(x, default=None) is not None).all()

        cust_ids_numeric = is_series_numeric(df["customer_id"]) if "customer_id" in df.columns else False
        prod_ids_numeric = is_series_numeric(df["product_id"]) if "product_id" in df.columns else False

        # If IDs are non-numeric, attempt to map codes to emails/product_names using provided raw dataframes
        if not (cust_ids_numeric and prod_ids_numeric):
            mapped = False
            if {"customer_id", "email"}.issubset(customers_df.columns):
                cust_map = customers_df.set_index("customer_id")["email"].to_dict()
                df["customer_email"] = df["customer_id"].astype(str).map(cust_map)
                mapped = True
            if {"product_id", "product_name"}.issubset(products_df.columns):
                prod_map = products_df.set_index("product_id")["product_name"].to_dict()
                df["product_name"] = df["product_id"].astype(str).map(prod_map)
                mapped = True

            if mapped:
                # Build a clean, name-based dataframe from the mapped codes (avoid in-place merge/order issues)
                prod_price_map = None
                if {"product_name", "price"}.issubset(products_df.columns):
                    prod_price_map = products_df.set_index("product_name")["price"].to_dict()

                new_rows = []
                for _, r in df.iterrows():
                    cust_email = r.get("customer_email") if pd.notna(r.get("customer_email")) else None
                    prod_name = r.get("product_name") if pd.notna(r.get("product_name")) else None
                    qty = safe_to_int(r.get("quantity"), default=None)
                    if not cust_email or not prod_name or qty is None or qty <= 0:
                        continue
                    unit = safe_to_decimal(r.get("unit_price"))
                    if unit is None and prod_price_map is not None:
                        unit = prod_price_map.get(prod_name)
                    new_rows.append({
                        "order_id": r.get("order_id"),
                        "order_date": r.get("order_date"),
                        "customer_email": cust_email,
                        "product_name": prod_name,
                        "quantity": qty,
                        "unit_price": unit,
                        "status": r.get("status", "")
                    })

                df = pd.DataFrame(new_rows)
                logger.info(f"after mapping to name-based rows: rows={len(df)}; preview:\n{df.to_string(index=False)}")
                # Update missing_handled: count how many were filtered out
                stats["missing_handled"] += (stats.get("processed", 0) - len(df))
                by_id = False
            else:
                # No mapping available; proceed treating IDs as numeric (this will drop non-numeric rows)
                pass

        if by_id:
            # IDs are numeric; ensure numeric and present
            df["customer_id"] = df["customer_id"].apply(lambda x: safe_to_int(x, default=None))
            df["product_id"] = df["product_id"].apply(lambda x: safe_to_int(x, default=None))
            before = len(df)
            df = df[~df["customer_id"].isna() & ~df["product_id"].isna()]
            stats["missing_handled"] += (before - len(df))
            logger.info(f"after numeric id presence filter: rows={len(df)} (removed {before - len(df)})")

            # Quantity
            df["quantity"] = df["quantity"].apply(lambda x: safe_to_int(x, default=None))
            before = len(df)
            df = df[~df["quantity"].isna() & (df["quantity"] > 0)]
            stats["missing_handled"] += (before - len(df))
            logger.info(f"after quantity filter (id-path): rows={len(df)} (removed {before - len(df)})")

            # Attach product price by product_id when unit_price missing
            if {"product_id", "price"}.issubset(products_df.columns):
                products_lookup = products_df[["product_id", "price"]].copy()
                products_lookup.rename(columns={"price": "product_price"}, inplace=True)
            else:
                products_lookup = pd.DataFrame(columns=["product_id", "product_price"])
            df = df.merge(products_lookup, on="product_id", how="left")

            # unit_price
            df["unit_price"] = df["unit_price"].apply(safe_to_decimal)
            def compute_unit_price_id(u, p):
                if u is None:
                    return p
                try:
                    return f"{float(u):.2f}"
                except Exception:
                    return p
            df["unit_price"] = [compute_unit_price_id(u, pp) for u, pp in zip(df["unit_price"], df.get("product_price", []))]
            before = len(df)
            df = df[~df["unit_price"].isna()]
            stats["missing_handled"] += (before - len(df))

    else:
        # Clean customer_email
        df["customer_email"] = df["customer_email"].fillna("").astype(str).str.strip()
        before = len(df)
        df = df[df["customer_email"] != ""]
        stats["missing_handled"] += (before - len(df))

        # Clean product_name
        df["product_name"] = df["product_name"].fillna("").astype(str).str.strip()
        before = len(df)
        df = df[df["product_name"] != ""]
        stats["missing_handled"] += (before - len(df))

        # Quantity and unit_price
        df["quantity"] = df["quantity"].apply(lambda x: safe_to_int(x, default=None))
        before = len(df)
        df = df[~df["quantity"].isna() & (df["quantity"] > 0)]
        stats["missing_handled"] += (before - len(df))

        # Attach product prices where unit_price missing
        if {"product_name", "price"}.issubset(products_df.columns):
            products_lookup = products_df[["product_name", "price"]].copy()
            products_lookup.rename(columns={"price": "product_price"}, inplace=True)
        else:
            # No product price information available in products_df, proceed with empty lookup
            products_lookup = pd.DataFrame(columns=["product_name", "product_price"])
        df = df.merge(products_lookup, on="product_name", how="left")

        # unit_price: if missing, use product_price
        def compute_unit_price(u, p):
            if u is None:
                return p
            try:
                return f"{float(u):.2f}"
            except Exception:
                return p

        df["unit_price"] = df["unit_price"].apply(safe_to_decimal)
        df["unit_price"] = [compute_unit_price(u, pp) for u, pp in zip(df["unit_price"], df["product_price"])]
        # Drop rows still missing unit_price
        before = len(df)
        df = df[~df["unit_price"].isna()]
        stats["missing_handled"] += (before - len(df))
        logger.info(f"after dropping missing unit_price (name-path): rows={len(df)} (removed {before - len(df)})")
        logger.info(f"after dropping missing unit_price (id-path): rows={len(df)} (removed {before - len(df)})")

    # Ensure unit_price is normalized and drop any rows still missing a usable unit_price
    def normalize_price(p):
        try:
            return f"{float(p):.2f}"
        except Exception:
            return None
    df["unit_price"] = df["unit_price"].apply(normalize_price)
    before = len(df)
    df = df[~df["unit_price"].isna()]
    stats["missing_handled"] += (before - len(df))

    # Compute subtotal
    df["subtotal"] = [f"{float(q) * float(up):.2f}" for q, up in zip(df["quantity"], df["unit_price"])]

    # Prepare orders dataframe and generate synthetic order key
    logger.info(f"transform_sales pre-order-key df columns: {list(df.columns)}; preview:\n{df[[c for c in ['order_id','order_date','customer_id','customer_email','product_id','product_name','unit_price','subtotal'] if c in df.columns]].to_string(index=False)}")
    if "order_id" not in df.columns or df["order_id"].isna().all():
        if by_id:
            df["order_key"] = df["customer_id"].astype(str) + "|" + df["order_date"]
        else:
            df["order_key"] = df["customer_email"] + "|" + df["order_date"]
    else:
        # Normalize order_id strings
        df["order_id"] = df["order_id"].fillna("").astype(str).str.strip()
        # If missing, fallback to composite key
        if by_id:
            df["order_key"] = df.apply(
                lambda r: r["order_id"] if r["order_id"] != "" else (str(r["customer_id"]) + "|" + r["order_date"]),
                axis=1
            )
        else:
            df["order_key"] = df.apply(
                lambda r: r["order_id"] if r["order_id"] != "" else (r["customer_email"] + "|" + r["order_date"]),
                axis=1
            )

    # Ensure status column exists (may be missing in some sales files)
    if "status" not in df.columns:
        df["status"] = ""

    # Build orders table: one row per order_key
    if by_id:
        orders_df = (
            df.groupby("order_key")
              .agg({
                  "customer_id": "first",
                  "order_date": "first",
                  "subtotal": lambda x: f"{sum(float(v) for v in x):.2f}",
                  "status": "first"
              })
              .reset_index(drop=False)
        )
    else:
        orders_df = (
            df.groupby("order_key")
              .agg({
                  "customer_email": "first",
                  "order_date": "first",
                  "subtotal": lambda x: f"{sum(float(v) for v in x):.2f}",
                  "status": "first"
              })
              .reset_index(drop=False)
        )

    # Default status if missing
    orders_df["status"] = orders_df["status"].fillna("").astype(str).str.strip()
    missing_status = orders_df["status"].eq("").sum()
    if missing_status:
        orders_df.loc[orders_df["status"] == "", "status"] = DEFAULT_ORDER_STATUS
        stats["missing_handled"] += missing_status

    # order_items from df
    if by_id:
        order_items_df = df[[
            "order_key", "product_id", "quantity", "unit_price", "subtotal"
        ]].copy()
    else:
        order_items_df = df[[
            "order_key", "product_name", "quantity", "unit_price", "subtotal"
        ]].copy()

    return orders_df, order_items_df, stats


# -----------------------
# Load
# -----------------------

def ensure_schema(engine: Engine):
    """
    Ensure target tables exist. This uses the provided schema.
    For PostgreSQL, AUTO_INCREMENT becomes SERIAL-like via identity; for MySQL it's native.
    Assumes DB 'fleximart' already exists.
    """
    # Normalize auto-increment syntax for Postgres
    db_type = "postgres" if "postgresql" in str(engine.url) else "mysql"

    if db_type == "mysql":
        customers_sql = """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT PRIMARY KEY AUTO_INCREMENT,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            phone VARCHAR(20),
            city VARCHAR(50),
            registration_date DATE
        );
        """
        products_sql = """
        CREATE TABLE IF NOT EXISTS products (
            product_id INT PRIMARY KEY AUTO_INCREMENT,
            product_name VARCHAR(100) NOT NULL,
            category VARCHAR(50) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            stock_quantity INT DEFAULT 0
        );
        """
        orders_sql = """
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT PRIMARY KEY AUTO_INCREMENT,
            customer_id INT NOT NULL,
            order_date DATE NOT NULL,
            total_amount DECIMAL(10,2) NOT NULL,
            status VARCHAR(20) DEFAULT 'Pending',
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        """
        order_items_sql = """
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id INT PRIMARY KEY AUTO_INCREMENT,
            order_id INT NOT NULL,
            product_id INT NOT NULL,
            quantity INT NOT NULL,
            unit_price DECIMAL(10,2) NOT NULL,
            subtotal DECIMAL(10,2) NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );
        """
    else:
        customers_sql = """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            phone VARCHAR(20),
            city VARCHAR(50),
            registration_date DATE
        );
        """
        products_sql = """
        CREATE TABLE IF NOT EXISTS products (
            product_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            product_name VARCHAR(100) NOT NULL,
            category VARCHAR(50) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            stock_quantity INT DEFAULT 0
        );
        """
        orders_sql = """
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            customer_id INT NOT NULL,
            order_date DATE NOT NULL,
            total_amount DECIMAL(10,2) NOT NULL,
            status VARCHAR(20) DEFAULT 'Pending',
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        """
        order_items_sql = """
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            order_id INT NOT NULL,
            product_id INT NOT NULL,
            quantity INT NOT NULL,
            unit_price DECIMAL(10,2) NOT NULL,
            subtotal DECIMAL(10,2) NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );
        """

    with engine.begin() as conn:
        conn.execute(text(customers_sql))
        conn.execute(text(products_sql))
        conn.execute(text(orders_sql))
        conn.execute(text(order_items_sql))


def load_customers(engine: Engine, customers_df: pd.DataFrame) -> Dict[str, int]:
    """
    Insert customers; return stats.
    Upsert-like behavior: skip on email conflict.
    """
    stats = {"loaded": 0}
    insert_sql = """
    INSERT INTO customers (first_name, last_name, email, phone, city, registration_date)
    VALUES (:first_name, :last_name, :email, :phone, :city, :registration_date)
    """
    # MySQL: use INSERT IGNORE or ON DUPLICATE KEY UPDATE; Postgres: ON CONFLICT
    db_type = "postgres" if "postgresql" in str(engine.url) else "mysql"
    if db_type == "mysql":
        insert_sql = """
        INSERT INTO customers (first_name, last_name, email, phone, city, registration_date)
        VALUES (:first_name, :last_name, :email, :phone, :city, :registration_date)
        ON DUPLICATE KEY UPDATE email=email
        """
    else:
        insert_sql = """
        INSERT INTO customers (first_name, last_name, email, phone, city, registration_date)
        VALUES (:first_name, :last_name, :email, :phone, :city, :registration_date)
        ON CONFLICT (email) DO NOTHING
        """

    with engine.begin() as conn:
        for _, row in customers_df.iterrows():
            try:
                conn.execute(text(insert_sql), {
                    "first_name": row["first_name"],
                    "last_name": row["last_name"],
                    "email": row["email"],
                    "phone": row["phone"] if row["phone"] != "" else None,
                    "city": row.get("city", None) if row.get("city", "") != "" else None,
                    "registration_date": row["registration_date"]
                })
                stats["loaded"] += 1
            except SQLAlchemyError as e:
                logger.warning(f"Skip customer {row['email']}: {e}")
    return stats


def load_products(engine: Engine, products_df: pd.DataFrame) -> Dict[str, int]:
    stats = {"loaded": 0}
    db_type = "postgres" if "postgresql" in str(engine.url) else "mysql"
    if db_type == "mysql":
        insert_sql = """
        INSERT INTO products (product_name, category, price, stock_quantity)
        VALUES (:product_name, :category, :price, :stock_quantity)
        """
        # MySQL doesn't enforce uniqueness here; duplicates were removed in transform.
    else:
        insert_sql = """
        INSERT INTO products (product_name, category, price, stock_quantity)
        VALUES (:product_name, :category, :price, :stock_quantity)
        """

    with engine.begin() as conn:
        for _, row in products_df.iterrows():
            try:
                conn.execute(text(insert_sql), {
                    "product_name": row["product_name"],
                    "category": row["category"],
                    "price": row["price"],
                    "stock_quantity": int(row["stock_quantity"])
                })
                stats["loaded"] += 1
            except SQLAlchemyError as e:
                logger.warning(f"Skip product {row['product_name']}: {e}")
    return stats


def resolve_customer_ids(engine: Engine, orders_df: pd.DataFrame) -> pd.DataFrame:
    """Map customer_email to customer_id from DB."""
    if "customer_email" not in orders_df.columns:
        raise RuntimeError("Cannot resolve customer IDs: orders dataframe has no 'customer_email' column. If your sales file used non-numeric customer IDs, enable mapping by providing matching 'customer_id' values in customers CSV or use email-based sales files.")
    with engine.begin() as conn:
        emails = orders_df["customer_email"].unique().tolist()
        q = text("SELECT customer_id, email FROM customers WHERE email = ANY(:emails)") \
            if "postgresql" in str(engine.url) else text(
            "SELECT customer_id, email FROM customers WHERE email IN :emails"
        )
        param = {"emails": emails} if "postgresql" in str(engine.url) else {"emails": tuple(emails)}
        res = conn.execute(q, param)
        map_rows = res.fetchall()
    email_to_id = {r[1]: r[0] for r in map_rows}
    orders_df["customer_id"] = orders_df["customer_email"].map(email_to_id)
    return orders_df


def resolve_product_ids(engine: Engine, order_items_df: pd.DataFrame) -> pd.DataFrame:
    """Map product_name to product_id from DB."""
    with engine.begin() as conn:
        names = order_items_df["product_name"].unique().tolist()
        q = text("SELECT product_id, product_name FROM products WHERE product_name = ANY(:names)") \
            if "postgresql" in str(engine.url) else text(
            "SELECT product_id, product_name FROM products WHERE product_name IN :names"
        )
        param = {"names": names} if "postgresql" in str(engine.url) else {"names": tuple(names)}
        res = conn.execute(q, param)
        map_rows = res.fetchall()
    name_to_id = {r[1]: r[0] for r in map_rows}
    order_items_df["product_id"] = order_items_df["product_name"].map(name_to_id)
    return order_items_df


def load_orders_and_items(engine: Engine, orders_df: pd.DataFrame, order_items_df: pd.DataFrame) -> Dict[str, int]:
    """
    Load orders and order_items after resolving customer_id and product_id.
    Generates order_id via DB auto-increment; then maps order_items to that order_id through order_key.
    """
    stats = {"orders_loaded": 0, "order_items_loaded": 0, "missing_handled": 0}

    # Resolve customer_id if not already present (support both email-based and id-based sales files)
    if "customer_id" not in orders_df.columns or orders_df["customer_id"].isna().all():
        orders_df = resolve_customer_ids(engine, orders_df)
        before = len(orders_df)
        orders_df = orders_df[~orders_df["customer_id"].isna()]
        stats["missing_handled"] += (before - len(orders_df))
    else:
        # Ensure numeric customer_id and drop invalid
        orders_df["customer_id"] = orders_df["customer_id"].apply(lambda x: safe_to_int(x, default=None))
        before = len(orders_df)
        orders_df = orders_df[~orders_df["customer_id"].isna()]
        stats["missing_handled"] += (before - len(orders_df))

    # Insert orders, capture generated order_id keyed by order_key
    insert_order_sql = """
    INSERT INTO orders (customer_id, order_date, total_amount, status)
    VALUES (:customer_id, :order_date, :total_amount, :status)
    """
    # Retrieve last inserted id per row: DB-agnostic approach uses RETURNING for Postgres
    returning = "postgresql" in str(engine.url)

    order_key_to_id = {}

    with engine.begin() as conn:
        for _, row in orders_df.iterrows():
            try:
                if returning:
                    res = conn.execute(
                        text(insert_order_sql + " RETURNING order_id"),
                        {
                            "customer_id": int(row["customer_id"]),
                            "order_date": row["order_date"],
                            "total_amount": row["subtotal"],
                            "status": row["status"] if row["status"] else DEFAULT_ORDER_STATUS
                        }
                    )
                    new_id = res.scalar()
                else:
                    conn.execute(
                        text(insert_order_sql),
                        {
                            "customer_id": int(row["customer_id"]),
                            "order_date": row["order_date"],
                            "total_amount": row["subtotal"],
                            "status": row["status"] if row["status"] else DEFAULT_ORDER_STATUS
                        }
                    )
                    # MySQL: fetch last insert id
                    new_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()

                order_key_to_id[row["order_key"]] = new_id
                stats["orders_loaded"] += 1
            except SQLAlchemyError as e:
                logger.warning(f"Skip order {row['order_key']}: {e}")

    # Resolve product_id when needed (support name-based or id-based order items)
    if "product_id" not in order_items_df.columns or order_items_df["product_id"].isna().all():
        order_items_df = resolve_product_ids(engine, order_items_df)
        before = len(order_items_df)
        order_items_df = order_items_df[~order_items_df["product_id"].isna()]
        stats["missing_handled"] += (before - len(order_items_df))
    else:
        order_items_df["product_id"] = order_items_df["product_id"].apply(lambda x: safe_to_int(x, default=None))
        before = len(order_items_df)
        order_items_df = order_items_df[~order_items_df["product_id"].isna()]
        stats["missing_handled"] += (before - len(order_items_df))

    # Attach order_id via order_key
    order_items_df["order_id"] = order_items_df["order_key"].map(order_key_to_id)
    before = len(order_items_df)
    order_items_df = order_items_df[~order_items_df["order_id"].isna()]
    stats["missing_handled"] += (before - len(order_items_df))

    # Insert order_items
    insert_item_sql = """
    INSERT INTO order_items (order_id, product_id, quantity, unit_price, subtotal)
    VALUES (:order_id, :product_id, :quantity, :unit_price, :subtotal)
    """
    with engine.begin() as conn:
        for _, row in order_items_df.iterrows():
            try:
                conn.execute(text(insert_item_sql), {
                    "order_id": int(row["order_id"]),
                    "product_id": int(row["product_id"]),
                    "quantity": int(row["quantity"]),
                    "unit_price": row["unit_price"],
                    "subtotal": row["subtotal"],
                })
                stats["order_items_loaded"] += 1
            except SQLAlchemyError as e:
                logger.warning(f"Skip order_item ({row['order_key']}, {row['product_name']}): {e}")

    return stats


# -----------------------
# Report
# -----------------------

def write_quality_report(path: str, customers_stats: Dict[str, int], products_stats: Dict[str, int],
                         sales_stats: Dict[str, int], load_customers_stats: Dict[str, int],
                         load_products_stats: Dict[str, int], load_sales_stats: Dict[str, int]) -> None:
    lines = []
    lines.append("Data Quality Report")
    lines.append("-------------------")
    lines.append(f"Customers processed: {customers_stats.get('processed', 0)}")
    lines.append(f"Customers duplicates removed: {customers_stats.get('duplicates_removed', 0)}")
    lines.append(f"Customers missing values handled: {customers_stats.get('missing_handled', 0)}")
    lines.append(f"Customers loaded: {load_customers_stats.get('loaded', 0)}")
    lines.append("")
    lines.append(f"Products processed: {products_stats.get('processed', 0)}")
    lines.append(f"Products duplicates removed: {products_stats.get('duplicates_removed', 0)}")
    lines.append(f"Products missing values handled: {products_stats.get('missing_handled', 0)}")
    lines.append(f"Products loaded: {load_products_stats.get('loaded', 0)}")
    lines.append("")
    lines.append(f"Sales processed: {sales_stats.get('processed', 0)}")
    lines.append(f"Sales duplicates removed: {sales_stats.get('duplicates_removed', 0)}")
    lines.append(f"Sales missing values handled: {sales_stats.get('missing_handled', 0)}")
    lines.append(f"Orders loaded: {load_sales_stats.get('orders_loaded', 0)}")
    lines.append(f"Order items loaded: {load_sales_stats.get('order_items_loaded', 0)}")

    with open("data_quality_report.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logger.info("Wrote data_quality_report.txt")


# -----------------------
# Main
# -----------------------

def main():
    check_runtime_deps()
    try:
        # Extract
        dfs = extract()

        # Transform customers
        customers_clean, customers_stats = transform_customers(dfs["customers"])
        # Transform products
        products_clean, products_stats = transform_products(dfs["products"])
        # Transform sales into orders and order_items
        orders_df, order_items_df, sales_stats = transform_sales(dfs["sales"], customers_clean, products_clean)

        # Load
        engine = get_db_engine()
        ensure_schema(engine)

        load_customers_stats = load_customers(engine, customers_clean)
        load_products_stats = load_products(engine, products_clean)
        load_sales_stats = load_orders_and_items(engine, orders_df, order_items_df)

        # Report
        write_quality_report(
            "data_quality_report.txt",
            customers_stats,
            products_stats,
            sales_stats,
            load_customers_stats,
            load_products_stats,
            load_sales_stats
        )

        logger.info("ETL pipeline completed successfully.")
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
