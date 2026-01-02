import pandas as pd
import pytest
from etl_pipeline import parse_date_to_iso, transform_sales



# -----------------------------
# Date Parsing Tests
# -----------------------------
def test_parse_date_to_iso_various():
    cases = {
        "2025-12-30": "2025-12-30",
        "30-12-2025": "2025-12-30",
        "12-30-2025": "2025-12-30",
        "30/12/2025": "2025-12-30",
        "2025/12/30": "2025-12-30",
        "invalid": "",
        "": "",
        None: "",
    }
    for inp, expected in cases.items():
        assert parse_date_to_iso(inp) == expected


# -----------------------------
# Sales Transformation Tests
# -----------------------------
def test_transform_sales_name_based():
    sales = pd.DataFrame({
        "transaction_id": [1, 1],
        "transaction_date": ["2025-12-30", "2025-12-30"],
        "customer_email": ["a@x.com", "a@x.com"],
        "product_name": ["X", "Y"],
        "quantity": [1, 2],
        "unit_price": ["10", None],
        "status": ["", "Delivered"]
    })
    products = pd.DataFrame({"product_name": ["X", "Y"], "price": [10.0, 7.5]})

    orders, items, stats = transform_sales(sales, pd.DataFrame(), products)
    assert isinstance(orders, pd.DataFrame)
    assert isinstance(items, pd.DataFrame)
    assert stats["processed"] == 2
    assert len(orders) == 1  # grouped into one order
    assert len(items) == 2


def test_transform_sales_id_based():
    sales = pd.DataFrame({
        "transaction_id": [10, 10],
        "transaction_date": ["2025-12-30", "2025-12-30"],
        "customer_id": [100, 100],
        "product_id": [200, 201],
        "quantity": [1, 3],
        "unit_price": [None, "5.00"],
    })
    products = pd.DataFrame({"product_id": [200, 201], "price": [12.5, 5.0]})

    orders, items, stats = transform_sales(sales, pd.DataFrame(), products)
    assert isinstance(orders, pd.DataFrame)
    assert isinstance(items, pd.DataFrame)
    assert stats["processed"] == 2
    assert len(orders) == 1
    assert len(items) == 2
    # check that unit_price was filled for product_id 200
    assert items.iloc[0]["unit_price"] in ("12.50", 12.5)


def test_transform_sales_code_based_ids():
    # Sales uses alphanumeric codes for customer_id/product_id; customers/products provide mappings
    sales = pd.DataFrame({
        "transaction_id": [1001, 1001],
        "transaction_date": ["2025-12-30", "2025-12-30"],
        "customer_id": ["C001", "C001"],
        "product_id": ["P001", "P002"],
        "quantity": [1, 2],
        "unit_price": [None, "5.00"],
    })
    customers = pd.DataFrame({"customer_id": ["C001"], "email": ["code@user.com"]})
    products = pd.DataFrame({"product_id": ["P001", "P002"], "product_name": ["X", "Y"], "price": [12.5, 5.0]})

    orders, items, stats = transform_sales(sales, customers, products)
    assert isinstance(orders, pd.DataFrame)
    assert isinstance(items, pd.DataFrame)
    assert stats["processed"] == 2
    assert len(orders) == 1
    assert len(items) == 2
    # verify unit_price filled for P001
    assert items[items["product_name"] == "X"].iloc[0]["unit_price"] in ("12.50", 12.5)


# -----------------------------
# Error Handling & Edge Cases
# -----------------------------
def test_transform_sales_missing_columns_error():
    bad_sales = pd.DataFrame({"order_date": ["2025-12-30"], "quantity": [1], "unit_price": [10]})
    with pytest.raises(KeyError):
        transform_sales(bad_sales, pd.DataFrame(), pd.DataFrame())


def test_transform_sales_empty():
    sales = pd.DataFrame()
    customers = pd.DataFrame()
    products = pd.DataFrame()
    orders, items, stats = transform_sales(sales, customers, products)
    assert len(orders) == 0
    assert len(items) == 0
    assert stats["processed"] == 0


def test_transform_sales_duplicates():
    sales = pd.DataFrame({
        "transaction_id": [1, 1],
        "transaction_date": ["2025-12-30", "2025-12-30"],
        "customer_email": ["dup@x.com", "dup@x.com"],
        "product_name": ["X", "X"],
        "quantity": [1, 1],
        "unit_price": ["10", "10"],
    })
    products = pd.DataFrame({"product_name": ["X"], "price": [10.0]})
    orders, items, stats = transform_sales(sales, pd.DataFrame(), products)
    assert len(orders) == 1
    assert len(items) == 2
    assert stats["processed"] == 2


def test_transform_sales_invalid_date():
    sales = pd.DataFrame({
        "transaction_id": [1],
        "transaction_date": ["invalid"],
        "customer_email": ["bad@x.com"],
        "product_name": ["X"],
        "quantity": [1],
        "unit_price": ["10"],
    })
    products = pd.DataFrame({"product_name": ["X"], "price": [10.0]})
    orders, items, stats = transform_sales(sales, pd.DataFrame(), products)
    assert stats["processed"] == 1
    # transaction_date should be empty or NaN
    assert orders.iloc[0]["transaction_date"] in ("", None) or pd.isna(orders.iloc[0]["transaction_date"])


def test_transform_sales_totals():
    sales = pd.DataFrame({
        "transaction_id": [1],
        "transaction_date": ["2025-12-30"],
        "customer_email": ["a@x.com"],
        "product_name": ["X"],
        "quantity": [2],
        "unit_price": ["15"],
    })
    products = pd.DataFrame({"product_name": ["X"], "price": [15.0]})
    orders, items, stats = transform_sales(sales, pd.DataFrame(), products)
    assert items.iloc[0]["quantity"] == 2
    assert float(items.iloc[0]["unit_price"]) == 15.0
    assert float(items.iloc[0]["quantity"]) * float(items.iloc[0]["unit_price"]) == 30.0
