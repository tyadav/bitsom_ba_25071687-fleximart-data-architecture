import pytest

def test_warehouse_schema_contains_tables():
    with open("part3-datawarehouse/warehouse_schema.sql") as f:
        sql = f.read().lower()
    assert "create table dim_customers" in sql
    assert "create table dim_products" in sql
    assert "create table dim_date" in sql
    assert "create table fact_sales" in sql

def test_analytics_queries_file_exists():
    with open("part3-datawarehouse/analytics_queries.sql") as f:
        content = f.read()
    assert "select" in content.lower()
