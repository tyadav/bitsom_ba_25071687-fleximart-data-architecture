import json
import pytest

def test_products_catalog_json_loads():
    with open("part2-nosql/products_catalog.json") as f:
        data = json.load(f)
    assert isinstance(data, list)
    assert len(data) > 0
    assert "product_id" in data[0]
    assert "product_name" in data[0]

def test_mongodb_operations_file_exists():
    # Just check the file is present and readable
    with open("part2-nosql/mongodb_operations.js") as f:
        content = f.read()
    assert "db." in content  # basic sanity check
