"""
mongodb_operations.py
Practical MongoDB tasks for FlexiMart product catalog
Author: Tej Yadav
"""

from pymongo import MongoClient
from datetime import datetime

# Connect to local MongoDB (adjust URI if needed)
client = MongoClient("mongodb://localhost:27017/")
db = client["fleximart"]
products = db["products"]

# -------------------------------
# Operation 1: Load Data (1 mark)
# -------------------------------
# Import JSON file into 'products' collection (run in terminal, not Python):
# mongoimport --db fleximart --collection products --file products_catalog.json --jsonArray

# -------------------------------
# Operation 2: Basic Query (2 marks)
# -------------------------------
print("Operation 2: Electronics under 50000")
electronics = products.find(
    {"category": "Electronics", "price": {"$lt": 50000}},
    {"_id": 0, "name": 1, "price": 1, "stock": 1}
)
for doc in electronics:
    print(doc)

# -------------------------------
# Operation 3: Review Analysis (2 marks)
# -------------------------------
print("\nOperation 3: Products with avg rating >= 4.0")
high_rated = products.aggregate([
    {"$unwind": "$reviews"},
    {"$group": {
        "_id": "$product_id",
        "name": {"$first": "$name"},
        "avg_rating": {"$avg": "$reviews.rating"}
    }},
    {"$match": {"avg_rating": {"$gte": 4.0}}}
])
for doc in high_rated:
    print(doc)

# -------------------------------
# Operation 4: Update Operation (2 marks)
# -------------------------------
print("\nOperation 4: Add new review to ELEC001")
result = products.update_one(
    {"product_id": "ELEC001"},
    {"$push": {
        "reviews": {
            "user": "U999",
            "rating": 4,
            "comment": "Good value",
            "date": datetime.utcnow()
        }
    }}
)
print(f"Matched: {result.matched_count}, Modified: {result.modified_count}")

# -------------------------------
# Operation 5: Complex Aggregation (3 marks)
# -------------------------------
print("\nOperation 5: Average price by category")
avg_price_by_category = products.aggregate([
    {"$group": {
        "_id": "$category",
        "avg_price": {"$avg": "$price"},
        "product_count": {"$sum": 1}
    }},
    {"$project": {
        "_id": 0,
        "category": "$_id",
        "avg_price": 1,
        "product_count": 1
    }},
    {"$sort": {"avg_price": -1}}
])
for doc in avg_price_by_category:
    print(doc)

client.close()
