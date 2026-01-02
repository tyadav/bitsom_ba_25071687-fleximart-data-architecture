# 📁 part2-nosql/README.md

## Part 2: NoSQL Product Catalog (MongoDB)

## 📌 Overview
This part explores FlexiMart’s product catalog using MongoDB. It demonstrates how flexible schemas can support diverse product attributes and dynamic querying.

---

## 🔧 Components
- `nosql_analysis.md` → Explains schema design choices and NoSQL advantages.  
- `mongodb_operations.js` → Mongo shell script to insert and query product documents.  
- `products_catalog.json` → Sample product data with nested attributes and categories.  

---

## 🧪 How to Run
```bash
# Load product catalog into MongoDB
mongosh < mongodb_operations.py
