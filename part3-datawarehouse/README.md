# Part 3: Data Warehouse & OLAP Analytics

## 📌 Overview
This part builds a star-schema data warehouse for FlexiMart and runs OLAP-style queries to support business analysis.

---

## 🔧 Components
- `star_schema_design.md` → Describes warehouse schema and dimensional modeling.  
- `warehouse_schema.sql` → SQL script to create fact and dimension tables in `fleximart_dw`.  
- `warehouse_data.sql` → Inserts 30 dates, 15 products, 12 customers, and 40 sales transactions.  
- `analytics_queries.sql` → OLAP queries for monthly drill-down, product performance, and customer segmentation.  

---

## 🧪 How to Run
```bash
# Create warehouse schema
mysql -u root -p fleximart_dw < warehouse_schema.sql

# Load warehouse data
mysql -u root -p fleximart_dw < warehouse_data.sql

# Run OLAP queries
mysql -u root -p fleximart_dw < analytics_queries.sql
