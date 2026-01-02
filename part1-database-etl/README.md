# Part 1: Database ETL Pipeline

## 📌 Overview
This part builds the foundation of FlexiMart’s relational database using raw CSV files. It demonstrates a complete ETL workflow: extracting messy data, transforming it into clean formats, and loading it into normalized MySQL tables.

---

## 🔧 Components
- `etl_pipeline.py` → Python script to clean and load customer, product, and sales data.  
- `schema_documentation.md` → Describes relational schema and table relationships.  
- `business_queries.sql` → SQL queries for customer segmentation, top products, and city-wise revenue.  
- `data_quality_report.txt` → Generated report summarizing missing values, duplicates, and validation checks.  
- `requirements.txt` → Python dependencies for running the ETL pipeline.  

---

## 📥 Input Files
Located in `/data/`:
- `customers_raw.csv`  
- `products_raw.csv`  
- `sales_raw.csv`  

---

## 🧪 How to Run
```bash
# Install dependencies
pip install -r requirements.txt

# Run ETL pipeline
python etl_pipeline.py
