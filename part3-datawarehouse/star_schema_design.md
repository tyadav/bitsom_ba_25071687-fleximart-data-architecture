# Star Schema Design Documentation

## Section 1: Schema Overview

### Fact Table: fact_sales
- **Grain:** One row per product per order line item  
- **Business Process:** Sales transactions  
- **Measures (Numeric Facts):**
  - **quantity_sold:** Number of units sold  
  - **unit_price:** Price per unit at time of sale  
  - **discount_amount:** Discount applied  
  - **total_amount:** Final amount (quantity × unit_price − discount)  
- **Foreign Keys:**
  - **date_key** → dim_date  
  - **product_key** → dim_product  
  - **customer_key** → dim_customer  

### Dimension Table: dim_date
- **Purpose:** Date dimension for time‑based analysis  
- **Type:** Conformed dimension  
- **Attributes:**
  - **date_key (PK):** Surrogate key (integer, format YYYYMMDD)  
  - **full_date:** Actual date  
  - **day_of_week:** Monday, Tuesday, etc.  
  - **month:** 1–12  
  - **month_name:** January, February, etc.  
  - **quarter:** Q1, Q2, Q3, Q4  
  - **year:** 2023, 2024, etc.  
  - **is_weekend:** Boolean  

### Dimension Table: dim_product
- **Purpose:** Product dimension for category and attribute analysis  
- **Attributes:**
  - **product_key (PK):** Surrogate key  
  - **product_name:** Name of product  
  - **category:** Product category (e.g., Electronics, Fashion)  
  - **brand:** Brand name  
  - **price_band:** Derived attribute (e.g., Low, Medium, High)  

### Dimension Table: dim_customer
- **Purpose:** Customer dimension for demographic and geographic analysis  
- **Attributes:**
  - **customer_key (PK):** Surrogate key  
  - **customer_name:** Full name  
  - **email:** Contact email  
  - **city:** City of residence  
  - **region:** Region/State  
  - **registration_date:** Date customer joined  

---

## Section 2: Design Decisions

The chosen granularity is **transaction line‑item level**, meaning each row in the fact table represents a single product sold in an order. This level of detail ensures maximum flexibility: analysts can aggregate sales by customer, product, category, or time period without losing precision. **Surrogate keys** are used instead of natural keys (like product IDs or customer emails) to maintain consistency, avoid duplication, and support slowly changing dimensions. Surrogate keys also decouple the warehouse from operational system changes, ensuring stability over time.  

This design supports **drill‑down and roll‑up operations** naturally. Analysts can roll up from daily sales to monthly or yearly trends using `dim_date`, or drill down from category to individual product performance using `dim_product`. Similarly, customer analysis can move from city to region to national level. The star schema’s simplicity and denormalized structure make queries efficient and intuitive for business intelligence tools.

---

## Section 3: Sample Data Flow

**Source Transaction:**  
Order #101, Customer "John Doe", Product "Laptop", Qty: 2, Price: 50000  

**Becomes in Data Warehouse:**  

- **fact_sales:**  
  ```json
  {
    "date_key": 20240115,
    "product_key": 5,
    "customer_key": 12,
    "quantity_sold": 2,
    "unit_price": 50000,
    "total_amount": 100000
  }
