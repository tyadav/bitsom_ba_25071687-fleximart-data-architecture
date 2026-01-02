-- FlexiMart Data Warehouse Schema
-- Student: Tej Yadav
-- Date: Jan 2nd, 2026

-- Drop tables if they already exist (for resets)
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_customers;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_date;

-- ============================
-- Dimension Tables
-- ============================

-- Customers Dimension
CREATE TABLE dim_customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_name VARCHAR(100) NOT NULL,
    city VARCHAR(100),
    segment VARCHAR(50)   -- e.g., High Value, Medium Value, Low Value
);

-- Products Dimension
CREATE TABLE dim_products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),     -- e.g., Electronics, Fashion, Appliances
    price DECIMAL(10,2) NOT NULL
);

-- Date Dimension
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY AUTO_INCREMENT,
    full_date DATE NOT NULL,
    year INT,
    quarter VARCHAR(2),
    month_name VARCHAR(20)
);

-- ============================
-- Fact Table
-- ============================

CREATE TABLE fact_sales (
    sales_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    date_id INT NOT NULL,
    quantity INT NOT NULL,
    total_sales DECIMAL(12,2) NOT NULL,

    -- Foreign Keys
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);
