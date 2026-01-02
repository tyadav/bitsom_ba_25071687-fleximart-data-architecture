-- Database: fleximart_dw
USE fleximart_dw;

-------------------------------------------------
-- DIM_DATE: 30 dates (Jan–Feb 2024, weekdays + weekends)
-------------------------------------------------
INSERT INTO dim_date (date_key, full_date, day_of_week, day_of_month, month, month_name, quarter, year, is_weekend)
VALUES
(20240101, '2024-01-01', 'Monday', 1, 1, 'January', 'Q1', 2024, FALSE),
(20240102, '2024-01-02', 'Tuesday', 2, 1, 'January', 'Q1', 2024, FALSE),
(20240103, '2024-01-03', 'Wednesday', 3, 1, 'January', 'Q1', 2024, FALSE),
(20240104, '2024-01-04', 'Thursday', 4, 1, 'January', 'Q1', 2024, FALSE),
(20240105, '2024-01-05', 'Friday', 5, 1, 'January', 'Q1', 2024, FALSE),
(20240106, '2024-01-06', 'Saturday', 6, 1, 'January', 'Q1', 2024, TRUE),
(20240107, '2024-01-07', 'Sunday', 7, 1, 'January', 'Q1', 2024, TRUE),
(20240108, '2024-01-08', 'Monday', 8, 1, 'January', 'Q1', 2024, FALSE),
(20240109, '2024-01-09', 'Tuesday', 9, 1, 'January', 'Q1', 2024, FALSE),
(20240110, '2024-01-10', 'Wednesday', 10, 1, 'January', 'Q1', 2024, FALSE),
(20240111, '2024-01-11', 'Thursday', 11, 1, 'January', 'Q1', 2024, FALSE),
(20240112, '2024-01-12', 'Friday', 12, 1, 'January', 'Q1', 2024, FALSE),
(20240113, '2024-01-13', 'Saturday', 13, 1, 'January', 'Q1', 2024, TRUE),
(20240114, '2024-01-14', 'Sunday', 14, 1, 'January', 'Q1', 2024, TRUE),
(20240115, '2024-01-15', 'Monday', 15, 1, 'January', 'Q1', 2024, FALSE),
(20240120, '2024-01-20', 'Saturday', 20, 1, 'January', 'Q1', 2024, TRUE),
(20240121, '2024-01-21', 'Sunday', 21, 1, 'January', 'Q1', 2024, TRUE),
(20240125, '2024-01-25', 'Thursday', 25, 1, 'January', 'Q1', 2024, FALSE),
(20240131, '2024-01-31', 'Wednesday', 31, 1, 'January', 'Q1', 2024, FALSE),
(20240201, '2024-02-01', 'Thursday', 1, 2, 'February', 'Q1', 2024, FALSE),
(20240202, '2024-02-02', 'Friday', 2, 2, 'February', 'Q1', 2024, FALSE),
(20240203, '2024-02-03', 'Saturday', 3, 2, 'February', 'Q1', 2024, TRUE),
(20240204, '2024-02-04', 'Sunday', 4, 2, 'February', 'Q1', 2024, TRUE),
(20240205, '2024-02-05', 'Monday', 5, 2, 'February', 'Q1', 2024, FALSE),
(20240210, '2024-02-10', 'Saturday', 10, 2, 'February', 'Q1', 2024, TRUE),
(20240211, '2024-02-11', 'Sunday', 11, 2, 'February', 'Q1', 2024, TRUE),
(20240214, '2024-02-14', 'Wednesday', 14, 2, 'February', 'Q1', 2024, FALSE),
(20240218, '2024-02-18', 'Sunday', 18, 2, 'February', 'Q1', 2024, TRUE),
(20240225, '2024-02-25', 'Sunday', 25, 2, 'February', 'Q1', 2024, TRUE),
(20240228, '2024-02-28', 'Wednesday', 28, 2, 'February', 'Q1', 2024, FALSE);

-------------------------------------------------
-- DIM_PRODUCT: 15 products across 3 categories
-------------------------------------------------
INSERT INTO dim_product (product_id, product_name, category, subcategory, unit_price)
VALUES
('P001', 'Laptop', 'Electronics', 'Computers', 50000.00),
('P002', 'Smartphone', 'Electronics', 'Mobiles', 30000.00),
('P003', 'Headphones', 'Electronics', 'Audio', 2000.00),
('P004', 'Refrigerator', 'Appliances', 'Kitchen', 25000.00),
('P005', 'Microwave Oven', 'Appliances', 'Kitchen', 8000.00),
('P006', 'Washing Machine', 'Appliances', 'Laundry', 18000.00),
('P007', 'T-Shirt', 'Fashion', 'Clothing', 500.00),
('P008', 'Jeans', 'Fashion', 'Clothing', 1500.00),
('P009', 'Sneakers', 'Fashion', 'Footwear', 3000.00),
('P010', 'Smartwatch', 'Electronics', 'Wearables', 12000.00),
('P011', 'Tablet', 'Electronics', 'Computers', 20000.00),
('P012', 'Air Conditioner', 'Appliances', 'Cooling', 35000.00),
('P013', 'Dress', 'Fashion', 'Clothing', 2500.00),
('P014', 'Formal Shoes', 'Fashion', 'Footwear', 4000.00),
('P015', 'Bluetooth Speaker', 'Electronics', 'Audio', 5000.00);

-------------------------------------------------
-- DIM_CUSTOMER: 12 customers across 4 cities
-------------------------------------------------
INSERT INTO dim_customer (customer_id, customer_name, city, state, customer_segment)
VALUES
('C001', 'John Doe', 'Mumbai', 'Maharashtra', 'Retail'),
('C002', 'Priya Sharma', 'Delhi', 'Delhi', 'Retail'),
('C003', 'Amit Patel', 'Ahmedabad', 'Gujarat', 'Wholesale'),
('C004', 'Sara Khan', 'Bengaluru', 'Karnataka', 'Retail'),
('C005', 'Ravi Iyer', 'Mumbai', 'Maharashtra', 'Wholesale'),
('C006', 'Neha Gupta', 'Delhi', 'Delhi', 'Retail'),
('C007', 'Arjun Mehta', 'Ahmedabad', 'Gujarat', 'Retail'),
('C008', 'Kiran Rao', 'Bengaluru', 'Karnataka', 'Wholesale'),
('C009', 'Meena Joshi', 'Mumbai', 'Maharashtra', 'Retail'),
('C010', 'Vikram Singh', 'Delhi', 'Delhi', 'Wholesale'),
('C011', 'Anita Desai', 'Ahmedabad', 'Gujarat', 'Retail'),
('C012', 'Rahul Verma', 'Bengaluru', 'Karnataka', 'Retail');

-------------------------------------------------
-- FACT_SALES: 40 transactions (realistic patterns)
-------------------------------------------------
INSERT INTO fact_sales (date_key, product_key, customer_key, quantity_sold, unit_price, discount_amount, total_amount)
VALUES
-- Week 1 (Jan 1–7)
(20240101, 1, 1, 2, 50000.00, 0, 100000.00),
(20240102, 2, 2, 1, 30000.00, 2000.00, 28000.00),
(20240103, 3, 3, 3, 2000.00, 0, 6000.00),
(20240104, 4, 4, 1, 25000.00, 2500.00, 22500.00),
(20240105, 5, 5, 2, 8000.00, 800.00, 15200.00),
(20240106, 7, 6, 5, 500.00, 0, 2500.00),
(20240107, 8, 7, 3, 1500.00, 0, 4500.00),

-- Week 2 (Jan 8–14)
(20240108, 9, 8, 1, 3000.00, 300.00, 2700.00),
(20240109, 10, 9, 2, 12000.00, 1000.00, 23000.00),
(20240110, 11, 10, 1, 20000.00, 0, 20000.00),
(20240111, 12, 11, 1, 35000.00, 3500.00, 31500.00),
(20240112, 13, 12, 2, 2500.00, 0, 5000.00),
(20240113, 14, 1, 1, 4000.00, 400.00, 3600.00),
(20240114, 15, 2, 3, 5000.00, 0, 15000.00),

-- Week 3 (Jan 15–21)
(20240115, 2, 3, 2, 30000.00, 0, 60000.00),
(20240120, 3, 4, 4, 2000.00, 0, 8000.00),
(20240121, 4, 5, 1, 25000.00, 2500.00, 22500.00),

-- Week 4 (Jan 25–31)
(20240125, 5, 6, 3, 8000.00, 800.00, 22400.00),
(20240131, 6, 7, 2, 18000.00, 1800.00, 34200.00),

-- Week 5 (Feb 1–4)
(20240201, 7, 8, 6, 500.00, 0, 3000.00),
(20240202, 8, 9, 2, 1500.00, 0, 3000.00),
(20240203, 9, 10, 1, 3000.00, 300.00, 2700.00),
(20240204, 10, 11, 2, 12000.00, 1200.00, 22800.00),

-- Week 6 (Feb 5–11)
(20240205, 11, 12, 1, 20000.00, 0, 20000.00),
(20240210, 12, 1, 1, 35000.00, 3500.00, 31500.00),
(20240211, 13, 2, 2, 2500.00, 0, 5000.00),

-- Week 7 (Feb 14–18)
(20240214, 14, 3, 1, 4000.00, 400.00, 3600.00),
(20240218, 15, 4, 3, 5000.00, 0, 15000.00),

-- Week 8 (Feb 25–28)
(20240225, 1, 5, 1, 50000.00, 5000.00, 45000.00),
(20240228, 2, 6, 2, 30000.00, 0, 60000.00),
(20240109, 3, 7, 5, 2000.00, 0, 10000.00),
(20240112, 4, 8, 2, 25000.00, 2500.00, 47500.00),
(20240120, 5, 9, 1, 8000.00, 0, 8000.00),
(20240131, 6, 10, 3, 18000.00, 1800.00, 52200.00),
(20240201, 7, 11, 4, 500.00, 0, 2000.00),
(20240203, 8, 12, 2, 1500.00, 0, 3000.00),
(20240204, 9, 1, 1, 3000.00, 300.00, 2700.00),
(20240210, 10, 2, 2, 12000.00, 1200.00, 22800.00),
(20240211, 11, 3, 1, 20000.00, 0, 20000.00),
(20240214, 12, 4, 1, 35000.00, 3500.00, 31500.00),
(20240218, 13, 5, 2, 2500.00, 0, 5000.00),
(20240225, 14, 6, 1, 4000.00, 400.00, 3600.00),
(20240228, 15, 7, 3, 5000.00, 0, 15000.00);
