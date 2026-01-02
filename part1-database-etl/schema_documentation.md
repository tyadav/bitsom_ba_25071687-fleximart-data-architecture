# Task 1.2: Database Schema Documentation

## Entity-Relationship Description (Text Format)

This schema models FlexiMart's transactional system with four core entities: `customers`, `products`, `orders`, and `order_items`.

ENTITY: customers
Purpose: Stores customer information.
Attributes:
- customer_id: Unique identifier (Primary Key)
- first_name: Customer's first name
- last_name: Customer's last name
- email: Customer email (unique contact)
- phone: Contact phone number
- city: City of residence
- registration_date: Date the customer registered (YYYY-MM-DD)

Relationships:
- One customer can place MANY orders (1:M with `orders`).


ENTITY: products
Purpose: Product catalog and pricing.
Attributes:
- product_id: Unique identifier (Primary Key)
- product_name: Human-readable product name
- category: Product category (normalized to Title case)
- price: Unit price (DECIMAL)
- stock_quantity: Current stock level (INT)

Relationships:
- One product may appear in MANY order_items (1:M with `order_items`).


ENTITY: orders
Purpose: Represents a customer order (header-level information).
Attributes:
- order_id: Unique identifier (Primary Key)
- customer_id: FK -> `customers.customer_id`
- order_date: Date of the order (YYYY-MM-DD)
- total_amount: Order total (DECIMAL)
- status: Order status (e.g., Pending, Completed)

Relationships:
- One order contains MANY order_items (1:M with `order_items`).
- Each order belongs to ONE customer (M:1 with `customers`).


ENTITY: order_items
Purpose: Line items of an order.
Attributes:
- order_item_id: Unique identifier (Primary Key)
- order_id: FK -> `orders.order_id`
- product_id: FK -> `products.product_id`
- quantity: Quantity of product ordered
- unit_price: Price per unit at order time
- subtotal: Quantity * unit_price

Relationships:
- Each order_item references one `order` and one `product` (many-to-one).

---

## Normalization Explanation (3NF justification)

This design adheres to Third Normal Form (3NF). Primary keys uniquely identify rows: `customer_id`, `product_id`, `order_id`, and `order_item_id`. All non-key attributes are fully functionally dependent on their table’s primary key (no partial dependencies exist because composite keys are not used in these base tables). Additionally, there are no transitive dependencies: each non-key attribute depends directly on the primary key rather than on another non-key attribute. For example, in `orders` the `total_amount` depends on `order_id`, not on `status` or `order_date`; in `products`, `price` and `category` depend on `product_id` and not on other non-key fields.

Functional dependencies include:
- customers: customer_id -> {first_name, last_name, email, phone, city, registration_date}
- products: product_id -> {product_name, category, price, stock_quantity}
- orders: order_id -> {customer_id, order_date, total_amount, status}
- order_items: order_item_id -> {order_id, product_id, quantity, unit_price, subtotal}

Because each piece of information has a single canonical location, the design avoids update anomalies: changing a product price is done in `products` and affects order histories only when intentionally recorded in `order_items` (unit_price is stored at order time if required). Insert anomalies are avoided because new products or customers can be inserted independently of orders; order_items require existing product and order references, enforced by foreign keys. Delete anomalies are mitigated through referential constraints (deleting an order will not remove product or customer rows), and cascading rules can be applied as policy when appropriate.

(Word count ≈ 210 words)

---

## Sample Data Representation

### customers
| customer_id | first_name | last_name | email | phone | city | registration_date |
|-------------|------------|-----------|------------------------|---------------|-----------|-------------------|
| C001 | Rahul | Sharma | rahul.sharma@gmail.com | +91-9876543210 | Bangalore | 2023-01-15 |
| C002 | Priya | Patel | priya.patel@yahoo.com | +91-9988776655 | Mumbai | 2023-02-20 |

### products
| product_id | product_name | category | price | stock_quantity |
|------------|--------------|----------|--------:|----------------:|
| P001 | UltraPhone X | Electronics | 45999.00 | 10 |
| P002 | SoundBuds Pro | Accessories | 3499.00 | 50 |

### orders
| order_id | customer_id | order_date | total_amount | status |
|----------|-------------|------------|-------------:|--------|
| 1 | C001 | 2024-01-15 | 45999.00 | Completed |
| 2 | C002 | 2024-01-16 | 5998.00 | Completed |

### order_items
| order_item_id | order_id | product_id | quantity | unit_price | subtotal |
|---------------|----------|------------|---------:|-----------:|---------:|
| 1 | 1 | P001 | 1 | 45999.00 | 45999.00 |
| 2 | 2 | P002 | 2 | 2999.00 | 5998.00 |

---

## Notes
- Foreign keys maintain referential integrity between `orders.customer_id -> customers.customer_id` and `order_items.product_id -> products.product_id` / `order_items.order_id -> orders.order_id`.
- The schema favors a clear separation of concerns, supporting analytical and transactional use cases while ensuring minimal redundancy and safe updates.
