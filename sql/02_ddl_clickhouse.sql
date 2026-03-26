CREATE TABLE IF NOT EXISTS report_sales_by_products (
    product_id Int64,
    product_name String,
    product_category String,
    total_revenue Decimal(18,2),
    total_quantity Int64,
    category_revenue Decimal(18,2),
    avg_rating Decimal(10,4),
    reviews_count Int64
) ENGINE = MergeTree()
ORDER BY (product_category, total_revenue, product_id);

TRUNCATE TABLE report_sales_by_products;

CREATE TABLE IF NOT EXISTS report_sales_by_customers (
    customer_id Int64,
    first_name String,
    last_name String,
    country String,
    total_spent Decimal(18,2),
    orders_count Int64,
    avg_check Decimal(18,2),
    country_customers_count Int64
) ENGINE = MergeTree()
ORDER BY (country, total_spent, customer_id);

TRUNCATE TABLE report_sales_by_customers;

CREATE TABLE IF NOT EXISTS report_sales_by_time (
    year Int32,
    month Int32,
    orders_count Int64,
    total_revenue Decimal(18,2),
    avg_order_value Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (year, month);

TRUNCATE TABLE report_sales_by_time;

CREATE TABLE IF NOT EXISTS report_sales_by_stores (
    store_id Int64,
    store_name String,
    city String,
    country String,
    orders_count Int64,
    total_revenue Decimal(18,2),
    avg_check Decimal(18,2),
    city_country_revenue Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (country, city, total_revenue, store_id);

TRUNCATE TABLE report_sales_by_stores;

CREATE TABLE IF NOT EXISTS report_sales_by_suppliers (
    supplier_id Int64,
    supplier_name String,
    supplier_country String,
    products_count Int64,
    total_revenue Decimal(18,2),
    avg_product_price Decimal(18,2),
    supplier_country_revenue Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (supplier_country, total_revenue, supplier_id);

TRUNCATE TABLE report_sales_by_suppliers;

CREATE TABLE IF NOT EXISTS report_product_quality (
    product_id Int64,
    product_name String,
    product_category String,
    rating Decimal(5,2),
    reviews Int32,
    sold_quantity Int64,
    rank_best Int64,
    rank_worst Int64,
    rating_sales_correlation Decimal(10,6)
) ENGINE = MergeTree()
ORDER BY (rating, product_id);

TRUNCATE TABLE report_product_quality;
