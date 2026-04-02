CREATE TABLE IF NOT EXISTS report_products_top10 (
    product_id Int64,
    product_name String,
    product_category String,
    total_quantity Int64,
    total_revenue Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (total_quantity, product_id);
TRUNCATE TABLE report_products_top10;

CREATE TABLE IF NOT EXISTS report_products_category_revenue (
    product_category String,
    total_revenue Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (product_category);
TRUNCATE TABLE report_products_category_revenue;

CREATE TABLE IF NOT EXISTS report_products_rating_reviews (
    product_id Int64,
    product_name String,
    product_category String,
    avg_rating Decimal(10,4),
    reviews_count Int64
) ENGINE = MergeTree()
ORDER BY (product_id);
TRUNCATE TABLE report_products_rating_reviews;

CREATE TABLE IF NOT EXISTS report_customers_top10 (
    customer_id Int64,
    first_name String,
    last_name String,
    country String,
    total_spent Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (total_spent, customer_id);
TRUNCATE TABLE report_customers_top10;

CREATE TABLE IF NOT EXISTS report_customers_country_distribution (
    country String,
    customers_count Int64
) ENGINE = MergeTree()
ORDER BY (country);
TRUNCATE TABLE report_customers_country_distribution;

CREATE TABLE IF NOT EXISTS report_customers_avg_check (
    customer_id Int64,
    first_name String,
    last_name String,
    avg_check Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (customer_id);
TRUNCATE TABLE report_customers_avg_check;

CREATE TABLE IF NOT EXISTS report_time_monthly_trends (
    year Int32,
    month Int32,
    total_revenue Decimal(18,2),
    orders_count Int64
) ENGINE = MergeTree()
ORDER BY (year, month);
TRUNCATE TABLE report_time_monthly_trends;

CREATE TABLE IF NOT EXISTS report_time_period_revenue (
    year Int32,
    total_revenue Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (year);
TRUNCATE TABLE report_time_period_revenue;

CREATE TABLE IF NOT EXISTS report_time_avg_order_by_month (
    year Int32,
    month Int32,
    avg_order_value Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (year, month);
TRUNCATE TABLE report_time_avg_order_by_month;

CREATE TABLE IF NOT EXISTS report_stores_top5 (
    store_id Int64,
    store_name String,
    city String,
    country String,
    total_revenue Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (total_revenue, store_id);
TRUNCATE TABLE report_stores_top5;

CREATE TABLE IF NOT EXISTS report_stores_city_country_distribution (
    city String,
    country String,
    total_revenue Decimal(18,2),
    orders_count Int64
) ENGINE = MergeTree()
ORDER BY (country, city);
TRUNCATE TABLE report_stores_city_country_distribution;

CREATE TABLE IF NOT EXISTS report_stores_avg_check (
    store_id Int64,
    store_name String,
    avg_check Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (store_id);
TRUNCATE TABLE report_stores_avg_check;

CREATE TABLE IF NOT EXISTS report_suppliers_top5 (
    supplier_id Int64,
    supplier_name String,
    supplier_country String,
    total_revenue Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (total_revenue, supplier_id);
TRUNCATE TABLE report_suppliers_top5;

CREATE TABLE IF NOT EXISTS report_suppliers_avg_price (
    supplier_id Int64,
    supplier_name String,
    avg_product_price Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (supplier_id);
TRUNCATE TABLE report_suppliers_avg_price;

CREATE TABLE IF NOT EXISTS report_suppliers_country_distribution (
    supplier_country String,
    total_revenue Decimal(18,2)
) ENGINE = MergeTree()
ORDER BY (supplier_country);
TRUNCATE TABLE report_suppliers_country_distribution;

CREATE TABLE IF NOT EXISTS report_quality_high_low_rating (
    product_id Int64,
    product_name String,
    rating Decimal(5,2),
    rank_best Int64,
    rank_worst Int64
) ENGINE = MergeTree()
ORDER BY (rating, product_id);
TRUNCATE TABLE report_quality_high_low_rating;

CREATE TABLE IF NOT EXISTS report_quality_rating_sales_correlation (
    metric String,
    correlation_value Decimal(10,6)
) ENGINE = MergeTree()
ORDER BY (metric);
TRUNCATE TABLE report_quality_rating_sales_correlation;

CREATE TABLE IF NOT EXISTS report_quality_top_reviews (
    product_id Int64,
    product_name String,
    reviews Int32
) ENGINE = MergeTree()
ORDER BY (reviews, product_id);
TRUNCATE TABLE report_quality_top_reviews;
