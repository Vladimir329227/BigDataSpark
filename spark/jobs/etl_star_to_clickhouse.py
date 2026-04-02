import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, corr, dense_rank, desc, lit, sum as sum_
from pyspark.sql.window import Window


def create_clickhouse_tables(
    spark: SparkSession, ch_url: str, ch_user: str, ch_password: str, ddl_path: str
):
    with open(ddl_path, "r", encoding="utf-8") as f:
        ddl = f.read()
    spark._sc._jvm.java.lang.Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    conn = spark._sc._jvm.java.sql.DriverManager.getConnection(ch_url, ch_user, ch_password)
    try:
        stmt = conn.createStatement()
        stmt.execute(ddl)
        stmt.close()
    finally:
        conn.close()


def main():
    db_host = os.getenv("DB_HOST", "postgres")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "bigdataspark")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")

    ch_host = os.getenv("CH_HOST", "clickhouse")
    ch_port = os.getenv("CH_PORT", "8123")
    ch_db = os.getenv("CH_DB", "default")
    ch_user = os.getenv("CH_USER", "default")
    ch_password = os.getenv("CH_PASSWORD", "")
    ch_ddl_path = os.getenv("CH_DDL_PATH", "/opt/spark/sql/02_ddl_clickhouse.sql")

    pg_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    ch_url = f"jdbc:clickhouse://{ch_host}:{ch_port}/{ch_db}"
    pg_props = {"user": db_user, "password": db_password, "driver": "org.postgresql.Driver"}
    ch_props = {"user": ch_user, "password": ch_password, "driver": "com.clickhouse.jdbc.ClickHouseDriver"}

    spark = SparkSession.builder.appName("BigDataSpark-ETL-Star-To-ClickHouse").getOrCreate()

    create_clickhouse_tables(spark, ch_url, ch_user, ch_password, ch_ddl_path)

    fact_sales = spark.read.jdbc(pg_url, "mart.fact_sales", properties=pg_props)
    dim_product = spark.read.jdbc(pg_url, "mart.dim_product", properties=pg_props)
    dim_customer = spark.read.jdbc(pg_url, "mart.dim_customer", properties=pg_props)
    dim_date = spark.read.jdbc(pg_url, "mart.dim_date", properties=pg_props)
    dim_store = spark.read.jdbc(pg_url, "mart.dim_store", properties=pg_props)

    # 1) PRODUCTS (3 tables)
    report_products_base = (
        fact_sales.join(dim_product, "product_id", "left")
        .groupBy("product_id", "name", "category")
        .agg(
            sum_("sale_total").alias("total_revenue"),
            sum_("sale_quantity").alias("total_quantity"),
            avg("rating").alias("avg_rating"),
            sum_("reviews").alias("reviews_count"),
        )
    )
    report_products_top10 = (
        report_products_base.orderBy(desc("total_quantity"))
        .limit(10)
        .select(
            col("product_id"),
            col("name").alias("product_name"),
            col("category").alias("product_category"),
            col("total_quantity"),
            col("total_revenue"),
        )
    )
    report_products_top10.write.mode("append").jdbc(ch_url, "report_products_top10", properties=ch_props)
    report_products_category_revenue = report_products_base.groupBy("category").agg(
        sum_("total_revenue").alias("total_revenue")
    ).select(col("category").alias("product_category"), col("total_revenue"))
    report_products_category_revenue.write.mode("append").jdbc(
        ch_url, "report_products_category_revenue", properties=ch_props
    )
    report_products_rating_reviews = report_products_base.select(
        col("product_id"),
        col("name").alias("product_name"),
        col("category").alias("product_category"),
        col("avg_rating"),
        col("reviews_count"),
    )
    report_products_rating_reviews.write.mode("append").jdbc(
        ch_url, "report_products_rating_reviews", properties=ch_props
    )

    # 2) CUSTOMERS (3 tables)
    report_customers_base = (
        fact_sales.join(dim_customer, "customer_id", "left")
        .groupBy("customer_id", "first_name", "last_name", "country")
        .agg(
            sum_("sale_total").alias("total_spent"),
            count("*").alias("orders_count"),
            avg("sale_total").alias("avg_check"),
        )
    )
    report_customers_top10 = report_customers_base.orderBy(desc("total_spent")).limit(10).select(
        "customer_id", "first_name", "last_name", "country", "total_spent"
    )
    report_customers_top10.write.mode("append").jdbc(ch_url, "report_customers_top10", properties=ch_props)
    report_customers_country_distribution = (
        report_customers_base.groupBy("country")
        .agg(count("*").alias("customers_count"))
        .select("country", "customers_count")
    )
    report_customers_country_distribution.write.mode("append").jdbc(
        ch_url, "report_customers_country_distribution", properties=ch_props
    )
    report_customers_avg_check = report_customers_base.select(
        "customer_id", "first_name", "last_name", "avg_check"
    )
    report_customers_avg_check.write.mode("append").jdbc(
        ch_url, "report_customers_avg_check", properties=ch_props
    )

    # 3) TIME (3 tables)
    report_time_base = (
        fact_sales.join(dim_date, "date_id", "left")
        .groupBy("year", "month")
        .agg(
            count("*").alias("orders_count"),
            sum_("sale_total").alias("total_revenue"),
            avg("sale_total").alias("avg_order_value"),
        )
    )
    report_time_monthly_trends = report_time_base.select("year", "month", "total_revenue", "orders_count")
    report_time_monthly_trends.write.mode("append").jdbc(
        ch_url, "report_time_monthly_trends", properties=ch_props
    )
    report_time_period_revenue = report_time_base.groupBy("year").agg(
        sum_("total_revenue").alias("total_revenue")
    )
    report_time_period_revenue.write.mode("append").jdbc(
        ch_url, "report_time_period_revenue", properties=ch_props
    )
    report_time_avg_order = report_time_base.select("year", "month", col("avg_order_value"))
    report_time_avg_order.write.mode("append").jdbc(
        ch_url, "report_time_avg_order_by_month", properties=ch_props
    )

    # 4) STORES (3 tables)
    report_stores_base = (
        fact_sales.join(dim_store, "store_id", "left")
        .groupBy("store_id", "store_name", "city", "country")
        .agg(
            count("*").alias("orders_count"),
            sum_("sale_total").alias("total_revenue"),
            avg("sale_total").alias("avg_check"),
        )
    )
    report_stores_top5 = report_stores_base.orderBy(desc("total_revenue")).limit(5).select(
        "store_id", "store_name", "city", "country", "total_revenue"
    )
    report_stores_top5.write.mode("append").jdbc(ch_url, "report_stores_top5", properties=ch_props)
    report_stores_distribution = report_stores_base.groupBy("city", "country").agg(
        sum_("total_revenue").alias("total_revenue"),
        sum_("orders_count").alias("orders_count"),
    )
    report_stores_distribution.write.mode("append").jdbc(
        ch_url, "report_stores_city_country_distribution", properties=ch_props
    )
    report_stores_avg_check = report_stores_base.select("store_id", "store_name", "avg_check")
    report_stores_avg_check.write.mode("append").jdbc(
        ch_url, "report_stores_avg_check", properties=ch_props
    )

    # 5) SUPPLIERS (3 tables)
    report_suppliers_base = (
        fact_sales.join(dim_product, "product_id", "left")
        .groupBy("supplier_name", "supplier_country")
        .agg(
            count("product_id").alias("products_count"),
            sum_("sale_total").alias("total_revenue"),
            avg("price").alias("avg_product_price"),
        )
    )
    report_suppliers_top5 = report_suppliers_base.orderBy(desc("total_revenue")).limit(5).select(
        lit(None).cast("long").alias("supplier_id"),
        col("supplier_name"),
        col("supplier_country"),
        col("total_revenue"),
    )
    report_suppliers_top5.write.mode("append").jdbc(ch_url, "report_suppliers_top5", properties=ch_props)
    report_suppliers_avg_price = report_suppliers_base.select(
        lit(None).cast("long").alias("supplier_id"),
        col("supplier_name"),
        col("avg_product_price"),
    )
    report_suppliers_avg_price.write.mode("append").jdbc(
        ch_url, "report_suppliers_avg_price", properties=ch_props
    )
    report_suppliers_country_distribution = report_suppliers_base.groupBy("supplier_country").agg(
        sum_("total_revenue").alias("total_revenue")
    ).select(col("supplier_country"), col("total_revenue"))
    report_suppliers_country_distribution.write.mode("append").jdbc(
        ch_url, "report_suppliers_country_distribution", properties=ch_props
    )

    # 6) QUALITY (3 tables)
    report_quality_base = (
        fact_sales.join(dim_product, "product_id", "left")
        .groupBy("product_id", "name", "category", "rating", "reviews")
        .agg(sum_("sale_quantity").alias("sold_quantity"))
    )
    corr_value = report_quality_base.select(corr("rating", "sold_quantity").alias("c")).first()["c"] or 0.0
    rank_by_best = Window.orderBy(col("rating").desc(), col("reviews").desc())
    rank_by_worst = Window.orderBy(col("rating").asc(), col("reviews").desc())

    report_quality_rank = (
        report_quality_base
        .withColumn("rank_best", dense_rank().over(rank_by_best))
        .withColumn("rank_worst", dense_rank().over(rank_by_worst))
        .select(
            col("product_id"),
            col("name").alias("product_name"),
            col("rating"),
            col("rank_best"),
            col("rank_worst"),
        )
    )
    report_quality_rank.write.mode("append").jdbc(
        ch_url, "report_quality_high_low_rating", properties=ch_props
    )
    report_quality_correlation = spark.createDataFrame(
        [("rating_sales_correlation", float(corr_value))],
        ["metric", "correlation_value"],
    ).select(col("metric"), col("correlation_value").cast("decimal(10,6)"))
    report_quality_correlation.write.mode("append").jdbc(
        ch_url, "report_quality_rating_sales_correlation", properties=ch_props
    )
    report_quality_top_reviews = report_quality_base.orderBy(desc("reviews")).limit(10).select(
        col("product_id"), col("name").alias("product_name"), col("reviews")
    )
    report_quality_top_reviews.write.mode("append").jdbc(
        ch_url, "report_quality_top_reviews", properties=ch_props
    )

    spark.stop()


if __name__ == "__main__":
    main()
