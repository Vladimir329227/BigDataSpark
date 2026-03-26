import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, corr, dense_rank, sum as sum_
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
    dim_supplier = spark.read.jdbc(pg_url, "mart.dim_supplier", properties=pg_props)

    # 1) Витрина продаж по продуктам:
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
    report_products = (
        report_products_base.withColumn(
            "category_revenue",
            sum_("total_revenue").over(Window.partitionBy("category")),
        )
        .select(
            col("product_id"),
            col("name").alias("product_name"),
            col("category").alias("product_category"),
            col("total_revenue"),
            col("total_quantity"),
            col("category_revenue"),
            col("avg_rating"),
            col("reviews_count"),
        )
    )
    report_products.write.mode("append").jdbc(ch_url, "report_sales_by_products", properties=ch_props)

    # 2) Витрина продаж по клиентам:
    report_customers_base = (
        fact_sales.join(dim_customer, "customer_id", "left")
        .groupBy("customer_id", "first_name", "last_name", "country")
        .agg(
            sum_("sale_total").alias("total_spent"),
            count("*").alias("orders_count"),
            avg("sale_total").alias("avg_check"),
        )
    )
    report_customers = report_customers_base.withColumn(
        "country_customers_count",
        count("*").over(Window.partitionBy("country")),
    )
    report_customers.write.mode("append").jdbc(ch_url, "report_sales_by_customers", properties=ch_props)

    # 3) Витрина продаж по времени
    report_time = (
        fact_sales.join(dim_date, "date_id", "left")
        .groupBy("year", "month")
        .agg(
            count("*").alias("orders_count"),
            sum_("sale_total").alias("total_revenue"),
            avg("sale_total").alias("avg_order_value"),
        )
        .orderBy("year", "month")
    )
    report_time.write.mode("append").jdbc(ch_url, "report_sales_by_time", properties=ch_props)

    # 4) Витрина продаж по магазинам:
    report_stores_base = (
        fact_sales.join(dim_store, "store_id", "left")
        .groupBy("store_id", "store_name", "city", "country")
        .agg(
            count("*").alias("orders_count"),
            sum_("sale_total").alias("total_revenue"),
            avg("sale_total").alias("avg_check"),
        )
    )
    report_stores = report_stores_base.withColumn(
        "city_country_revenue",
        sum_("total_revenue").over(Window.partitionBy("city", "country")),
    )
    report_stores.write.mode("append").jdbc(ch_url, "report_sales_by_stores", properties=ch_props)

    # 5) Витрина продаж по поставщикам:
    report_suppliers_base = (
        fact_sales.join(dim_product, "product_id", "left")
        .join(dim_supplier, "supplier_id", "left")
        .groupBy("supplier_id", "name", "country")
        .agg(
            count("product_id").alias("products_count"),
            sum_("sale_total").alias("total_revenue"),
            avg("price").alias("avg_product_price"),
        )
    )
    report_suppliers = (
        report_suppliers_base.withColumn(
            "supplier_country_revenue",
            sum_("total_revenue").over(Window.partitionBy("country")),
        )
        .select(
            col("supplier_id"),
            col("name").alias("supplier_name"),
            col("country").alias("supplier_country"),
            col("products_count"),
            col("total_revenue"),
            col("avg_product_price"),
            col("supplier_country_revenue"),
        )
    )
    report_suppliers.write.mode("append").jdbc(ch_url, "report_sales_by_suppliers", properties=ch_props)

    # 6) Витрина качества продукции:
    report_quality_base = (
        fact_sales.join(dim_product, "product_id", "left")
        .groupBy("product_id", "name", "category", "rating", "reviews")
        .agg(sum_("sale_quantity").alias("sold_quantity"))
    )
    corr_value = report_quality_base.select(corr("rating", "sold_quantity").alias("c")).first()["c"] or 0.0
    rank_by_best = Window.orderBy(col("rating").desc(), col("reviews").desc())
    rank_by_worst = Window.orderBy(col("rating").asc(), col("reviews").desc())

    report_quality = (
        report_quality_base
        .withColumn("rank_best", dense_rank().over(rank_by_best))
        .withColumn("rank_worst", dense_rank().over(rank_by_worst))
        .withColumn("rating_sales_correlation", col("rating") * 0 + corr_value)
        .select(
            col("product_id"),
            col("name").alias("product_name"),
            col("category").alias("product_category"),
            col("rating"),
            col("reviews"),
            col("sold_quantity"),
            col("rank_best"),
            col("rank_worst"),
            col("rating_sales_correlation"),
        )
    )
    report_quality.write.mode("append").jdbc(ch_url, "report_product_quality", properties=ch_props)

    spark.stop()


if __name__ == "__main__":
    main()
