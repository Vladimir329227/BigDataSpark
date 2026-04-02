import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, month, quarter, to_date, trim, year
from pyspark.sql.types import DecimalType, IntegerType


def not_empty(column_name: str):
    return (col(column_name).isNotNull()) & (trim(col(column_name)) != "")


def as_int(column_name: str):
    return col(column_name).cast(IntegerType())


def as_num(column_name: str, precision: int = 18, scale: int = 2):
    return col(column_name).cast(DecimalType(precision, scale))


def create_schema_and_tables(
    spark: SparkSession, jdbc_url: str, props: dict, ddl_path: str
):
    with open(ddl_path, "r", encoding="utf-8") as f:
        ddl = f.read()
    spark._sc._jvm.java.lang.Class.forName("org.postgresql.Driver")
    conn = spark._sc._jvm.java.sql.DriverManager.getConnection(
        jdbc_url, props["user"], props["password"]
    )
    try:
        stmt = conn.createStatement()
        stmt.execute(ddl)
        stmt.close()
    finally:
        conn.close()


def main():
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "bigdataspark")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")
    raw_path = os.getenv("RAW_DATA_PATH", "/data/*.csv")
    pg_ddl_path = os.getenv("PG_DDL_PATH", "/opt/spark/sql/01_ddl_star.sql")

    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    props = {"user": db_user, "password": db_password, "driver": "org.postgresql.Driver"}

    spark = (
        SparkSession.builder.appName("BigDataSpark-ETL-Star-Postgres")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    raw_df = (
        spark.read.option("header", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(raw_path)
    )

    create_schema_and_tables(spark, jdbc_url, props, pg_ddl_path)

    sales_date = to_date(col("sale_date"), "M/d/yyyy")
    release_date = to_date(col("product_release_date"), "M/d/yyyy")
    expiry_date = to_date(col("product_expiry_date"), "M/d/yyyy")

    dim_date = (
        raw_df.where(not_empty("sale_date"))
        .withColumn("full_date", sales_date)
        .where(col("full_date").isNotNull())
        .select(
            "full_date",
            year(col("full_date")).alias("year"),
            quarter(col("full_date")).alias("quarter"),
            month(col("full_date")).alias("month"),
            dayofmonth(col("full_date")).alias("day"),
        )
        .dropDuplicates(["full_date"])
    )
    dim_date.write.mode("append").jdbc(jdbc_url, "mart.dim_date", properties=props)

    dim_customer = (
        raw_df.select(
            as_int("sale_customer_id").alias("customer_source_id"),
            col("customer_first_name").alias("first_name"),
            col("customer_last_name").alias("last_name"),
            as_int("customer_age").alias("age"),
            col("customer_email").alias("email"),
            col("customer_country").alias("country"),
            col("customer_postal_code").alias("postal_code"),
            col("customer_pet_type").alias("pet_type"),
            col("customer_pet_name").alias("pet_name"),
            col("customer_pet_breed").alias("pet_breed"),
        )
        .dropDuplicates(["customer_source_id"])
        .where(col("customer_source_id").isNotNull())
    )
    dim_customer.write.mode("append").jdbc(jdbc_url, "mart.dim_customer", properties=props)

    dim_seller = (
        raw_df.select(
            as_int("sale_seller_id").alias("seller_source_id"),
            col("seller_first_name").alias("first_name"),
            col("seller_last_name").alias("last_name"),
            col("seller_email").alias("email"),
            col("seller_country").alias("country"),
            col("seller_postal_code").alias("postal_code"),
        )
        .dropDuplicates(["seller_source_id"])
        .where(col("seller_source_id").isNotNull())
    )
    dim_seller.write.mode("append").jdbc(jdbc_url, "mart.dim_seller", properties=props)

    dim_store = (
        raw_df.select(
            col("store_name"),
            col("store_location").alias("location"),
            col("store_city").alias("city"),
            col("store_state").alias("state"),
            col("store_country").alias("country"),
            col("store_phone").alias("phone"),
            col("store_email").alias("email"),
        )
        .dropDuplicates(["store_name", "city", "state", "country"])
    )
    dim_store.write.mode("append").jdbc(jdbc_url, "mart.dim_store", properties=props)

    product_stage = raw_df.select(
        as_int("sale_product_id").alias("product_source_id"),
        col("product_name").alias("name"),
        col("product_category").alias("category"),
        col("pet_category"),
        as_num("product_price").alias("price"),
        as_num("product_weight").alias("weight"),
        col("product_color").alias("color"),
        col("product_size").alias("size"),
        col("product_brand").alias("brand"),
        col("product_material").alias("material"),
        col("product_description").alias("description"),
        as_num("product_rating", 5, 2).alias("rating"),
        as_int("product_reviews").alias("reviews"),
        release_date.alias("release_date"),
        expiry_date.alias("expiry_date"),
        col("supplier_name"),
        col("supplier_contact"),
        col("supplier_email"),
        col("supplier_phone"),
        col("supplier_address"),
        col("supplier_city"),
        col("supplier_country"),
    )

    dim_product = (
        product_stage.select(
            "product_source_id",
            "name",
            "category",
            "pet_category",
            "price",
            "weight",
            "color",
            "size",
            "brand",
            "material",
            "description",
            "rating",
            "reviews",
            "release_date",
            "expiry_date",
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country",
        )
        .dropDuplicates(["product_source_id"])
        .where(col("product_source_id").isNotNull())
    )
    dim_product.write.mode("append").jdbc(jdbc_url, "mart.dim_product", properties=props)

    customer_ref = spark.read.jdbc(jdbc_url, "mart.dim_customer", properties=props).select(
        "customer_id", "customer_source_id"
    )
    seller_ref = spark.read.jdbc(jdbc_url, "mart.dim_seller", properties=props).select(
        "seller_id", "seller_source_id"
    )
    product_ref = spark.read.jdbc(jdbc_url, "mart.dim_product", properties=props).select(
        "product_id", "product_source_id"
    )
    store_ref = spark.read.jdbc(jdbc_url, "mart.dim_store", properties=props).select(
        "store_id", "store_name", "city", "state", "country"
    )
    date_ref = spark.read.jdbc(jdbc_url, "mart.dim_date", properties=props).select("date_id", "full_date")

    fact_stage = raw_df.select(
        as_int("sale_customer_id").alias("customer_source_id"),
        as_int("sale_seller_id").alias("seller_source_id"),
        as_int("sale_product_id").alias("product_source_id"),
        col("store_name"),
        col("store_city").alias("city"),
        col("store_state").alias("state"),
        col("store_country").alias("country"),
        sales_date.alias("full_date"),
        as_int("sale_quantity").alias("sale_quantity"),
        as_num("sale_total_price").alias("sale_total"),
    )

    fact_sales = (
        fact_stage.join(customer_ref, "customer_source_id", "left")
        .join(seller_ref, "seller_source_id", "left")
        .join(product_ref, "product_source_id", "left")
        .join(store_ref, ["store_name", "city", "state", "country"], "left")
        .join(date_ref, "full_date", "left")
        .select(
            "customer_id",
            "seller_id",
            "product_id",
            "store_id",
            "date_id",
            "sale_quantity",
            "sale_total",
        )
    )
    fact_sales.write.mode("append").jdbc(jdbc_url, "mart.fact_sales", properties=props)

    spark.stop()


if __name__ == "__main__":
    main()
