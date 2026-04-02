CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_id      BIGSERIAL PRIMARY KEY,
    full_date    DATE UNIQUE NOT NULL,
    year         INTEGER NOT NULL,
    quarter      INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    day          INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.dim_customer (
    customer_id         BIGSERIAL PRIMARY KEY,
    customer_source_id  INTEGER UNIQUE,
    first_name          TEXT,
    last_name           TEXT,
    age                 INTEGER,
    email               TEXT,
    country             TEXT,
    postal_code         TEXT,
    pet_type            TEXT,
    pet_name            TEXT,
    pet_breed           TEXT
);

CREATE TABLE IF NOT EXISTS mart.dim_seller (
    seller_id         BIGSERIAL PRIMARY KEY,
    seller_source_id  INTEGER UNIQUE,
    first_name        TEXT,
    last_name         TEXT,
    email             TEXT,
    country           TEXT,
    postal_code       TEXT
);

CREATE TABLE IF NOT EXISTS mart.dim_store (
    store_id    BIGSERIAL PRIMARY KEY,
    store_name  TEXT,
    location    TEXT,
    city        TEXT,
    state       TEXT,
    country     TEXT,
    phone       TEXT,
    email       TEXT,
    CONSTRAINT uq_dim_store UNIQUE (store_name, city, state, country)
);

CREATE TABLE IF NOT EXISTS mart.dim_product (
    product_id         BIGSERIAL PRIMARY KEY,
    product_source_id  INTEGER UNIQUE,
    name               TEXT,
    category           TEXT,
    pet_category       TEXT,
    price              NUMERIC(18,2),
    weight             NUMERIC(18,2),
    color              TEXT,
    size               TEXT,
    brand              TEXT,
    material           TEXT,
    description        TEXT,
    rating             NUMERIC(5,2),
    reviews            INTEGER,
    release_date       DATE,
    expiry_date        DATE,
    supplier_name      TEXT,
    supplier_contact   TEXT,
    supplier_email     TEXT,
    supplier_phone     TEXT,
    supplier_address   TEXT,
    supplier_city      TEXT,
    supplier_country   TEXT
);

CREATE TABLE IF NOT EXISTS mart.fact_sales (
    sales_id       BIGSERIAL PRIMARY KEY,
    customer_id    BIGINT REFERENCES mart.dim_customer (customer_id),
    seller_id      BIGINT REFERENCES mart.dim_seller (seller_id),
    product_id     BIGINT REFERENCES mart.dim_product (product_id),
    store_id       BIGINT REFERENCES mart.dim_store (store_id),
    date_id        BIGINT REFERENCES mart.dim_date (date_id),
    sale_quantity  INTEGER,
    sale_total     NUMERIC(18,2)
);

TRUNCATE TABLE mart.fact_sales RESTART IDENTITY;
TRUNCATE TABLE mart.dim_product RESTART IDENTITY CASCADE;
TRUNCATE TABLE mart.dim_store RESTART IDENTITY CASCADE;
TRUNCATE TABLE mart.dim_seller RESTART IDENTITY CASCADE;
TRUNCATE TABLE mart.dim_customer RESTART IDENTITY CASCADE;
TRUNCATE TABLE mart.dim_date RESTART IDENTITY CASCADE;
