CREATE SCHEMA e_commerce;

DROP TABLE IF EXISTS e_commerce.product;

CREATE TABLE e_commerce.product (
    product_id serial PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(50),
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INT NOT NULL,
    sku VARCHAR(50),
    brand VARCHAR(50),
    image_url TEXT,
    weight DECIMAL(10, 2),
    length DECIMAL(10, 2),
    width DECIMAL(10, 2),
    height DECIMAL(10, 2),
    product_rating DECIMAL(3, 2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
