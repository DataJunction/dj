--
-- Create schemas
--
CREATE SCHEMA IF NOT EXISTS jaffle_shop;
CREATE SCHEMA IF NOT EXISTS stripe;

--
-- Create tables
--
CREATE TABLE IF NOT EXISTS jaffle_shop.customers(
  id integer,
  first_name varchar(50),
  last_name varchar(50)
);

CREATE TABLE IF NOT EXISTS jaffle_shop.orders(
  id integer,
  user_id integer,
  order_date date,
  status varchar(50),
  _etl_loaded_at timestamp default current_timestamp
);

CREATE TABLE IF NOT EXISTS stripe.payments(
  id integer,
  orderid integer,
  paymentmethod varchar(50),
  status varchar(50),
  amount integer,
  created date,
  _batched_at timestamp default current_timestamp
);

--
-- Load data
--
COPY jaffle_shop.customers( id, first_name, last_name)
FROM '/docker-entrypoint-dbt/jaffle_shop_customers.csv'
DELIMITER ','
CSV HEADER;

COPY jaffle_shop.orders(id, user_id, order_date, status)
FROM '/docker-entrypoint-dbt/jaffle_shop_orders.csv'
DELIMITER ','
CSV HEADER;

COPY stripe.payments(id, orderid, paymentmethod, status, amount, created)
FROM '/docker-entrypoint-dbt/stripe_payments.csv'
DELIMITER ','
CSV HEADER;