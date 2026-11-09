CREATE TABLE IF NOT EXISTS staging_customers (
    customer_id text,
    company_name text,
    contact_name text,
    contact_title text,
    address text,
    city text,
    region text,
    postal_code text,
    country text,
    phone text,
    fax text
);
CREATE TABLE IF NOT EXISTS staging_employees (
    employee_id integer,
    last_name text,
    first_name text,
    title text,
    title_of_courtesy text,
    birth_date timestamp,
    hire_date timestamp,
    address text,
    city text,
    region text,
    postal_code text,
    country text,
    home_phone text,
    extension text,
    reports_to integer
);
CREATE TABLE IF NOT EXISTS staging_factorders (
    order_id integer,
    product_id integer,
    customer_id text,
    employee_id integer,
    order_date timestamp,
    required_date timestamp,
    shipped_date timestamp,
    ship_via integer,
    ship_name text,
    ship_address text,
    ship_city text,
    ship_region text,
    ship_postal_code text,
    ship_country text,
    unit_price numeric(18,2),
    quantity integer,
    discount numeric(10,4)
);
CREATE TABLE IF NOT EXISTS staging_geography (
    country text,
    region text,
    city text,
    postal_code text,
    address text
);
CREATE TABLE IF NOT EXISTS staging_products (
    product_id integer,
    supplier_id integer,
    product_name text,
    category_name text,
    quantity_per_unit text,
    unit_price numeric(18,2),
    units_in_stock integer,
    units_on_order integer,
    reorder_level integer,
    discontinued boolean
);
CREATE TABLE IF NOT EXISTS staging_shippers (
    shipper_id integer,
    company_name text,
    phone text
);
CREATE TABLE IF NOT EXISTS staging_suppliers (
    supplier_id integer,
    company_name text,
    contact_name text,
    contact_title text,
    address text,
    city text,
    region text,
    postal_code text,
    country text,
    phone text,
    fax text,
    home_page text
);
