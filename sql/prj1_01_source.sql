-- ~/de_lab/projects/enterprise-cdc-lakehouse/sql/prj1_01_source.sql
-- The source system (your fake SAP) and the outbox table.
CREATE SCHEMA IF NOT EXISTS daimler;
CREATE SCHEMA IF NOT EXISTS outbox;
 
CREATE TABLE IF NOT EXISTS daimler.vehicle_sales (
  sale_id        TEXT PRIMARY KEY,
  vin            TEXT NOT NULL,
  plant_id       TEXT NOT NULL,
  dealer_id      TEXT NOT NULL,
  customer_id    TEXT NOT NULL,
  customer_email TEXT NOT NULL,
  customer_phone TEXT NOT NULL,
  sale_date      DATE NOT NULL,
  sale_amount    NUMERIC(12,2) NOT NULL,
  currency       TEXT NOT NULL DEFAULT 'EUR',
  engine_type    TEXT NOT NULL,
  model_code     TEXT NOT NULL,
  warranty_tier  TEXT NOT NULL DEFAULT 'STANDARD'
);
 
-- Exactly the five columns Debezium's outbox router expects by default.
CREATE TABLE IF NOT EXISTS outbox.event (
  id            UUID         PRIMARY KEY,
  aggregatetype VARCHAR(255) NOT NULL,
  aggregateid   VARCHAR(255) NOT NULL,
  type          VARCHAR(255) NOT NULL,
  payload       JSONB        NOT NULL
);
