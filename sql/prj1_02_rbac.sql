-- ~/de_lab/projects/enterprise-cdc-lakehouse/sql/prj1_02_rbac.sql
-- Two roles. An analyst sees business columns only. A steward sees everything.
-- No password in this file. lab_roles.py sets it from .env right after.
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'analyst_alice') THEN
    CREATE ROLE analyst_alice LOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'steward_sam') THEN
    CREATE ROLE steward_sam LOGIN;
  END IF;
END $$;
 
REVOKE ALL ON daimler.vehicle_sales FROM analyst_alice, steward_sam;
GRANT USAGE ON SCHEMA daimler TO analyst_alice, steward_sam;
-- Column-level grant: the analyst gets every column EXCEPT email and phone.
GRANT SELECT (sale_id, vin, plant_id, dealer_id, customer_id, sale_date, sale_amount,
              currency, engine_type, model_code, warranty_tier)
  ON daimler.vehicle_sales TO analyst_alice;
GRANT SELECT ON daimler.vehicle_sales TO steward_sam;
