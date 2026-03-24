-- Phase cleanup migration: remove company-level Financial Intelligence V2 feature flag
ALTER TABLE companies
DROP COLUMN IF EXISTS financial_intelligence_v2_enabled;
