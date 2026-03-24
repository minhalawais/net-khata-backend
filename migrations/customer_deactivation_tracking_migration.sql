-- Add explicit deactivation tracking on customers for accurate churn analytics.
ALTER TABLE customers
ADD COLUMN IF NOT EXISTS deactivated_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS deactivated_reason VARCHAR(255);

-- Backfill legacy inactive rows so churn charts can start using deactivated_at.
UPDATE customers
SET deactivated_at = COALESCE(deactivated_at, updated_at)
WHERE is_active = FALSE
  AND deactivated_at IS NULL;
