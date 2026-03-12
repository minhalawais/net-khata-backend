-- Recovery Task Model Simplification Migration
-- This migration removes unnecessary columns from recovery_tasks table

-- Drop the columns that are no longer needed
ALTER TABLE recovery_tasks DROP COLUMN IF EXISTS recovery_type;
ALTER TABLE recovery_tasks DROP COLUMN IF EXISTS attempts_count;
ALTER TABLE recovery_tasks DROP COLUMN IF EXISTS last_attempt_date;
ALTER TABLE recovery_tasks DROP COLUMN IF EXISTS recovered_amount;
ALTER TABLE recovery_tasks DROP COLUMN IF EXISTS reason;
ALTER TABLE recovery_tasks DROP COLUMN IF EXISTS is_active;

-- Update status column to VARCHAR type
ALTER TABLE recovery_tasks ALTER COLUMN status TYPE VARCHAR(20);

-- Add NOT NULL constraints to required fields
ALTER TABLE recovery_tasks ALTER COLUMN invoice_id SET NOT NULL;
ALTER TABLE recovery_tasks ALTER COLUMN assigned_to SET NOT NULL;
