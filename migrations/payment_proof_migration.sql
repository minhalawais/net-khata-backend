-- Migration to add payment_proof column to expenses and extra_incomes tables
-- Execute this on your PostgreSQL database

-- Add payment_proof column to expenses table
ALTER TABLE expenses ADD COLUMN IF NOT EXISTS payment_proof VARCHAR(500);

-- Add payment_proof column to extra_incomes table
ALTER TABLE extra_incomes ADD COLUMN IF NOT EXISTS payment_proof VARCHAR(500);

-- Add index for faster lookups (optional)
-- CREATE INDEX IF NOT EXISTS idx_expenses_payment_proof ON expenses(payment_proof) WHERE payment_proof IS NOT NULL;
-- CREATE INDEX IF NOT EXISTS idx_extra_incomes_payment_proof ON extra_incomes(payment_proof) WHERE payment_proof IS NOT NULL;
