-- Employee Commission System Migration
-- Run this script to add the required columns and tables

-- Add commission fields to users table
ALTER TABLE users 
ADD COLUMN IF NOT EXISTS current_balance NUMERIC(10, 2) DEFAULT 0.00,
ADD COLUMN IF NOT EXISTS commission_amount_per_connection NUMERIC(10, 2) DEFAULT 0.00,
ADD COLUMN IF NOT EXISTS commission_amount_per_complaint NUMERIC(10, 2) DEFAULT 0.00;

-- Add technician_id to customers table
ALTER TABLE customers 
ADD COLUMN IF NOT EXISTS technician_id UUID REFERENCES users(id);

-- Create employee_ledger table
CREATE TABLE IF NOT EXISTS employee_ledger (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID NOT NULL REFERENCES companies(id),
    employee_id UUID NOT NULL REFERENCES users(id),
    transaction_type VARCHAR(50) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    description TEXT,
    reference_id UUID,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_employee_ledger_employee_id ON employee_ledger(employee_id);
CREATE INDEX IF NOT EXISTS idx_employee_ledger_company_id ON employee_ledger(company_id);
CREATE INDEX IF NOT EXISTS idx_employee_ledger_created_at ON employee_ledger(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_customers_technician_id ON customers(technician_id);

-- Set default values for existing employees
UPDATE users 
SET current_balance = 0.00,
    commission_amount_per_connection = 0.00,
    commission_amount_per_complaint = 0.00
WHERE current_balance IS NULL;

COMMENT ON TABLE employee_ledger IS 'Tracks all financial transactions for employees including commissions, salary, and payouts';
COMMENT ON COLUMN users.current_balance IS 'Current unpaid balance owed to employee';
COMMENT ON COLUMN users.commission_amount_per_connection IS 'Monthly commission rate per active connection managed';
COMMENT ON COLUMN users.commission_amount_per_complaint IS 'Commission earned per resolved complaint';
COMMENT ON COLUMN customers.technician_id IS 'Employee eligible for monthly connection commission';
