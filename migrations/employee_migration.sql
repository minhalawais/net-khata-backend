-- Employee Model Enhancement Migration
-- Run this migration to add new employee fields to the users table

-- Add new columns to users table
ALTER TABLE users ADD COLUMN IF NOT EXISTS emergency_contact VARCHAR(20);
ALTER TABLE users ADD COLUMN IF NOT EXISTS house_address TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS cnic_image VARCHAR(500);
ALTER TABLE users ADD COLUMN IF NOT EXISTS picture VARCHAR(500);
ALTER TABLE users ADD COLUMN IF NOT EXISTS utility_bill_image VARCHAR(500);
ALTER TABLE users ADD COLUMN IF NOT EXISTS joining_date DATE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS salary NUMERIC(10, 2);
ALTER TABLE users ADD COLUMN IF NOT EXISTS reference_name VARCHAR(100);
ALTER TABLE users ADD COLUMN IF NOT EXISTS reference_contact VARCHAR(20);
ALTER TABLE users ADD COLUMN IF NOT EXISTS reference_cnic_image VARCHAR(500);

-- Verification query
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'users' 
AND column_name IN (
    'emergency_contact', 'house_address', 'cnic_image', 'picture',
    'utility_bill_image', 'joining_date', 'salary', 'reference_name',
    'reference_contact', 'reference_cnic_image'
);
