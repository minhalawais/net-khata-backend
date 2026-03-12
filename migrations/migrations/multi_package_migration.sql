-- =====================================================
-- Multi-Package Customer Feature - Database Migration
-- Run these queries in order on your PostgreSQL database
-- =====================================================

-- Step 1: Create customer_packages table
CREATE TABLE customer_packages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    service_plan_id UUID NOT NULL REFERENCES service_plans(id) ON DELETE RESTRICT,
    start_date DATE NOT NULL,
    end_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Step 2: Create indexes for customer_packages
CREATE INDEX idx_customer_packages_customer_id ON customer_packages(customer_id);
CREATE INDEX idx_customer_packages_service_plan_id ON customer_packages(service_plan_id);
CREATE INDEX idx_customer_packages_active ON customer_packages(customer_id, is_active);

-- Step 3: Create invoice_line_items table
CREATE TABLE invoice_line_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    invoice_id UUID NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
    customer_package_id UUID REFERENCES customer_packages(id) ON DELETE SET NULL,
    description VARCHAR(255) NOT NULL,
    quantity INTEGER DEFAULT 1,
    unit_price NUMERIC(10, 2) NOT NULL,
    discount_amount NUMERIC(10, 2) DEFAULT 0,
    line_total NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Step 4: Create indexes for invoice_line_items
CREATE INDEX idx_invoice_line_items_invoice_id ON invoice_line_items(invoice_id);
CREATE INDEX idx_invoice_line_items_customer_package_id ON invoice_line_items(customer_package_id);

-- Step 5: Enable UUID extension (run one of these based on your PostgreSQL version)
-- For PostgreSQL 13+:
-- CREATE EXTENSION IF NOT EXISTS pgcrypto;
-- For older versions:
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Step 6: Migrate existing customers' service_plan_id to customer_packages table
-- This creates a package entry for each customer using their current service_plan_id
INSERT INTO customer_packages (id, customer_id, service_plan_id, start_date, is_active, notes)
SELECT 
    uuid_generate_v4() AS id,
    c.id AS customer_id,
    c.service_plan_id,
    COALESCE(c.installation_date, CURRENT_DATE) AS start_date,
    c.is_active,
    'Migrated from legacy service_plan_id'
FROM customers c
WHERE c.service_plan_id IS NOT NULL;

-- Step 7: Verify migration (run this to check)
-- SELECT 
--     c.id, c.first_name, c.last_name, c.service_plan_id as legacy_plan,
--     cp.service_plan_id as migrated_plan, cp.start_date
-- FROM customers c
-- LEFT JOIN customer_packages cp ON c.id = cp.customer_id
-- LIMIT 20;

-- Step 8: Remove the legacy service_plan_id column from customers table
-- Run this AFTER verifying the migration was successful
ALTER TABLE customers DROP COLUMN IF EXISTS service_plan_id;
