-- Sub-Zone Migration Script
-- Run this script to add sub_zones table and update customers table

-- Create sub_zones table
CREATE TABLE IF NOT EXISTS sub_zones (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID REFERENCES companies(id),
    area_id UUID NOT NULL REFERENCES areas(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Add sub_zone_id to customers table
ALTER TABLE customers ADD COLUMN IF NOT EXISTS sub_zone_id UUID REFERENCES sub_zones(id);

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_sub_zones_area_id ON sub_zones(area_id);
CREATE INDEX IF NOT EXISTS idx_sub_zones_company_id ON sub_zones(company_id);
CREATE INDEX IF NOT EXISTS idx_customers_sub_zone_id ON customers(sub_zone_id);
