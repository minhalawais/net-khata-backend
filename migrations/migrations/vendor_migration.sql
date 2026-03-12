-- Vendor Migration Script
-- Run this script to create the vendors table

CREATE TABLE IF NOT EXISTS vendors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID NOT NULL REFERENCES companies(id),
    name VARCHAR(255) NOT NULL,
    phone VARCHAR(20) NOT NULL,
    email VARCHAR(255),
    cnic VARCHAR(15) UNIQUE NOT NULL,
    picture VARCHAR(500),
    cnic_front_image VARCHAR(500),
    cnic_back_image VARCHAR(500),
    agreement_document VARCHAR(500),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_vendors_company_id ON vendors(company_id);
CREATE INDEX IF NOT EXISTS idx_vendors_cnic ON vendors(cnic);
CREATE INDEX IF NOT EXISTS idx_vendors_phone ON vendors(phone);
