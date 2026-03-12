-- Inventory Module Redesign Migration
-- Run this SQL on your PostgreSQL database

-- Add inventory_item_id to invoice_line_items for equipment tracking
ALTER TABLE invoice_line_items 
ADD COLUMN IF NOT EXISTS inventory_item_id UUID REFERENCES inventory_items(id);

-- Add item_type to distinguish package vs equipment line items
ALTER TABLE invoice_line_items 
ADD COLUMN IF NOT EXISTS item_type VARCHAR(20) DEFAULT 'package';

-- Index for equipment lookups
CREATE INDEX IF NOT EXISTS idx_invoice_line_items_inventory 
ON invoice_line_items(inventory_item_id) WHERE inventory_item_id IS NOT NULL;

-- Note: After confirming everything works, run this to remove redundant table:
-- DROP TABLE IF EXISTS inventory_assignments;
