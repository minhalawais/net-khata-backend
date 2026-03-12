-- Task Model Migration
-- This migration updates the tasks table and creates the task_assignees junction table

-- First, drop columns that are no longer needed (if they exist)
-- Note: Run these one at a time and handle any errors for columns that don't exist

-- Drop old columns
ALTER TABLE tasks DROP COLUMN IF EXISTS title;
ALTER TABLE tasks DROP COLUMN IF EXISTS description;
ALTER TABLE tasks DROP COLUMN IF EXISTS related_complaint_id;
ALTER TABLE tasks DROP COLUMN IF EXISTS assigned_to;

-- Add new columns
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS customer_id UUID REFERENCES customers(id);

-- Update task_type column to VARCHAR if it's using ENUM
ALTER TABLE tasks ALTER COLUMN task_type TYPE VARCHAR(50);
ALTER TABLE tasks ALTER COLUMN priority TYPE VARCHAR(20);
ALTER TABLE tasks ALTER COLUMN status TYPE VARCHAR(20);

-- Update due_date to include timezone
ALTER TABLE tasks ALTER COLUMN due_date TYPE TIMESTAMP WITH TIME ZONE;

-- Create task_assignees junction table for multiple employee assignments
CREATE TABLE IF NOT EXISTS task_assignees (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    employee_id UUID NOT NULL REFERENCES users(id),
    assigned_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(task_id, employee_id)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_tasks_customer_id ON tasks(customer_id);
CREATE INDEX IF NOT EXISTS idx_task_assignees_task_id ON task_assignees(task_id);
CREATE INDEX IF NOT EXISTS idx_task_assignees_employee_id ON task_assignees(employee_id);
