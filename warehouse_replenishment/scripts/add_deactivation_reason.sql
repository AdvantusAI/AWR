-- Add deactivation_reason column to vendor table
ALTER TABLE vendor ADD COLUMN deactivation_reason VARCHAR(255);

-- Add comment to explain the column's purpose
COMMENT ON COLUMN vendor.deactivation_reason IS 'Reason for vendor deactivation when deactivate_until is set'; 