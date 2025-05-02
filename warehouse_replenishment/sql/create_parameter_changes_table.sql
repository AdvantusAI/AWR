-- Create parameter_changes table
CREATE TABLE parameter_changes (
    id SERIAL PRIMARY KEY,
    item_id INTEGER NOT NULL REFERENCES item(id),
    parameter_type VARCHAR(20) NOT NULL,
    current_value FLOAT NOT NULL,
    recommended_value FLOAT NOT NULL,
    change_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_date TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    reason VARCHAR(500),
    approved_by VARCHAR(100),
    approved_date TIMESTAMP
);

-- Add comment to table
COMMENT ON TABLE parameter_changes IS 'Stores parameter change recommendations for items, including alpha factor, lead time, and safety stock adjustments';

-- Add indexes
CREATE INDEX idx_parameter_changes_item_id ON parameter_changes(item_id);
CREATE INDEX idx_parameter_changes_status ON parameter_changes(status);
CREATE INDEX idx_parameter_changes_effective_date ON parameter_changes(effective_date); 