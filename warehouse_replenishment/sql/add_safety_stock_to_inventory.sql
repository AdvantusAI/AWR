-- Add safety_stock column to inventory table
ALTER TABLE inventory
ADD COLUMN safety_stock DOUBLE PRECISION DEFAULT 0.0;

-- Add comment to describe the column
COMMENT ON COLUMN inventory.safety_stock IS 'Historical safety stock level for this item/location combination. This represents the calculated or manually set safety stock level that was in effect at the time of the last inventory update.';

-- Create an index for faster queries on safety_stock
CREATE INDEX idx_inventory_safety_stock ON inventory(safety_stock);

-- Add a check constraint to ensure safety_stock is not negative
ALTER TABLE inventory
ADD CONSTRAINT check_safety_stock_positive 
CHECK (safety_stock >= 0.0); 