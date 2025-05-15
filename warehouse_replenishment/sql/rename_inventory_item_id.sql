-- Rename item_id to product_id in inventory table
ALTER TABLE inventory
RENAME COLUMN item_id TO product_id;

-- Update the foreign key constraint
ALTER TABLE inventory
DROP CONSTRAINT IF EXISTS inventory_item_id_fkey,
ADD CONSTRAINT inventory_product_id_fkey 
FOREIGN KEY (product_id) REFERENCES item(id);

-- Update the index
DROP INDEX IF EXISTS idx_inventory_item_warehouse;
CREATE INDEX idx_inventory_product_warehouse ON inventory(product_id, warehouse_id); 