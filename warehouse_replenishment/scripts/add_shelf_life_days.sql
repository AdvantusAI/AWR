-- Add shelf_life_days column to items table
ALTER TABLE items
ADD COLUMN shelf_life_days INTEGER;

-- Add comment to describe the column
COMMENT ON COLUMN items.shelf_life_days IS 'Number of days an item can be stored before it expires';

-- Set default value to NULL for existing records
UPDATE items
SET shelf_life_days = NULL
WHERE shelf_life_days IS NULL; 