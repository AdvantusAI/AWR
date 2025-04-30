-- First, create a temporary column to store the string values
ALTER TABLE item ADD COLUMN buyer_class_new VARCHAR(1);

-- Update the new column with string values from the enum
UPDATE item 
SET buyer_class_new = 
    CASE buyer_class::text
        WHEN 'REGULAR' THEN 'R'
        WHEN 'WEEKLY' THEN 'W'
        WHEN 'MONTHLY' THEN 'M'
        WHEN 'DAILY' THEN 'D'
        WHEN 'UNINITIALIZED' THEN 'U'
        ELSE 'U'
    END;

-- Drop the old enum column
ALTER TABLE item DROP COLUMN buyer_class;

-- Rename the new column to buyer_class
ALTER TABLE item RENAME COLUMN buyer_class_new TO buyer_class;

-- Add a default value
ALTER TABLE item ALTER COLUMN buyer_class SET DEFAULT 'U';

-- Add a check constraint to ensure only valid values are used
ALTER TABLE item ADD CONSTRAINT check_buyer_class 
    CHECK (buyer_class IN ('R', 'W', 'M', 'D', 'U'));

-- Drop the old enum type if it exists
DROP TYPE IF EXISTS buyerclasscode; 