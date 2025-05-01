-- Add physical characteristics columns to item table
ALTER TABLE item
    ADD COLUMN IF NOT EXISTS units_per_case INTEGER DEFAULT 1,
    ADD COLUMN IF NOT EXISTS weight_per_unit DOUBLE PRECISION DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS volume_per_unit DOUBLE PRECISION DEFAULT 0.0;

-- Add comments for documentation
COMMENT ON COLUMN item.units_per_case IS 'Number of units per case';
COMMENT ON COLUMN item.weight_per_unit IS 'Weight per unit in kilograms';
COMMENT ON COLUMN item.volume_per_unit IS 'Volume per unit in cubic meters'; 