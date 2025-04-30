-- Add missing columns to item table
ALTER TABLE item
    ADD COLUMN IF NOT EXISTS buyer_id VARCHAR(50),
    ADD COLUMN IF NOT EXISTS system_class VARCHAR(1) DEFAULT 'U',
    ADD COLUMN IF NOT EXISTS auxiliary_balance DOUBLE PRECISION DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS manual_ss_type VARCHAR(50),
    ADD COLUMN IF NOT EXISTS item_cycle_units DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS item_cycle_days DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS purchase_price_divisor DOUBLE PRECISION DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS carrying_cost_adjustments DOUBLE PRECISION DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS handling_cost_adjustments DOUBLE PRECISION DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS supersede_to_item_id VARCHAR(50),
    ADD COLUMN IF NOT EXISTS supersede_from_item_id VARCHAR(50),
    ADD COLUMN IF NOT EXISTS forecasting_periodicity INTEGER DEFAULT 13,
    ADD COLUMN IF NOT EXISTS history_periodicity INTEGER DEFAULT 13,
    ADD COLUMN IF NOT EXISTS forecast_method VARCHAR(15),
    ADD COLUMN IF NOT EXISTS freeze_until_date DATE,
    ADD COLUMN IF NOT EXISTS demand_profile VARCHAR(20),
    ADD COLUMN IF NOT EXISTS buyer_max DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS buyer_min DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS type_for_min_max VARCHAR(1),
    ADD COLUMN IF NOT EXISTS calculated_in_days INTEGER,
    ADD COLUMN IF NOT EXISTS calculated_variance DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS lead_time_profile VARCHAR(20),
    ADD COLUMN IF NOT EXISTS fill_in_lead_time INTEGER,
    ADD COLUMN IF NOT EXISTS convenience_pack DOUBLE PRECISION DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS conv_pk_breakpoint DOUBLE PRECISION DEFAULT 0.0,
    ADD COLUMN IF NOT EXISTS number_of_conv_packs INTEGER;

-- Add constraints for system_class
ALTER TABLE item
    ADD CONSTRAINT check_system_class 
    CHECK (system_class IN ('S', 'L', 'U', 'R', 'N', 'A'));

-- Add comments for documentation
COMMENT ON COLUMN item.buyer_id IS 'ID of the buyer responsible for this item';
COMMENT ON COLUMN item.system_class IS 'System classification (S=Slow, L=Lumpy, U=Uninitialized, R=Regular, N=New, A=Alternate)';
COMMENT ON COLUMN item.auxiliary_balance IS 'Additional balance field for special calculations';
COMMENT ON COLUMN item.manual_ss_type IS 'Type of manual safety stock calculation';
COMMENT ON COLUMN item.item_cycle_units IS 'Item cycle in units';
COMMENT ON COLUMN item.item_cycle_days IS 'Item cycle in days';
COMMENT ON COLUMN item.purchase_price_divisor IS 'Divisor used in purchase price calculations';
COMMENT ON COLUMN item.carrying_cost_adjustments IS 'Adjustments to carrying costs';
COMMENT ON COLUMN item.handling_cost_adjustments IS 'Adjustments to handling costs';
COMMENT ON COLUMN item.supersede_to_item_id IS 'ID of the item this item is being superseded to';
COMMENT ON COLUMN item.supersede_from_item_id IS 'ID of the item this item is being superseded from';
COMMENT ON COLUMN item.forecasting_periodicity IS 'Number of periods used in forecasting';
COMMENT ON COLUMN item.history_periodicity IS 'Number of periods used in history';
COMMENT ON COLUMN item.forecast_method IS 'Method used for forecasting';
COMMENT ON COLUMN item.freeze_until_date IS 'Date until which forecasting is frozen';
COMMENT ON COLUMN item.demand_profile IS 'Profile of demand pattern';
COMMENT ON COLUMN item.buyer_max IS 'Maximum quantity set by buyer';
COMMENT ON COLUMN item.buyer_min IS 'Minimum quantity set by buyer';
COMMENT ON COLUMN item.type_for_min_max IS 'Type indicator for min/max calculations';
COMMENT ON COLUMN item.calculated_in_days IS 'Lead time calculated in days';
COMMENT ON COLUMN item.calculated_variance IS 'Calculated variance in lead time';
COMMENT ON COLUMN item.lead_time_profile IS 'Profile of lead time pattern';
COMMENT ON COLUMN item.fill_in_lead_time IS 'Fill-in lead time for special orders';
COMMENT ON COLUMN item.convenience_pack IS 'Convenience pack quantity';
COMMENT ON COLUMN item.conv_pk_breakpoint IS 'Breakpoint for convenience pack usage';
COMMENT ON COLUMN item.number_of_conv_packs IS 'Number of convenience packs'; 