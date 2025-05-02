-- Add forecast_lead_time column to item table
ALTER TABLE item 
ADD COLUMN forecast_lead_time INTEGER DEFAULT 7;

-- Add comment to explain the column
COMMENT ON COLUMN item.forecast_lead_time IS 'The forecasted lead time in days for this item. This is used in forecasting and inventory calculations. Default value is 7 days.'; 