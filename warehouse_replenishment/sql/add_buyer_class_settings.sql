-- Add buyer_class_settings column to vendor table
ALTER TABLE vendor 
ADD COLUMN buyer_class_settings JSONB DEFAULT '{
    "alpha_factor": 10.0,
    "service_level_goal": 95.0,
    "lead_time_forecast_control": 1,
    "enable_history_adjust": false,
    "automatic_rebuild": 0,
    "auto_approval_bracket": null,
    "supv_build_option": 0
}'::jsonb;

-- Add comment to explain the column
COMMENT ON COLUMN vendor.buyer_class_settings IS 'JSON field containing vendor-specific settings for different buyer classes. Default values are provided for common settings.'; 