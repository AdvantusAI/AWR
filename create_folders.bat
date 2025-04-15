@echo off
echo Creating Warehouse Replenishment folder structure...

:: Create main directories
mkdir warehouse_replenishment
mkdir warehouse_replenishment\config
mkdir warehouse_replenishment\logs
mkdir warehouse_replenishment\scripts
mkdir warehouse_replenishment\warehouse_replenishment
mkdir warehouse_replenishment\warehouse_replenishment\core
mkdir warehouse_replenishment\warehouse_replenishment\services
mkdir warehouse_replenishment\warehouse_replenishment\batch
mkdir warehouse_replenishment\warehouse_replenishment\utils

:: Create empty files for package initialization
type nul > warehouse_replenishment\warehouse_replenishment\__init__.py
type nul > warehouse_replenishment\warehouse_replenishment\models.py
type nul > warehouse_replenishment\warehouse_replenishment\config.py
type nul > warehouse_replenishment\warehouse_replenishment\db.py
type nul > warehouse_replenishment\warehouse_replenishment\logging_setup.py
type nul > warehouse_replenishment\warehouse_replenishment\exceptions.py
type nul > warehouse_replenishment\main.py

:: Create package files
type nul > warehouse_replenishment\warehouse_replenishment\core\__init__.py
type nul > warehouse_replenishment\warehouse_replenishment\core\demand_forecast.py
type nul > warehouse_replenishment\warehouse_replenishment\core\safety_stock.py
type nul > warehouse_replenishment\warehouse_replenishment\core\lead_time.py
type nul > warehouse_replenishment\warehouse_replenishment\core\order_policy.py

type nul > warehouse_replenishment\warehouse_replenishment\services\__init__.py
type nul > warehouse_replenishment\warehouse_replenishment\services\vendor_service.py
type nul > warehouse_replenishment\warehouse_replenishment\services\item_service.py
type nul > warehouse_replenishment\warehouse_replenishment\services\order_service.py
type nul > warehouse_replenishment\warehouse_replenishment\services\exception_service.py
type nul > warehouse_replenishment\warehouse_replenishment\services\reporting_service.py

type nul > warehouse_replenishment\warehouse_replenishment\batch\__init__.py
type nul > warehouse_replenishment\warehouse_replenishment\batch\nightly_job.py
type nul > warehouse_replenishment\warehouse_replenishment\batch\weekly_job.py
type nul > warehouse_replenishment\warehouse_replenishment\batch\period_end_job.py
type nul > warehouse_replenishment\warehouse_replenishment\batch\time_based_params.py

type nul > warehouse_replenishment\warehouse_replenishment\utils\__init__.py
type nul > warehouse_replenishment\warehouse_replenishment\utils\date_utils.py
type nul > warehouse_replenishment\warehouse_replenishment\utils\math_utils.py
type nul > warehouse_replenishment\warehouse_replenishment\utils\validation.py

type nul > warehouse_replenishment\scripts\setup_db.py
type nul > warehouse_replenishment\scripts\import_data.py
type nul > warehouse_replenishment\config\settings.ini

echo Folder structure created successfully!