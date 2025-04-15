# warehouse_replenishment/warehouse_replenishment/models.py
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Boolean, ForeignKey, Text, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum

Base = declarative_base()

class BuyerClassCode(enum.Enum):
    REGULAR = 'R'
    WATCH = 'W'
    MANUAL = 'M'
    DISCONTINUED = 'D'
    UNINITIALIZED = 'U'  # Added UNINITIALIZED to match usage in the Item class

class SystemClassCode(enum.Enum):
    REGULAR = 'R'
    SLOW = 'S'
    LUMPY = 'L'
    NEW = 'N'
    UNINITIALIZED = 'U'
    ALTERNATE = 'A'

class VendorType(enum.Enum):
    REGULAR = 'R'
    ALTERNATE = 'A'
    EDA = 'J'
    KITTING = 'K'
    TRANSFER = 'T'
    REGIONAL_WHS = 'W'

class ForecastMethod(enum.Enum):
    E3_REGULAR_AVS = 'E3_REGULAR_AVS'
    E3_ENHANCED_AVS = 'E3_ENHANCED_AVS'
    DEMAND_IMPORT = 'DEMAND_IMPORT'
    E3_OPT_FORECAST = 'E3_OPT_FORECAST'
    E3_ALTERNATE = 'E3_ALTERNATE'

class SafetyStockType(enum.Enum):
    NEVER = 0
    LESSER_OF = 1
    ALWAYS = 2

class Company(Base):
    __tablename__ = 'company'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    
    # Demand Forecasting Factors
    basic_alpha_factor = Column(Float, default=10.0)
    demand_from_days_out = Column(Integer, default=1)
    lumpy_demand_limit = Column(Float, default=50.0)
    slow_mover_limit = Column(Float, default=10.0)
    demand_filter_high = Column(Float, default=5.0)
    demand_filter_low = Column(Float, default=3.0)
    tracking_signal_limit = Column(Float, default=55.0)
    op_prime_limit_pct = Column(Float, default=95.0)
    forecast_demand_limit = Column(Float, default=5.0)
    update_frequency_impact_control = Column(Integer, default=2)
    
    # Management Factors
    service_level_goal = Column(Float, default=95.0)
    borrowing_rate = Column(Float, default=5.0)
    capital_cost_rate = Column(Float, default=25.0)
    physical_carrying_cost = Column(Float, default=15.0)
    other_rate = Column(Float, default=0.0)
    total_carrying_rate = Column(Float, default=40.0)
    gross_margin = Column(Float, default=35.0)
    overhead_rate = Column(Float, default=25.0)
    cost_of_lost_sales = Column(Float, default=100.0)
    
    # Acquisition Cost Parameters
    order_header_cost = Column(Float, default=25.0)
    order_line_cost = Column(Float, default=1.0)
    
    # Forward Buying Factors
    forward_buy_maximum = Column(Integer, default=60)
    forward_buy_filter = Column(Integer, default=30)
    discount_effect_rate = Column(Float, default=100.0)
    advertising_effect_rate = Column(Float, default=100.0)
    
    # Process Control
    keep_old_tb_parms_days = Column(Integer, default=30)
    keep_archived_exceptions_days = Column(Integer, default=90)
    lead_time_forecast_control = Column(Integer, default=1)
    
    # Other controls
    history_periodicity_default = Column(Integer, default=13)  # 13=4-weekly
    forecasting_periodicity_default = Column(Integer, default=13)  # 13=4-weekly
    
    warehouses = relationship("Warehouse", back_populates="company")

class Warehouse(Base):
    __tablename__ = 'warehouse'
    
    id = Column(Integer, primary_key=True)
    warehouse_id = Column(String(20), nullable=False, unique=True)
    name = Column(String(100), nullable=False)
    company_id = Column(Integer, ForeignKey('company.id'))
    
    # If warehouse-specific control factors are activated
    service_level_goal = Column(Float)
    lead_time_forecast_control = Column(Integer)
    warehouse_control_factors_active = Column(Boolean, default=False)
    
    company = relationship("Company", back_populates="warehouses")
    vendors = relationship("Vendor", back_populates="warehouse")
    items = relationship("Item", back_populates="warehouse")

class Vendor(Base):
    __tablename__ = 'vendor'
    
    id = Column(Integer, primary_key=True)
    vendor_id = Column(String(20), nullable=False)
    name = Column(String(100), nullable=False)
    warehouse_id = Column(Integer, ForeignKey('warehouse.id'))
    
    # Vendor Control Factors
    service_level_goal = Column(Float)
    order_cycle = Column(Integer)
    sub_vendor_approval = Column(Boolean, default=False)
    super_vendor_id = Column(String(20))
    super_vendor_warehouse = Column(String(20))
    regional_whs_id = Column(String(20))
    buyer_id = Column(String(20))
    vendor_type = Column(Enum(VendorType))
    lead_time_quoted = Column(Integer)
    lead_time_forecast = Column(Integer)
    lead_time_variance = Column(Float)
    active_items_count = Column(Integer, default=0)
    purchased_dollars_ytd = Column(Float, default=0.0)
    enable_history_adjust = Column(Boolean, default=False)
    current_bracket = Column(Integer, default=1)
    automatic_rebuild = Column(Integer, default=0)
    auto_approval_bracket = Column(Integer)
    supv_build_option = Column(Integer, default=0)
    vendor_group_codes = Column(String(100))
    
    # Order Control Factors
    deactivate_until = Column(Date)
    order_days_in_week = Column(String(7))  # e.g. "135" for Mon, Wed, Fri
    week = Column(Integer, default=0)  # 0=every, 1=odd, 2=even
    order_day_in_month = Column(Integer)
    next_order_date = Column(Date)
    history_periodicity = Column(Integer)
    forecasting_periodicity = Column(Integer)
    
    # Alternate Source Controls
    alternate_source_minimum = Column(Float, default=0.0)
    alternate_source_contract = Column(Boolean, default=False)
    
    # Plan Controls
    vendor_plan_control = Column(Boolean, default=False)
    order_cycle_as_plan_window = Column(Boolean, default=False)
    plan_warning_window = Column(Integer)
    
    # Transfer Controls
    exclude_from_auto_transfer = Column(Boolean, default=False)
    
    # Pricing Controls
    amount_as_net_cost = Column(Boolean, default=False)
    pricing_discount_sequence = Column(String(5))
    discount_sequence_option = Column(String(5))
    no_jmp_break_after_deal = Column(Boolean, default=False)
    
    # Order Overrides
    empirical_ss_buffering = Column(Boolean, default=False)
    order_when_minimum_met = Column(Boolean, default=False)
    item_cycle_maximum = Column(Integer)
    
    # E3 Enhanced AVS Controls
    forecasting_demand_limit = Column(Integer)
    
    # OPA Fields
    header_cost = Column(Float)
    line_cost = Column(Float)
    last_opa_date = Column(Date)
    last_opa_profit = Column(Float)
    last_opa_cycle = Column(Integer)
    
    warehouse = relationship("Warehouse", back_populates="vendors")
    brackets = relationship("VendorBracket", back_populates="vendor")
    items = relationship("Item", back_populates="vendor")
    
    __table_args__ = (
        # Unique constraint for vendor_id and warehouse_id combination
        # Ensure we don't have duplicate vendors in same warehouse
        {'sqlite_autoincrement': True},
    )

class VendorBracket(Base):
    __tablename__ = 'vendor_bracket'
    
    id = Column(Integer, primary_key=True)
    vendor_id = Column(Integer, ForeignKey('vendor.id'))
    bracket_number = Column(Integer, nullable=False)
    
    minimum = Column(Float, default=0.0)
    maximum = Column(Float, default=0.0)
    unit = Column(Integer, default=1)  # 1=amount, 2=each, 3=weight, etc.
    up_to_max_option = Column(Integer, default=0)
    
    # PO Discounts
    item_bracket_pricing = Column(Integer, default=0)
    discount = Column(Float, default=0.0)
    
    # OPA Factors
    savings_per_order = Column(Float, default=0.0)
    savings_unit = Column(Integer, default=1)
    savings_per_unit = Column(Float, default=0.0)
    savings_per_minimum = Column(Float, default=0.0)
    discount_pct_unconditional = Column(Float, default=0.0)
    savings_per_unit_unconditional = Column(Float, default=0.0)
    discount_pass_on = Column(Boolean, default=False)
    freight_amount_per_minimum = Column(Float, default=0.0)
    freight_amount_per_unit = Column(Float, default=0.0)
    freight_unit = Column(Integer, default=1)
    
    vendor = relationship("Vendor", back_populates="brackets")

class Item(Base):
    __tablename__ = 'item'
    
    id = Column(Integer, primary_key=True)
    item_id = Column(String(50), nullable=False)
    description = Column(String(255))
    vendor_id = Column(Integer, ForeignKey('vendor.id'))
    warehouse_id = Column(Integer, ForeignKey('warehouse.id'))
    
    # Item Detail
    service_level_goal = Column(Float)
    service_level_maintained = Column(Boolean, default=False)  # If service level was manually set
    service_level_attained = Column(Float, default=0.0)
    suggested_service_level = Column(Float)
    
    # Stock Status
    on_hand = Column(Float, default=0.0)
    on_order = Column(Float, default=0.0)
    customer_back_order = Column(Float, default=0.0)
    reserved = Column(Float, default=0.0)
    held_until = Column(Date)
    quantity_held = Column(Float, default=0.0)
    
    # Lead Time
    lead_time_forecast = Column(Integer)
    lead_time_variance = Column(Float)
    lead_time_maintained = Column(Boolean, default=False)  # If lead time was manually set
    calculated_in_days = Column(Integer)
    calculated_variance = Column(Float)
    lead_time_profile = Column(String(20))
    fill_in_lead_time = Column(Integer)
    
    # Item Parameters
    units_per_case = Column(Float, default=1.0)
    weight_per_unit = Column(Float, default=0.0)
    volume_per_unit = Column(Float, default=0.0)
    units_per_layer = Column(Float, default=0.0)
    units_per_pallet = Column(Float, default=0.0)
    buying_multiple = Column(Float, default=1.0)
    minimum_quantity = Column(Float, default=1.0)
    convenience_pack = Column(Float, default=0.0)
    conv_pk_breakpoint = Column(Float, default=0.0)
    number_of_conv_packs = Column(Integer, default=0)
    shelf_life_days = Column(Integer, default=0)
    out_of_stock_point = Column(Float, default=0.0)
    
    # Demand Forecasting
    buyer_class = Column(Enum(BuyerClassCode), default=BuyerClassCode.UNINITIALIZED)
    system_class = Column(Enum(SystemClassCode), default=SystemClassCode.UNINITIALIZED)
    forecast_method = Column(Enum(ForecastMethod), default=ForecastMethod.E3_REGULAR_AVS)
    forecasting_periodicity = Column(Integer)
    history_periodicity = Column(Integer)
    
    # Item classification
    item_group_codes = Column(String(100))
    
    # Forecast data
    demand_weekly = Column(Float, default=0.0)
    demand_4weekly = Column(Float, default=0.0)
    demand_monthly = Column(Float, default=0.0)
    demand_quarterly = Column(Float, default=0.0)
    demand_yearly = Column(Float, default=0.0)
    forecast_date = Column(DateTime)
    madp = Column(Float, default=0.0)
    track = Column(Float, default=0.0)
    sstf = Column(Float, default=0.0)  # Safety Stock Time Factor
    freeze_until_date = Column(Date)
    demand_profile = Column(String(20))
    
    # Manual max/min controls
    buyer_max = Column(Float, default=0.0)
    buyer_min = Column(Float, default=0.0)
    type_for_min_max = Column(String(1), default='U')  # 'U'=Units, 'D'=Days
    
    # Manual Safety Stock
    manual_ss = Column(Float, default=0.0)
    ss_type = Column(Enum(SafetyStockType), default=SafetyStockType.NEVER)
    
    # Price information
    purchase_price = Column(Float, default=0.0)
    purchase_price_divisor = Column(Float, default=1.0)
    sales_price = Column(Float, default=0.0)
    carrying_cost_adjustments = Column(Float, default=0.0)
    handling_cost_adjustments = Column(Float, default=0.0)
    
    # Supersession relationships
    supersede_to_item_id = Column(String(50))
    supersede_from_item_id = Column(String(50))
    
    # Calculated fields for ordering
    item_order_point_units = Column(Float, default=0.0)  # IOP
    item_order_point_days = Column(Float, default=0.0)
    vendor_order_point_days = Column(Float, default=0.0)  # VOP
    order_up_to_level_units = Column(Float, default=0.0)  # OUTL
    order_up_to_level_days = Column(Float, default=0.0)
    item_cycle_units = Column(Float, default=0.0)  # ICYC
    item_cycle_days = Column(Float, default=0.0)
    
    vendor = relationship("Vendor", back_populates="items")
    warehouse = relationship("Warehouse", back_populates="items")
    demand_history = relationship("DemandHistory", back_populates="item")
    item_prices = relationship("ItemPrice", back_populates="item")
    
    __table_args__ = (
        # Unique constraint for item_id, vendor_id and warehouse_id combination
        {'sqlite_autoincrement': True},
    )

class DemandHistory(Base):
    __tablename__ = 'demand_history'
    
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('item.id'))
    period_number = Column(Integer, nullable=False)
    period_year = Column(Integer, nullable=False)
    
    shipped = Column(Float, default=0.0)
    lost_sales = Column(Float, default=0.0)
    promotional_demand = Column(Float, default=0.0)
    total_demand = Column(Float, default=0.0)
    
    is_ignored = Column(Boolean, default=False)
    is_adjusted = Column(Boolean, default=False)
    out_of_stock_days = Column(Integer, default=0)
    
    item = relationship("Item", back_populates="demand_history")

class ItemPrice(Base):
    __tablename__ = 'item_price'
    
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('item.id'))
    bracket_number = Column(Integer, nullable=False)
    
    price = Column(Float, default=0.0)
    
    item = relationship("Item", back_populates="item_prices")

class Order(Base):
    __tablename__ = 'order'
    
    id = Column(Integer, primary_key=True)
    vendor_id = Column(Integer, ForeignKey('vendor.id'))
    warehouse_id = Column(Integer, ForeignKey('warehouse.id'))
    order_date = Column(DateTime, default=func.now())
    
    # Order status
    is_due = Column(Boolean, default=False)
    is_order_point_a = Column(Boolean, default=False)
    is_order_point = Column(Boolean, default=False)
    order_delay = Column(Integer, default=0)  # Days until order will be due
    
    # Order totals
    independent_amount = Column(Float, default=0.0)
    independent_eaches = Column(Float, default=0.0)
    independent_weight = Column(Float, default=0.0)
    independent_volume = Column(Float, default=0.0)
    independent_dozens = Column(Float, default=0.0)
    independent_cases = Column(Float, default=0.0)
    
    auto_adj_amount = Column(Float, default=0.0)
    auto_adj_eaches = Column(Float, default=0.0)
    auto_adj_weight = Column(Float, default=0.0)
    auto_adj_volume = Column(Float, default=0.0)
    auto_adj_dozens = Column(Float, default=0.0)
    auto_adj_cases = Column(Float, default=0.0)
    
    final_adj_amount = Column(Float, default=0.0)
    final_adj_eaches = Column(Float, default=0.0)
    final_adj_weight = Column(Float, default=0.0)
    final_adj_volume = Column(Float, default=0.0)
    final_adj_dozens = Column(Float, default=0.0)
    final_adj_cases = Column(Float, default=0.0)
    
    extra_days = Column(Float, default=0.0)
    current_bracket = Column(Integer, default=1)
    
    # Order status
    status = Column(String(20), default='OPEN')  # OPEN, ACCEPTED, PURGED
    expected_delivery_date = Column(Date)
    approval_date = Column(DateTime)
    
    # Checks
    order_point_checks = Column(Integer, default=0)
    planned_checks = Column(Integer, default=0)
    forward_checks = Column(Integer, default=0)
    deal_checks = Column(Integer, default=0)
    shelf_life_checks = Column(Integer, default=0)
    uninitialized_checks = Column(Integer, default=0)
    watch_checks = Column(Integer, default=0)
    
    order_items = relationship("OrderItem", back_populates="order")

class OrderItem(Base):
    __tablename__ = 'order_item'
    
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey('order.id'))
    item_id = Column(Integer, ForeignKey('item.id'))
    
    # Quantity details
    soq_units = Column(Float, default=0.0)
    soq_days = Column(Float, default=0.0)
    is_frozen = Column(Boolean, default=False)
    
    # Check flags
    is_order_point = Column(Boolean, default=False)
    is_manual = Column(Boolean, default=False)
    is_deal = Column(Boolean, default=False)
    is_planned = Column(Boolean, default=False)
    is_forward_buy = Column(Boolean, default=False)
    
    # For tracking why SOQ was generated
    item_order_point_units = Column(Float)
    balance_units = Column(Float)
    order_up_to_level_units = Column(Float)
    
    order = relationship("Order", back_populates="order_items")

class SeasonalProfile(Base):
    __tablename__ = 'seasonal_profile'
    
    id = Column(Integer, primary_key=True)
    profile_id = Column(String(20), nullable=False, unique=True)
    description = Column(String(255))
    periodicity = Column(Integer, nullable=False)  # 12 or 52
    
    indices = relationship("SeasonalProfileIndex", back_populates="profile")

class SeasonalProfileIndex(Base):
    __tablename__ = 'seasonal_profile_index'
    
    id = Column(Integer, primary_key=True)
    profile_id = Column(String(20), ForeignKey('seasonal_profile.profile_id'))
    period_number = Column(Integer, nullable=False)
    index_value = Column(Float, nullable=False)
    
    profile = relationship("SeasonalProfile", back_populates="indices")

class HistoryException(Base):
    __tablename__ = 'history_exception'
    
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('item.id'))
    exception_type = Column(String(50), nullable=False)  # DEMAND_FILTER_HIGH, DEMAND_FILTER_LOW, etc.
    creation_date = Column(DateTime, default=func.now())
    period_number = Column(Integer, nullable=False)
    period_year = Column(Integer, nullable=False)
    
    # Exception details
    forecast_value = Column(Float)
    actual_value = Column(Float)
    madp = Column(Float)
    track = Column(Float)
    notes = Column(Text)
    
    # Resolution status
    is_resolved = Column(Boolean, default=False)
    resolution_date = Column(DateTime)
    resolution_action = Column(String(50))
    resolution_notes = Column(Text)

class ManagementException(Base):
    __tablename__ = 'management_exception'
    
    id = Column(Integer, primary_key=True)
    warehouse_id = Column(Integer, ForeignKey('warehouse.id'))
    exception_type = Column(String(50), nullable=False)  # TOP_SELLING_ITEMS, FORECAST_GREATER_THAN_AVERAGE, etc.
    
    # Exception parameters
    parameter_x = Column(Float)
    parameter_y = Column(Float)
    is_enabled = Column(Boolean, default=True)

class ManagementExceptionItem(Base):
    __tablename__ = 'management_exception_item'
    
    id = Column(Integer, primary_key=True)
    exception_id = Column(Integer, ForeignKey('management_exception.id'))
    item_id = Column(Integer, ForeignKey('item.id'))
    creation_date = Column(DateTime, default=func.now())
    
    # Exception details
    value_x = Column(Float)
    value_y = Column(Float)
    notes = Column(Text)
    
    # Resolution status
    is_resolved = Column(Boolean, default=False)
    resolution_date = Column(DateTime)
    resolution_action = Column(String(50))
    resolution_notes = Column(Text)

class TimeBasedParameter(Base):
    __tablename__ = 'time_based_parameter'
    
    id = Column(Integer, primary_key=True)
    description = Column(String(255), nullable=False)
    parameter_type = Column(String(50), nullable=False)  # DEMAND_FORECAST, VENDOR_LEAD_TIME, etc.
    effective_date = Column(Date, nullable=False)
    expression = Column(String(255), nullable=False)
    buyer_id = Column(String(20))
    creation_date = Column(DateTime, default=func.now())
    status = Column(String(20), default='PENDING')  # PENDING, APPROVED, APPLIED, ERROR
    comment = Column(Text)

class TimeBasedParameterItem(Base):
    __tablename__ = 'time_based_parameter_item'
    
    id = Column(Integer, primary_key=True)
    parameter_id = Column(Integer, ForeignKey('time_based_parameter.id'))
    item_id = Column(Integer, ForeignKey('item.id'))
    
    effective_date = Column(Date)
    expression = Column(String(255))
    error_message = Column(Text)

class SuperVendorMember(Base):
    __tablename__ = 'super_vendor_member'
    
    id = Column(Integer, primary_key=True)
    super_vendor_id = Column(Integer, ForeignKey('vendor.id'))
    member_vendor_id = Column(Integer, ForeignKey('vendor.id'))

class SubVendorItem(Base):
    __tablename__ = 'sub_vendor_item'
    
    id = Column(Integer, primary_key=True)
    main_vendor_id = Column(Integer, ForeignKey('vendor.id'))
    sub_vendor_id = Column(String(20), nullable=False)
    item_id = Column(Integer, ForeignKey('item.id'))

class ArchivedHistoryException(Base):
    __tablename__ = 'archived_history_exception'
    
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('item.id'))
    exception_type = Column(String(50), nullable=False)
    creation_date = Column(DateTime)
    resolution_date = Column(DateTime)
    period_number = Column(Integer)
    period_year = Column(Integer)
    
    # Values before change
    before_forecast = Column(Float)
    before_madp = Column(Float)
    before_track = Column(Float)
    before_profile_id = Column(String(20))
    
    # Values after change
    after_forecast = Column(Float)
    after_madp = Column(Float)
    after_track = Column(Float)
    after_profile_id = Column(String(20))
    
    resolution_action = Column(String(50))
    resolution_notes = Column(Text)