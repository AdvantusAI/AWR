# warehouse_replenishment/models.py
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Boolean, ForeignKey, Text, Enum, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum

Base = declarative_base()

class BuyerClassCode(enum.Enum):
    """Enum for buyer class codes.
    
    Values:
        REGULAR ('R'): Regular items that are actively managed
        WATCH ('W'): Items that need special attention
        MANUAL ('M'): Items that require manual intervention
        DISCONTINUED ('D'): Items that are no longer active
        UNINITIALIZED ('U'): Items that haven't been classified yet
    """
    REGULAR = 'R'
    WATCH = 'W'
    MANUAL = 'M'
    DISCONTINUED = 'D'
    UNINITIALIZED = 'U'

    def __str__(self):
        """Return the string value of the enum."""
        return self.value

    @classmethod
    def from_string(cls, value: str) -> 'BuyerClassCode':
        """Create a BuyerClassCode from a string value.
        
        Args:
            value: String value ('R', 'W', 'M', 'D', 'U')
            
        Returns:
            BuyerClassCode enum value
            
        Raises:
            ValueError if the string value is not valid
        """
        try:
            return cls(value)
        except ValueError:
            raise ValueError(f"Invalid buyer class code: {value}. Valid values are: R, W, M, D, U")

class SystemClassCode(enum.Enum):
    REGULAR = 'R'
    SLOW = 'S'
    LUMPY = 'L'
    NEW = 'N'
    UNINITIALIZED = 'U'
    ALTERNATE = 'A'

class VendorType(enum.Enum):
    REGULAR = BuyerClassCode.REGULAR      # Standard vendor for normal inventory replenishment
                            # Business Impact: 
                            # - Primary source for regular inventory items
                            # - Subject to standard order cycles and lead times
                            # - Included in automatic order generation
                            # - Uses standard pricing and discount structures
                            # - Key for maintaining normal inventory levels

    ALTERNATE = 'ALTERNATE'  # Secondary/backup vendor for specific items
                            # Business Impact:
                            # - Used when primary vendor is unavailable
                            # - Helps maintain supply chain resilience
                            # - Often has different pricing structures
                            # - May have different lead times and order cycles
                            # - Critical for risk management and continuity

    EDA = 'EDA'             # Emergency/Disaster Alternative vendor
                            # Business Impact:
                            # - Specialized for emergency situations
                            # - May have expedited shipping options
                            # - Often has premium pricing
                            # - Critical for business continuity
                            # - Used when regular supply chains are disrupted

    KITTING = 'KITTING'      # Vendor for assembly/kit operations
                            # Business Impact:
                            # - Handles product assembly and packaging
                            # - May have special order requirements
                            # - Often involves multiple components
                            # - Critical for value-added services
                            # - May have different lead time calculations

    TRANSFER = 'TRANSFER'    # Internal transfer between warehouses
                            # Business Impact:
                            # - Manages internal inventory movement
                            # - No external purchasing involved
                            # - Used for warehouse balancing
                            # - Helps optimize inventory distribution
                            # - Critical for multi-warehouse operations

    REGIONAL_WHS = 'REGIONAL_WHS'  # Regional warehouse vendor
                                   # Business Impact:
                                   # - Manages regional inventory distribution
                                   # - May have different service level goals
                                   # - Often involves cross-docking operations
                                   # - Critical for regional market coverage
                                   # - Helps optimize regional inventory levels

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
    #warehouse_id = Column(Integer, ForeignKey('warehouse.id'))
    warehouse_id = Column(String(20), ForeignKey('warehouse.warehouse_id')) 
    
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
    deactivation_reason = Column(String(255))  # Reason for vendor deactivation
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
    """Item model for inventory management."""
    __tablename__ = 'item'

    id = Column(Integer, primary_key=True)
    item_id = Column(String(50), nullable=False)
    description = Column(String(255))
    vendor_id = Column(Integer, ForeignKey('vendor.id'), nullable=False)
    warehouse_id = Column(Integer, ForeignKey('warehouse.id'), nullable=False)
    service_level_goal = Column(Float)
    service_level_maintained = Column(Boolean, default=False)
    lead_time_forecast = Column(Integer)
    lead_time_variance = Column(Float)
    lead_time_maintained = Column(Boolean, default=False)
    buying_multiple = Column(Float, default=1.0)
    minimum_quantity = Column(Float, default=1.0)
    purchase_price = Column(Float, default=0.0)
    sales_price = Column(Float, default=0.0)
    buyer_id = Column(String(50))
    buyer_class = Column(String(1), default='U')  # Changed from Enum(BuyerClassCode) to String(1)
    on_hand = Column(Float, default=0.0)
    on_order = Column(Float, default=0.0)
    customer_back_order = Column(Float, default=0.0)
    reserved = Column(Float, default=0.0)
    held_until = Column(Date)
    quantity_held = Column(Float, default=0.0)
    auxiliary_balance = Column(Float, default=0.0)
    sstf = Column(Float)  # Safety Stock Time Factor
    item_order_point_days = Column(Float)
    item_order_point_units = Column(Float)
    vendor_order_point_days = Column(Float)
    order_up_to_level_days = Column(Float)
    order_up_to_level_units = Column(Float)
    item_cycle_days = Column(Float)
    demand_weekly = Column(Float, default=0.0)
    demand_4weekly = Column(Float, default=0.0)
    demand_monthly = Column(Float, default=0.0)
    demand_quarterly = Column(Float, default=0.0)
    demand_yearly = Column(Float, default=0.0)
    madp = Column(Float, default=0.0)  # Mean Absolute Deviation Percentage
    track = Column(Float, default=0.0)
    service_level_attained = Column(Float)
    manual_ss = Column(Float)
    manual_ss_type = Column(String(50))

    # Relationships
    vendor = relationship("Vendor", back_populates="items")
    warehouse = relationship("Warehouse", back_populates="items")
    demand_history = relationship("DemandHistory", back_populates="item")
    item_forecast = relationship("ItemForecast", back_populates="item")
    order_items = relationship("OrderItem", back_populates="item")
    item_prices = relationship("ItemPrice", back_populates="item")

    @property
    def buyer_class_enum(self) -> BuyerClassCode:
        """Get the buyer class as an enum value."""
        return BuyerClassCode.from_string(self.buyer_class)

    @buyer_class_enum.setter
    def buyer_class_enum(self, value: BuyerClassCode):
        """Set the buyer class from an enum value."""
        self.buyer_class = value.value

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
    warehouse_id = Column(String(20), ForeignKey('warehouse.warehouse_id'))  # Change to String
    #warehouse_id = Column(Integer, ForeignKey('warehouse.id'))
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
    
    # Relationships
    order = relationship("Order", back_populates="order_items")
    item = relationship("Item", back_populates="order_items")

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
    #warehouse_id = Column(Integer, ForeignKey('warehouse.id'))
    warehouse_id = Column(String(20), ForeignKey('warehouse.warehouse_id'))  # Change to String
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

# Add this to your models.py file

class ItemForecast(Base):
    """Model for tracking forecast history and accuracy over time."""
    __tablename__ = 'item_forecast'
    
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('item.id'), nullable=False)
    forecast_date = Column(DateTime, default=func.now(), nullable=False)
    period_number = Column(Integer, nullable=False)
    period_year = Column(Integer, nullable=False)
    
    # Forecast values
    forecast_value = Column(Float, default=0.0)
    madp = Column(Float, default=0.0)
    track = Column(Float, default=0.0)
    
    # Forecast method and parameters
    forecast_method = Column(Enum(ForecastMethod), default=ForecastMethod.E3_REGULAR_AVS)
    seasonality_applied = Column(Boolean, default=False)
    seasonal_profile_id = Column(String(20))
    
    # Actual values (filled after the period)
    actual_value = Column(Float)
    error = Column(Float)
    error_pct = Column(Float)
    
    # Additional metadata
    notes = Column(Text)
    created_by = Column(String(50))
    
    # Relationships
    item = relationship("Item", back_populates="item_forecast")
    
    __table_args__ = (
        # Index for faster lookups by item and period
        Index('idx_item_forecast_item_period', 'item_id', 'period_year', 'period_number'),
        # Index for faster lookups by date
        Index('idx_item_forecast_date', 'forecast_date'),
    )