"""
SKU models for the ASR system.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.associationproxy import association_proxy

from .base import BaseModel

class SKU(BaseModel):
    """
    SKU (Stock Keeping Unit) model.
    Represents a unique item at a specific location.
    """
    __tablename__ = 'skus'
    
    sku_id = Column(String, nullable=False, index=True)
    store_id = Column(String, nullable=False, index=True)
    source_id = Column(String, ForeignKey('sources.source_id'), nullable=False)
    name = Column(String, nullable=False)
    description = Column(String)
    
    # Group codes for reporting
    group_code_1 = Column(String)  # e.g., department
    group_code_2 = Column(String)  # e.g., class
    group_code_3 = Column(String)  # e.g., subclass
    
    # Buyer class code (R=Regular, W=Watch, M=Manual, D=Discontinued)
    buyer_class = Column(String, default='U')  # Default to Uninitialized
    
    # System class code (S=Slow, L=Lumpy, U=Uninitialized, R=Regular, N=New, A=Alternate)
    system_class = Column(String, default='U')
    
    # Pricing information
    purchase_price = Column(Float, default=0.0)
    sales_price = Column(Float, default=0.0)
    
    # Specification information
    weight = Column(Float, default=0.0)  # Weight per unit
    cube = Column(Float, default=0.0)    # Volume per unit
    
    # Ordering constraints
    buying_multiple = Column(Integer, default=1)  # Minimum ordering multiple
    minimum_quantity = Column(Integer, default=1)  # Minimum order quantity
    shelf_life_days = Column(Integer)  # Max shelf life days
    
    # Pack sizes
    units_per_case = Column(Integer, default=1)
    units_per_layer = Column(Integer)
    units_per_pallet = Column(Integer)
    
    # Service level information
    service_level_goal = Column(Float)  # Service level goal (%)
    attained_service_level = Column(Float)  # Actual attained service level
    
    # Forecasting fields
    demand_profile_id = Column(String)  # Seasonal profile ID
    freeze_forecast_until = Column(DateTime)  # Date until forecast is frozen
    
    # Lead time information
    lead_time_forecast = Column(Float)  # Lead time in days
    lead_time_variance = Column(Float)  # Lead time variance (%)
    
    # Safety stock settings
    manual_safety_stock = Column(Float)  # Manual safety stock value
    safety_stock_type = Column(Integer, default=0)  # 0=Never, 1=Lesser Of, 2=Always
    min_presentation_stock = Column(Float)  # Minimum presentation stock
    
    # Flag fields
    ignore_multiple = Column(Boolean, default=False)  # Ignore buying multiple
    
    # Relationships
    source = relationship("Source", back_populates="skus")
    stock_status = relationship("StockStatus", uselist=False, back_populates="sku")
    demand_history = relationship("DemandHistory", back_populates="sku")
    
    def __repr__(self):
        return f"<SKU(sku_id='{self.sku_id}', store_id='{self.store_id}', name='{self.name}')>"


class StockStatus(BaseModel):
    """
    Stock status model.
    Represents the current inventory status of a SKU.
    """
    __tablename__ = 'stock_status'
    
    sku_id = Column(String, nullable=False)
    store_id = Column(String, nullable=False)
    sku_store_id = Column(Integer, ForeignKey('skus.id'), nullable=False, unique=True)
    
    on_hand = Column(Float, default=0.0)  # Current on-hand inventory
    on_order = Column(Float, default=0.0)  # Current on-order inventory
    customer_back_order = Column(Float, default=0.0)  # Customer back orders
    reserved = Column(Float, default=0.0)  # Reserved quantity
    quantity_held = Column(Float, default=0.0)  # Quantity held until date
    held_until = Column(DateTime)  # Date until quantity is held
    
    # Calculated fields
    available_balance = Column(Float, default=0.0)  # On hand + On order - Back order - Reserved - Held
    
    # Overstock fields
    is_overstock = Column(Boolean, default=False)  # Is this SKU overstocked?
    overstock_quantity = Column(Float, default=0.0)  # Quantity of overstock
    
    # Relationships
    sku = relationship("SKU", back_populates="stock_status")
    
    def __repr__(self):
        return f"<StockStatus(sku_id='{self.sku_id}', store_id='{self.store_id}', on_hand={self.on_hand})>"


class DemandHistory(BaseModel):
    """
    Demand history model.
    Stores historical demand data for each SKU by period.
    """
    __tablename__ = 'demand_history'
    
    sku_id = Column(String, nullable=False)
    store_id = Column(String, nullable=False)
    sku_store_id = Column(Integer, ForeignKey('skus.id'), nullable=False)
    
    period_year = Column(Integer, nullable=False)  # Year of the period
    period_number = Column(Integer, nullable=False)  # Period number (1-13 or 1-52)
    
    units_sold = Column(Float, default=0.0)  # Units sold in period
    units_lost = Column(Float, default=0.0)  # Lost sales in period
    promotional_demand = Column(Float, default=0.0)  # Promotional demand in period
    
    # Calculated total demand
    total_demand = Column(Float, default=0.0)  # units_sold + units_lost - promotional_demand
    
    # Ignore flag for forecast calculation
    ignore_history = Column(Boolean, default=False)  # Ignore this period in forecast calculation
    
    # Relationships
    sku = relationship("SKU", back_populates="demand_history")
    
    def __repr__(self):
        return f"<DemandHistory(sku_id='{self.sku_id}', period_year={self.period_year}, period_number={self.period_number})>"


class ForecastData(BaseModel):
    """
    Forecast data model.
    Stores the current forecast data for each SKU.
    """
    __tablename__ = 'forecast_data'
    
    sku_id = Column(String, nullable=False)
    store_id = Column(String, nullable=False)
    sku_store_id = Column(Integer, ForeignKey('skus.id'), nullable=False, unique=True)
    
    # Forecast values
    weekly_forecast = Column(Float)  # Weekly demand forecast
    period_forecast = Column(Float)  # 4-weekly or monthly forecast
    quarterly_forecast = Column(Float)  # Quarterly forecast
    yearly_forecast = Column(Float)  # Yearly forecast
    
    # Forecast quality metrics
    madp = Column(Float)  # Mean Absolute Deviation Percentage
    track = Column(Float)  # Track/Trend Signal
    
    # Forecast metadata
    last_forecast_date = Column(DateTime)  # Date of last forecast
    last_manual_forecast_date = Column(DateTime)  # Date of last manual forecast adjustment
    
    def __repr__(self):
        return f"<ForecastData(sku_id='{self.sku_id}', store_id='{self.store_id}', weekly_forecast={self.weekly_forecast})>"


class SeasonalProfile(BaseModel):
    """
    Seasonal profile model.
    Stores the seasonal indices for each period of the year.
    """
    __tablename__ = 'seasonal_profiles'
    
    profile_id = Column(String, nullable=False, unique=True)
    description = Column(String)
    
    # Store indices as a JSON field or individual columns
    # For simplicity, we'll use individual columns for 13 periods
    p1_index = Column(Float, default=1.0)
    p2_index = Column(Float, default=1.0)
    p3_index = Column(Float, default=1.0)
    p4_index = Column(Float, default=1.0)
    p5_index = Column(Float, default=1.0)
    p6_index = Column(Float, default=1.0)
    p7_index = Column(Float, default=1.0)
    p8_index = Column(Float, default=1.0)
    p9_index = Column(Float, default=1.0)
    p10_index = Column(Float, default=1.0)
    p11_index = Column(Float, default=1.0)
    p12_index = Column(Float, default=1.0)
    p13_index = Column(Float, default=1.0)
    
    def __repr__(self):
        return f"<SeasonalProfile(profile_id='{self.profile_id}')>"