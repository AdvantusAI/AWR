# app/models/sku.py
"""
Path: app/models/sku.py

SKU/item master data and replenishment parameters.
"""
from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship
from app.db import Base

class SKU(Base):
    __tablename__ = 'sku'
    id = Column(Integer, primary_key=True)
    sku_code = Column(String, unique=True, nullable=False)
    description = Column(String)
    vendor_id = Column(Integer, ForeignKey('vendor.id'))
    min_order_qty = Column(Integer, default=1)
    max_order_qty = Column(Integer, nullable=True)
    buying_multiple = Column(Integer, default=1)
    safety_stock = Column(Float, default=0.0)
    presentation_stock = Column(Float, nullable=True)
    event_minimum = Column(Float, nullable=True)
    order_cycle_days = Column(Integer, default=7)
    lead_time_forecast = Column(Float, nullable=True)
    lead_time_variance_pct = Column(Float, nullable=True)
    service_level_goal = Column(Float, nullable=True)

    vendor = relationship("Vendor", back_populates="skus")
    inventories = relationship("Inventory", back_populates="sku")
    sales_histories = relationship("SalesHistory", back_populates="sku")
    replenishment_orders = relationship("ReplenishmentOrder", back_populates="sku")
    forecasts = relationship("Forecast", back_populates="sku")
