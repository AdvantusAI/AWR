# app/models/replenishment_order.py

"""
Path: app/models/replenishment_order.py

Suggested or actual replenishment orders for each SKU/store.
"""

from sqlalchemy import Column, Integer, Float, String, Date, ForeignKey
from sqlalchemy.orm import relationship
from app.db import Base

class ReplenishmentOrder(Base):
    __tablename__ = 'replenishment_order'
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey('store.id'), nullable=False)
    sku_id = Column(Integer, ForeignKey('sku.id'), nullable=False)
    order_date = Column(Date)
    quantity_ordered = Column(Float)
    status = Column(String, default='Pending')  # e.g., Pending, Approved, Completed

    store = relationship("Store", back_populates="replenishment_orders")
    sku = relationship("SKU", back_populates="replenishment_orders")
