# app/models/inventory.py

"""
Path: app/models/inventory.py

Inventory position for each SKU at each store.
"""

from sqlalchemy import Column, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship
from app.db import Base

class Inventory(Base):
    __tablename__ = 'inventory'
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey('store.id'), nullable=False)
    sku_id = Column(Integer, ForeignKey('sku.id'), nullable=False)
    quantity_on_hand = Column(Integer, default=0)
    on_order = Column(Integer, default=0)
    backorder = Column(Integer, default=0)
    reserved = Column(Integer, default=0)

    store = relationship("Store", back_populates="inventories")
    sku = relationship("SKU", back_populates="inventories")
