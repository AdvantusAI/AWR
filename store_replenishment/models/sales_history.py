# app/models/sales_history.py

"""
Path: app/models/sales_history.py

Sales history for each SKU at each store (for forecasting).
"""

from sqlalchemy import Column, Integer, Float, Date, ForeignKey
from sqlalchemy.orm import relationship
from app.db import Base

class SalesHistory(Base):
    __tablename__ = 'sales_history'
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey('store.id'), nullable=False)
    sku_id = Column(Integer, ForeignKey('sku.id'), nullable=False)
    sale_date = Column(Date, nullable=False)
    quantity_sold = Column(Float, default=0.0)

    store = relationship("Store", back_populates="sales_histories")
    sku = relationship("SKU", back_populates="sales_histories")
