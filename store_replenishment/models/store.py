# app/models/store.py

"""
Path: app/models/store.py

Store/location master data.
"""

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from app.db import Base

class Store(Base):
    __tablename__ = 'store'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    location_code = Column(String, nullable=True)

    inventories = relationship("Inventory", back_populates="store")
    sales_histories = relationship("SalesHistory", back_populates="store")
    replenishment_orders = relationship("ReplenishmentOrder", back_populates="store")
    forecasts = relationship("Forecast", back_populates="store")
