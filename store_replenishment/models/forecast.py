# app/models/forecast.py

"""
Path: app/models/forecast.py

Stores daily/periodic demand forecasts for each SKU/store.
"""

from sqlalchemy import Column, Integer, Float, String, Date, ForeignKey
from sqlalchemy.orm import relationship
from app.db import Base

class Forecast(Base):
    __tablename__ = 'forecast'
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey('store.id'), nullable=False)
    sku_id = Column(Integer, ForeignKey('sku.id'), nullable=False)
    forecast_date = Column(Date)
    forecast_qty = Column(Float)
    model_used = Column(String)
    promo_factor = Column(Float, default=1.0)
    season_factor = Column(Float, default=1.0)
    manual_override = Column(Float, nullable=True)

    store = relationship("Store", back_populates="forecasts")
    sku = relationship("SKU", back_populates="forecasts")
