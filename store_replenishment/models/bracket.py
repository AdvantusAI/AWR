# app/models/bracket.py

from sqlalchemy import Column, Integer, Float, String, ForeignKey
from sqlalchemy.orm import relationship
from app.db import Base

class Bracket(Base):
    __tablename__ = 'bracket'
    id = Column(Integer, primary_key=True)
    vendor_id = Column(Integer, ForeignKey('vendor.id'))
    min_qty = Column(Float)
    max_qty = Column(Float)
    unit = Column(String)
    discount_pct = Column(Float)
    vendor = relationship("Vendor", back_populates="brackets")
