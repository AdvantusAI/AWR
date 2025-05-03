# app/models/vendor.py

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from app.db import Base

class Vendor(Base):
    __tablename__ = 'vendor'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    brackets = relationship("Bracket", back_populates="vendor")
    service_level_goal = Column(Float, nullable=True)  # Can be null, fallback 
