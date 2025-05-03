# Example ExceptionLog model
# app/models/exception_log.py
"""
Path: app/models/exception_log.py

Logs replenishment exceptions for reporting and buyer review.
"""
from sqlalchemy import Column, Integer, String, Date, ForeignKey
from app.db import Base

class ExceptionLog(Base):
    __tablename__ = 'exception_log'
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey('store.id'), nullable=False)
    sku_id = Column(Integer, ForeignKey('sku.id'), nullable=False)
    exception_date = Column(Date, nullable=False)
    exception_type = Column(String, nullable=False)
    description = Column(String, nullable=False)
