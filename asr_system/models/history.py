"""
History exception models for the ASR system.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, ForeignKey, DateTime, Enum
from sqlalchemy.orm import relationship
import enum

from .base import BaseModel

class ExceptionType(enum.Enum):
    DEMAND_FILTER_HIGH = "demand_filter_high"
    DEMAND_FILTER_LOW = "demand_filter_low"
    TRACKING_SIGNAL_HIGH = "tracking_signal_high"
    TRACKING_SIGNAL_LOW = "tracking_signal_low"
    SERVICE_LEVEL_CHECK = "service_level_check"
    INFINITY_CHECK = "infinity_check"
    WATCH_SKU = "watch_sku"
    SEASONAL_SKU = "seasonal_sku"
    NEW_SKU = "new_sku"
    MANUAL_SKU = "manual_sku"
    DISCONTINUED_SKU = "discontinued_sku"


class HistoryException(BaseModel):
    """
    History exception model.
    Represents an exception identified during period-end processing.
    """
    __tablename__ = 'history_exceptions'
    
    sku_id = Column(Integer, ForeignKey('skus.id'), nullable=False)
    period_year = Column(Integer, nullable=False)
    period_number = Column(Integer, nullable=False)
    
    # Exception categorization
    exception_type = Column(Enum(ExceptionType), nullable=False)
    
    # Exception details
    old_forecast = Column(Float)
    new_forecast = Column(Float)
    actual_demand = Column(Float)
    old_madp = Column(Float)
    new_madp = Column(Float)
    old_track = Column(Float)
    new_track = Column(Float)
    
    # Service level details (for service level checks)
    service_level_goal = Column(Float)
    attained_service_level = Column(Float)
    
    # Status information
    is_reviewed = Column(Boolean, default=False)
    reviewed_by = Column(String)
    reviewed_at = Column(DateTime)
    action_taken = Column(String)
    
    # Relationships
    sku = relationship("SKU")
    
    def __repr__(self):
        return f"<HistoryException(sku_id={self.sku_id}, type={self.exception_type}, period={self.period_year}/{self.period_number})>"


class ArchivedException(BaseModel):
    """
    Archived exception model.
    Stores historical exception data for reporting and analysis.
    """
    __tablename__ = 'archived_exceptions'
    
    sku_id = Column(Integer, ForeignKey('skus.id'), nullable=False)
    period_year = Column(Integer, nullable=False)
    period_number = Column(Integer, nullable=False)
    
    # Exception categorization
    exception_type = Column(Enum(ExceptionType), nullable=False)
    
    # Exception details
    old_forecast = Column(Float)
    new_forecast = Column(Float)
    actual_demand = Column(Float)
    old_madp = Column(Float)
    new_madp = Column(Float)
    old_track = Column(Float)
    new_track = Column(Float)
    
    # Service level details (for service level checks)
    service_level_goal = Column(Float)
    attained_service_level = Column(Float)
    
    # Resolution information
    action_taken = Column(String)
    resolved_by = Column(String)
    resolved_at = Column(DateTime)
    
    # Relationships
    sku = relationship("SKU")
    
    def __repr__(self):
        return f"<ArchivedException(sku_id={self.sku_id}, type={self.exception_type}, period={self.period_year}/{self.period_number})>"