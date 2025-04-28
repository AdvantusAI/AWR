"""
Source models for the ASR system.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.associationproxy import association_proxy

from .base import BaseModel

class Source(BaseModel):
    """
    Source model.
    Represents a vendor or distribution center that supplies items.
    """
    __tablename__ = 'sources'
    
    source_id = Column(String, nullable=False, unique=True)
    name = Column(String, nullable=False)
    source_type = Column(String, nullable=False)  # 'V' for Vendor, 'D' for DC
    
    # Buyer information
    buyer_id = Column(String)
    
    # Group codes for reporting
    group_code_1 = Column(String)
    group_code_2 = Column(String)
    group_code_3 = Column(String)
    
    # Order control factors
    service_level_default = Column(Float, default=95.0)  # Default service level for SKUs
    order_cycle = Column(Integer)  # Order cycle in days
    
    # Lead time information
    lead_time_quoted = Column(Integer)  # Quoted lead time in days
    lead_time_forecast = Column(Integer)  # Forecasted lead time in days
    lead_time_variance = Column(Float)  # Lead time variance (%)
    
    # Order building controls
    current_bracket = Column(Integer, default=0)  # Current bracket (0-9)
    automatic_rebuild = Column(Integer, default=0)  # 0=None, 1=Forward, 2=Min, 3=Both, 4=Current, 5=All
    order_precision = Column(Boolean, default=False)  # True to build to bracket max precisely
    
    # Fixed order cycle settings
    order_days_in_week = Column(String)  # Days to order (1-7 for Mon-Sun)
    order_week = Column(Integer)  # 0=Every, 1=Odd, 2=Even
    order_day_in_month = Column(Integer)  # Fixed day of month to order
    
    # Order status information
    next_order_date = Column(DateTime)  # Next scheduled order date
    last_order_date = Column(DateTime)  # Last order date
    
    # Relationships
    skus = relationship("SKU", back_populates="source")
    brackets = relationship("SourceBracket", back_populates="source", cascade="all, delete-orphan")
    orders = relationship("Order", back_populates="source")
    
    # Super/Sub source relationships
    is_super_source = Column(Boolean, default=False)  # Is this a super source?
    sub_source_approval = Column(Boolean, default=False)  # Require sub source approval?
    parent_source_id = Column(Integer, ForeignKey('sources.id'), nullable=True)  # Parent source ID for sub sources
    
    # If super source, related member sources
    members = relationship("Source", 
                          backref=relationship("Source", remote_side=[id]),
                          foreign_keys=[parent_source_id])
    
    # Order viewing code for super sources
    view_order_code = Column(Integer, default=0)  # 0=Both, 1=Super only
    
    def __repr__(self):
        return f"<Source(id={self.id}, source_id='{self.source_id}', name='{self.name}')>"


class SourceBracket(BaseModel):
    """
    Source bracket model.
    Represents discount brackets offered by a source.
    """
    __tablename__ = 'source_brackets'
    
    source_id = Column(Integer, ForeignKey('sources.id'), nullable=False)
    bracket_number = Column(Integer, nullable=False)  # 1-9
    
    # Bracket constraints
    minimum = Column(Float, default=0.0)  # Minimum value for bracket
    maximum = Column(Float, default=0.0)  # Maximum value for bracket
    unit = Column(Integer)  # Unit type (1=Amount, 2=Eaches, 3=Weight, etc.)
    
    # Bracket options
    up_to_max_option = Column(Boolean, default=False)  # Build to maximum?
    
    # Discount information
    sku_bracket_pricing = Column(Integer, default=0)  # 0=None, 1=SKU, 2=Source, 9=Both
    discount_percentage = Column(Float, default=0.0)  # Discount percentage
    
    # Relationships
    source = relationship("Source", back_populates="brackets")
    
    __table_args__ = (
        # Composite unique constraint
        {"uniqueConstraints": {"source_bracket_unique": {"columns": ['source_id', 'bracket_number']}}},
    )
    
    def __repr__(self):
        return f"<SourceBracket(source_id={self.source_id}, bracket_number={self.bracket_number})>"