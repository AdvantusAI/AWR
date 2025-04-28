"""
Order models for the ASR system.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, ForeignKey, DateTime, Enum
from sqlalchemy.orm import relationship
import enum

from .base import BaseModel

class OrderStatus(enum.Enum):
    PLANNED = "planned"
    DUE = "due"
    ACCEPTED = "accepted"
    PURGED = "purged"
    DEACTIVATED = "deactivated"


class OrderCategory(enum.Enum):
    DUE = "due"
    PLANNED = "planned"
    A_ORDER_POINT = "a_order_point"
    ORDER_POINT = "order_point"
    ALL = "all"


class Order(BaseModel):
    """
    Order model.
    Represents a purchase order for a specific source and store.
    """
    __tablename__ = 'orders'
    
    source_id = Column(Integer, ForeignKey('sources.id'), nullable=False)
    store_id = Column(String, nullable=False)
    
    # Order status
    status = Column(Enum(OrderStatus), default=OrderStatus.PLANNED)
    category = Column(Enum(OrderCategory), default=OrderCategory.ALL)
    
    # Order dates
    order_date = Column(DateTime)
    expected_delivery_date = Column(DateTime)
    deactivate_until = Column(DateTime)
    
    # Order totals - Independent (service requirements only)
    independent_amount = Column(Float, default=0.0)
    independent_eaches = Column(Float, default=0.0)
    independent_weight = Column(Float, default=0.0)
    independent_volume = Column(Float, default=0.0)
    
    # Order totals - Auto adjusted (system adjustments to meet minimums, etc.)
    auto_adjust_amount = Column(Float, default=0.0)
    auto_adjust_eaches = Column(Float, default=0.0)
    auto_adjust_weight = Column(Float, default=0.0)
    auto_adjust_volume = Column(Float, default=0.0)
    
    # Order totals - Final adjusted (after buyer adjustments)
    final_adjust_amount = Column(Float, default=0.0)
    final_adjust_eaches = Column(Float, default=0.0)
    final_adjust_weight = Column(Float, default=0.0)
    final_adjust_volume = Column(Float, default=0.0)
    
    # Bracket information
    used_bracket = Column(Integer)
    extra_days = Column(Float, default=0.0)
    
    # Order delay information
    order_delay = Column(Integer)  # Approximate days until order is due
    
    # Relationships
    source = relationship("Source", back_populates="orders")
    order_lines = relationship("OrderLine", back_populates="order", cascade="all, delete-orphan")
    order_checks = relationship("OrderCheck", back_populates="order", cascade="all, delete-orphan")
    order_notes = relationship("OrderNote", back_populates="order", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Order(id={self.id}, source_id={self.source_id}, store_id='{self.store_id}', status={self.status})>"


class OrderLine(BaseModel):
    """
    Order line model.
    Represents a single line item on an order.
    """
    __tablename__ = 'order_lines'
    
    order_id = Column(Integer, ForeignKey('orders.id'), nullable=False)
    sku_id = Column(Integer, ForeignKey('skus.id'), nullable=False)
    
    # Quantities
    suggested_order_quantity = Column(Float, default=0.0)  # SOQ in units
    soq_days = Column(Float, default=0.0)  # SOQ in days of supply
    
    # Frozen flag
    is_frozen = Column(Boolean, default=False)  # Is SOQ frozen?
    
    # Order line details
    purchase_price = Column(Float)
    extended_amount = Column(Float)
    
    # Delay information
    item_delay = Column(Integer)  # Days to order point
    
    # Relationships
    order = relationship("Order", back_populates="order_lines")
    sku = relationship("SKU")
    
    def __repr__(self):
        return f"<OrderLine(id={self.id}, order_id={self.order_id}, sku_id={self.sku_id}, soq={self.suggested_order_quantity})>"


class OrderCheck(BaseModel):
    """
    Order check model.
    Represents a check category for items in an order.
    """
    __tablename__ = 'order_checks'
    
    order_id = Column(Integer, ForeignKey('orders.id'), nullable=False)
    check_type = Column(String, nullable=False)  # AOP, OP, Watch, Manual, etc.
    count = Column(Integer, default=0)
    
    # Relationships
    order = relationship("Order", back_populates="order_checks")
    
    def __repr__(self):
        return f"<OrderCheck(order_id={self.order_id}, check_type='{self.check_type}', count={self.count})>"


class OrderNote(BaseModel):
    """
    Order note model.
    Represents notes attached to an order.
    """
    __tablename__ = 'order_notes'
    
    order_id = Column(Integer, ForeignKey('orders.id'), nullable=False)
    note_text = Column(String, nullable=False)
    priority = Column(Integer, default=1)  # 1-9, with 1 being highest priority
    print_location = Column(String)  # Where to print (PO, etc.)
    expiration_date = Column(DateTime)
    
    # Relationships
    order = relationship("Order", back_populates="order_notes")
    
    def __repr__(self):
        return f"<OrderNote(order_id={self.order_id}, note_text='{self.note_text[:20]}...')>"