# warehouse_replenishment/models/ai_analysis.py
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Boolean, ForeignKey, Text, Index, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base

class AIAnalysis(Base):
    """Model for storing AI analysis results from nightly job processing."""
    __tablename__ = 'ai_analysis'
    
    id = Column(Integer, primary_key=True)
    analysis_date = Column(DateTime, default=func.now(), nullable=False)
    job_date = Column(Date, nullable=False)
    overall_health = Column(String(20), nullable=False)  # HEALTHY, FAIR, NEEDS_ATTENTION, ERROR
    
    # Summary fields
    executive_summary = Column(Text)
    total_items_processed = Column(Integer)
    total_orders_generated = Column(Integer)
    lost_sales_value = Column(Float)
    out_of_stock_count = Column(Integer)
    
    # Analysis results as JSON
    detailed_analysis = Column(JSON)
    insights = Column(JSON)
    recommendations = Column(JSON)
    
    # Processing metadata
    processing_duration = Column(Float)  # Duration in seconds
    error_message = Column(Text)
    
    # Relationships
    insights_records = relationship("AIAnalysisInsight", back_populates="analysis")
    recommendations_records = relationship("AIAnalysisRecommendation", back_populates="analysis")
    
    __table_args__ = (
        Index('idx_ai_analysis_date', 'analysis_date'),
        Index('idx_ai_analysis_job_date', 'job_date'),
        Index('idx_ai_analysis_health', 'overall_health'),
    )


class AIAnalysisInsight(Base):
    """Model for storing individual insights from AI analysis."""
    __tablename__ = 'ai_analysis_insight'
    
    id = Column(Integer, primary_key=True)
    analysis_id = Column(Integer, ForeignKey('ai_analysis.id'), nullable=False)
    
    type = Column(String(20))  # CONCERN, OPPORTUNITY, INFO
    category = Column(String(50))
    message = Column(Text)
    priority = Column(String(10))  # HIGH, MEDIUM, LOW
    
    # Additional metadata
    item_count = Column(Integer)
    financial_impact = Column(Float)
    
    # Relationships
    analysis = relationship("AIAnalysis", back_populates="insights_records")
    
    __table_args__ = (
        Index('idx_insight_analysis_id', 'analysis_id'),
        Index('idx_insight_priority', 'priority'),
        Index('idx_insight_category', 'category'),
    )


class AIAnalysisRecommendation(Base):
    """Model for storing individual recommendations from AI analysis."""
    __tablename__ = 'ai_analysis_recommendation'
    
    id = Column(Integer, primary_key=True)
    analysis_id = Column(Integer, ForeignKey('ai_analysis.id'), nullable=False)
    
    title = Column(String(255))
    priority = Column(String(10))  # HIGH, MEDIUM, LOW
    category = Column(String(50))
    description = Column(Text)
    action_items = Column(JSON)
    
    # Implementation tracking
    status = Column(String(20), default='PENDING')  # PENDING, IN_PROGRESS, COMPLETED, CANCELLED
    assigned_to = Column(String(100))
    due_date = Column(Date)
    completion_date = Column(DateTime)
    
    # Financial impact estimate
    estimated_savings = Column(Float)
    estimated_cost_to_implement = Column(Float)
    
    # Relationships
    analysis = relationship("AIAnalysis", back_populates="recommendations_records")
    
    __table_args__ = (
        Index('idx_recommendation_analysis_id', 'analysis_id'),
        Index('idx_recommendation_priority', 'priority'),
        Index('idx_recommendation_status', 'status'),
    )


class AIAnalysisMetric(Base):
    """Model for storing performance metrics over time."""
    __tablename__ = 'ai_analysis_metric'
    
    id = Column(Integer, primary_key=True)
    analysis_id = Column(Integer, ForeignKey('ai_analysis.id'), nullable=False)
    
    metric_name = Column(String(100), nullable=False)
    metric_value = Column(Float)
    metric_category = Column(String(50))
    
    # Trend information
    previous_value = Column(Float)
    percent_change = Column(Float)
    trend = Column(String(20))  # IMPROVING, DECLINING, STABLE
    
    __table_args__ = (
        Index('idx_metric_analysis_id', 'analysis_id'),
        Index('idx_metric_name', 'metric_name'),
        Index('idx_metric_category', 'metric_category'),
    )