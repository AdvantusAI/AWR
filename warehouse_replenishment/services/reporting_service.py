# warehouse_replenishment/services/reporting_service.py
from datetime import date, datetime, timedelta
from typing import List, Dict, Tuple, Optional, Union, Any
import logging
import csv
import io
import json
import statistics
import sys
import os
from pathlib import Path


from sqlalchemy import and_, func, or_, text, case, desc, asc
from sqlalchemy.orm import Session

from warehouse_replenishment.models import (
    Item, Order, OrderItem, Vendor, VendorBracket, Company, Warehouse,
    DemandHistory, HistoryException, BuyerClassCode, SystemClassCode
)
from warehouse_replenishment.exceptions import ReportingError
from warehouse_replenishment.utils.date_utils import (
    get_current_period, get_previous_period, get_period_dates
)
from warehouse_replenishment.logging_setup import logger
logger = logging.getLogger(__name__)

class ReportingService:
    """Service for generating reports and analytics."""
    
    def __init__(self, session: Session):
        """Initialize the reporting service.
        
        Args:
            session: Database session
        """
        self.session = session
        self._company_settings = None
    
    @property
    def company_settings(self) -> Dict:
        """Get company settings.
        
        Returns:
            Dictionary with company settings
        """
        if not self._company_settings:
            company = self.session.query(Company).first()
            if not company:
                raise ReportingError("Company settings not found")
            
            self._company_settings = {
                'history_periodicity_default': company.history_periodicity_default,
                'forecasting_periodicity_default': company.forecasting_periodicity_default,
                'service_level_goal': company.service_level_goal
            }
        
        return self._company_settings
    
    def inventory_status_report(
        self, 
        warehouse_id: Optional[int] = None,
        vendor_id: Optional[int] = None,
        buyer_class: Optional[List[str]] = None,
        system_class: Optional[List[str]] = None,
        sort_by: str = "item_id",
        sort_dir: str = "asc",
        include_zeros: bool = True
    ) -> Dict:
        """Generate inventory status report.
        
        Args:
            warehouse_id: Optional warehouse ID filter
            vendor_id: Optional vendor ID filter
            buyer_class: Optional list of buyer classes
            system_class: Optional list of system classes
            sort_by: Sort field
            sort_dir: Sort direction ('asc' or 'desc')
            include_zeros: Whether to include items with zero on-hand quantity
            
        Returns:
            Dictionary with report data
        """
        # Build query
        query = self.session.query(
            Item,
            Vendor.name.label("vendor_name"),
            Warehouse.name.label("warehouse_name")
        ).join(
            Vendor, Item.vendor_id == Vendor.id
        ).join(
            Warehouse, Item.warehouse_id == Warehouse.id
        )
        
        # Apply filters
        if warehouse_id:
            query = query.filter(Item.warehouse_id == warehouse_id)
            
        if vendor_id:
            query = query.filter(Item.vendor_id == vendor_id)
            
        if buyer_class:
            query = query.filter(Item.buyer_class.in_(buyer_class))
        else:
            # Default to active items
            query = query.filter(Item.buyer_class.in_(['R', 'W']))
            
        if system_class:
            query = query.filter(Item.system_class.in_(system_class))
            
        if not include_zeros:
            query = query.filter(Item.on_hand > 0)
        
        # Apply sorting
        if sort_by == "item_id":
            query = query.order_by(text(f"item.item_id {sort_dir}"))
        elif sort_by == "description":
            query = query.order_by(text(f"item.description {sort_dir}"))
        elif sort_by == "on_hand":
            query = query.order_by(text(f"item.on_hand {sort_dir}"))
        elif sort_by == "on_order":
            query = query.order_by(text(f"item.on_order {sort_dir}"))
        elif sort_by == "demand":
            query = query.order_by(text(f"item.demand_4weekly {sort_dir}"))
        elif sort_by == "vendor_name":
            query = query.order_by(text(f"vendor_name {sort_dir}"))
        else:
            # Default sort
            query = query.order_by(Item.item_id)
        
        # Execute query
        results = query.all()
        
        # Create report data
        report_data = []
        summary = {
            'total_items': len(results),
            'total_value': 0.0,
            'total_on_hand': 0.0,
            'total_on_order': 0.0,
            'out_of_stock_items': 0,
            'low_stock_items': 0
        }
        
        for row in results:
            item = row.Item
            vendor_name = row.vendor_name
            warehouse_name = row.warehouse_name
            
            # Calculate inventory value
            on_hand_value = item.on_hand * item.purchase_price
            on_order_value = item.on_order * item.purchase_price
            total_value = on_hand_value + on_order_value
            
            # Calculate days supply
            daily_demand = item.demand_4weekly / 28 if item.demand_4weekly > 0 else 0
            days_supply = item.on_hand / daily_demand if daily_demand > 0 else 0
            
            # Determine stock status
            stock_status = "OK"
            if item.on_hand <= 0:
                stock_status = "OUT_OF_STOCK"
                summary['out_of_stock_items'] += 1
            elif days_supply < item.lead_time_forecast:
                stock_status = "LOW_STOCK"
                summary['low_stock_items'] += 1
            
            # Create item data
            item_data = {
                'item_id': item.item_id,
                'description': item.description,
                'vendor_id': item.vendor_id,
                'vendor_name': vendor_name,
                'warehouse_id': item.warehouse_id,
                'warehouse_name': warehouse_name,
                'buyer_class': item.buyer_class.value if item.buyer_class else None,
                'system_class': item.system_class.value if item.system_class else None,
                'on_hand': item.on_hand,
                'on_order': item.on_order,
                'on_hand_value': on_hand_value,
                'on_order_value': on_order_value,
                'total_value': total_value,
                'demand_4weekly': item.demand_4weekly,
                'days_supply': round(days_supply, 1),
                'stock_status': stock_status,
                'purchase_price': item.purchase_price,
                'sale_price': item.sales_price,
                'lead_time': item.lead_time_forecast,
                'madp': item.madp,
                'track': item.track
            }
            
            report_data.append(item_data)
            
            # Update summary
            summary['total_value'] += total_value
            summary['total_on_hand'] += item.on_hand
            summary['total_on_order'] += item.on_order
        
        # Create report
        report = {
            'report_name': "Inventory Status Report",
            'report_date': datetime.now().isoformat(),
            'filters': {
                'warehouse_id': warehouse_id,
                'vendor_id': vendor_id,
                'buyer_class': buyer_class,
                'system_class': system_class,
                'include_zeros': include_zeros
            },
            'summary': summary,
            'data': report_data
        }
        
        return report
    
    def vendor_performance_report(
        self,
        warehouse_id: Optional[int] = None,
        vendor_id: Optional[int] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> Dict:
        """Generate vendor performance report.
        
        Args:
            warehouse_id: Optional warehouse ID filter
            vendor_id: Optional vendor ID filter
            from_date: Optional start date filter
            to_date: Optional end date filter
            
        Returns:
            Dictionary with report data
        """
        # Set default dates if not provided
        if to_date is None:
            to_date = date.today()
            
        if from_date is None:
            from_date = to_date - timedelta(days=90)  # Last 90 days
        
        # Get vendor data
        vendor_query = self.session.query(Vendor)
        
        if warehouse_id:
            vendor_query = vendor_query.filter(Vendor.warehouse_id == warehouse_id)
            
        if vendor_id:
            vendor_query = vendor_query.filter(Vendor.id == vendor_id)
        
        # Only include active vendors
        vendor_query = vendor_query.filter(Vendor.active_items_count > 0)
        
        vendors = vendor_query.all()
        
        # Get order data for each vendor
        report_data = []
        
        for vendor in vendors:
            # Get accepted orders in date range
            order_query = self.session.query(Order).filter(
                Order.vendor_id == vendor.id,
                Order.status.in_(['ACCEPTED', 'RECEIVED']),
                Order.approval_date >= from_date,
                Order.approval_date <= to_date
            )
            
            orders = order_query.all()
            
            # Calculate metrics
            total_orders = len(orders)
            total_amount = sum(order.final_adj_amount for order in orders)
            total_lines = 0
            on_time_orders = 0
            on_time_lines = 0
            service_level = 0.0
            
            # Get order details
            for order in orders:
                # Get order items
                order_items = self.session.query(OrderItem).filter(
                    OrderItem.order_id == order.id
                ).all()
                
                total_lines += len(order_items)
                
                # Check if order was on time
                # In a real implementation, we would compare receipt date to expected delivery date
                # For this example, we'll assume 50% of orders are on time
                if order.id % 2 == 0:  # Simulate 50% on-time
                    on_time_orders += 1
                    on_time_lines += len(order_items)
            
            # Calculate service level
            if total_orders > 0:
                service_level = (on_time_orders / total_orders) * 100
            
            # Get fill rate
            # In a real implementation, we would check order fill completeness
            # For this example, we'll use a placeholder
            fill_rate = 95.0  # Placeholder
            
            # Get lead time performance
            lead_time_adherence = (on_time_orders / total_orders * 100) if total_orders > 0 else 0
            
            # Create vendor data
            vendor_data = {
                'vendor_id': vendor.vendor_id,
                'name': vendor.name,
                'warehouse_id': vendor.warehouse_id,
                'active_items': vendor.active_items_count,
                'total_orders': total_orders,
                'total_amount': total_amount,
                'total_lines': total_lines,
                'on_time_orders': on_time_orders,
                'on_time_lines': on_time_lines,
                'service_level': round(service_level, 2),
                'fill_rate': round(fill_rate, 2),
                'lead_time_adherence': round(lead_time_adherence, 2),
                'quoted_lead_time': vendor.lead_time_quoted,
                'actual_lead_time': vendor.lead_time_forecast,
                'lead_time_variance': vendor.lead_time_variance
            }
            
            report_data.append(vendor_data)
        
        # Sort by service level (descending)
        report_data.sort(key=lambda x: x['service_level'], reverse=True)
        
        # Calculate summary metrics
        summary = {
            'total_vendors': len(report_data),
            'average_service_level': statistics.mean([v['service_level'] for v in report_data]) if report_data else 0,
            'average_fill_rate': statistics.mean([v['fill_rate'] for v in report_data]) if report_data else 0,
            'average_lead_time_adherence': statistics.mean([v['lead_time_adherence'] for v in report_data]) if report_data else 0,
            'total_orders': sum(v['total_orders'] for v in report_data),
            'total_amount': sum(v['total_amount'] for v in report_data)
        }
        
        # Create report
        report = {
            'report_name': "Vendor Performance Report",
            'report_date': datetime.now().isoformat(),
            'period': {
                'from_date': from_date.isoformat(),
                'to_date': to_date.isoformat()
            },
            'filters': {
                'warehouse_id': warehouse_id,
                'vendor_id': vendor_id
            },
            'summary': summary,
            'data': report_data
        }
        
        return report
    
    def forecast_accuracy_report(
        self,
        warehouse_id: Optional[int] = None,
        vendor_id: Optional[int] = None,
        item_id: Optional[int] = None,
        periods: int = 6  # Number of periods to analyze
    ) -> Dict:
        """Generate forecast accuracy report.
        
        Args:
            warehouse_id: Optional warehouse ID filter
            vendor_id: Optional vendor ID filter
            item_id: Optional item ID filter
            periods: Number of periods to analyze
            
        Returns:
            Dictionary with report data
        """
        # Build query for items
        item_query = self.session.query(Item)
        
        if warehouse_id:
            item_query = item_query.filter(Item.warehouse_id == warehouse_id)
            
        if vendor_id:
            item_query = item_query.filter(Item.vendor_id == vendor_id)
            
        if item_id:
            item_query = item_query.filter(Item.id == item_id)
        
        # Only include active items
        item_query = item_query.filter(Item.buyer_class.in_(['R', 'W']))
        
        items = item_query.all()
        
        # Get current period
        periodicity = self.company_settings['history_periodicity_default']
        current_period, current_year = get_current_period(periodicity)
        
        # Create report data
        report_data = []
        
        for item in items:
            # Get history for the last n periods
            history_data = []
            
            period = current_period
            year = current_year
            
            for i in range(periods):
                # Get history record
                history = self.session.query(DemandHistory).filter(
                    DemandHistory.item_id == item.id,
                    DemandHistory.period_number == period,
                    DemandHistory.period_year == year
                ).first()
                
                if history:
                    history_data.append({
                        'period_number': period,
                        'period_year': year,
                        'actual_demand': history.total_demand,
                        'forecast': None,  # Placeholder for forecast
                        'error': None,     # Placeholder for error
                        'abs_error': None, # Placeholder for absolute error
                        'error_pct': None  # Placeholder for error percentage
                    })
                
                # Move to previous period
                period, year = get_previous_period(period, year, periodicity)
            
            # Sort by period/year (oldest first)
            history_data.sort(key=lambda x: (x['period_year'], x['period_number']))
            
            # Calculate forecast accuracy
            total_abs_error = 0
            total_actual = 0
            periods_with_data = 0
            
            for i, period_data in enumerate(history_data):
                if i > 0:  # Skip first period (no forecast available)
                    # Get previous period's actual as "forecast" for current period
                    # In a real implementation, we would use stored forecasts
                    forecast = history_data[i-1]['actual_demand']
                    actual = period_data['actual_demand']
                    
                    if actual is not None and forecast is not None:
                        error = actual - forecast
                        abs_error = abs(error)
                        error_pct = (abs_error / actual) * 100 if actual > 0 else 0
                        
                        period_data['forecast'] = forecast
                        period_data['error'] = error
                        period_data['abs_error'] = abs_error
                        period_data['error_pct'] = error_pct
                        
                        total_abs_error += abs_error
                        total_actual += actual
                        periods_with_data += 1
            
            # Calculate overall accuracy
            mape = 0  # Mean Absolute Percentage Error
            wape = 0  # Weighted Absolute Percentage Error
            
            if periods_with_data > 0:
                mape = sum(p['error_pct'] for p in history_data if p['error_pct'] is not None) / periods_with_data
                
            if total_actual > 0:
                wape = (total_abs_error / total_actual) * 100
            
            # Create item data
            item_data = {
                'item_id': item.item_id,
                'description': item.description,
                'vendor_id': item.vendor_id,
                'warehouse_id': item.warehouse_id,
                'buyer_class': item.buyer_class.value if item.buyer_class else None,
                'system_class': item.system_class.value if item.system_class else None,
                'forecast_method': item.forecast_method.value if item.forecast_method else None,
                'current_madp': item.madp,
                'current_track': item.track,
                'mape': round(mape, 2) if mape is not None else None,
                'wape': round(wape, 2) if wape is not None else None,
                'periods_analyzed': periods_with_data,
                'periods_data': history_data
            }
            
            report_data.append(item_data)
        
        # Sort by WAPE (worst first)
        report_data.sort(key=lambda x: x['wape'] if x['wape'] is not None else 0, reverse=True)
        
        # Calculate summary
        items_with_data = [item for item in report_data if item['periods_analyzed'] > 0]
        
        summary = {
            'total_items': len(report_data),
            'items_with_data': len(items_with_data),
            'average_mape': statistics.mean([i['mape'] for i in items_with_data]) if items_with_data else 0,
            'average_wape': statistics.mean([i['wape'] for i in items_with_data]) if items_with_data else 0,
            'best_items': [
                {'item_id': i['item_id'], 'wape': i['wape']} 
                for i in sorted(items_with_data, key=lambda x: x['wape'])[:5]
            ] if items_with_data else [],
            'worst_items': [
                {'item_id': i['item_id'], 'wape': i['wape']} 
                for i in sorted(items_with_data, key=lambda x: x['wape'], reverse=True)[:5]
            ] if items_with_data else []
        }
        
        # Create report
        report = {
            'report_name': "Forecast Accuracy Report",
            'report_date': datetime.now().isoformat(),
            'filters': {
                'warehouse_id': warehouse_id,
                'vendor_id': vendor_id,
                'item_id': item_id,
                'periods': periods
            },
            'summary': summary,
            'data': report_data
        }
        
        return report
    
    def service_level_report(
        self,
        warehouse_id: Optional[int] = None,
        vendor_id: Optional[int] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> Dict:
        """Generate service level report.
        
        Args:
            warehouse_id: Optional warehouse ID filter
            vendor_id: Optional vendor ID filter
            from_date: Optional start date filter
            to_date: Optional end date filter
            
        Returns:
            Dictionary with report data
        """
        # Set default dates if not provided
        if to_date is None:
            to_date = date.today()
            
        if from_date is None:
            from_date = to_date - timedelta(days=90)  # Last 90 days
        
        # Build query for items
        item_query = self.session.query(Item)
        
        if warehouse_id:
            item_query = item_query.filter(Item.warehouse_id == warehouse_id)
            
        if vendor_id:
            item_query = item_query.filter(Item.vendor_id == vendor_id)
        
        # Only include active items
        item_query = item_query.filter(Item.buyer_class.in_(['R', 'W']))
        
        items = item_query.all()
        
        # Create report data
        report_data = []
        
        for item in items:
            # Get history in date range
            history_query = self.session.query(
                func.sum(DemandHistory.shipped).label("total_shipped"),
                func.sum(DemandHistory.lost_sales).label("total_lost_sales")
            ).join(
                Item, DemandHistory.item_id == Item.id
            ).filter(
                DemandHistory.item_id == item.id
            )
            
            # Apply date range filters
            # In a real implementation, we would convert dates to periods
            # For this example, we'll skip date filtering
            
            history_result = history_query.first()
            
            # Calculate service level
            total_shipped = history_result.total_shipped or 0
            total_lost_sales = history_result.total_lost_sales or 0
            total_demand = total_shipped + total_lost_sales
            
            service_level_attained = 0
            if total_demand > 0:
                service_level_attained = (total_shipped / total_demand) * 100
            
            # Compare with goal
            service_level_goal = item.service_level_goal
            service_level_gap = service_level_goal - service_level_attained
            
            # Create item data
            item_data = {
                'item_id': item.item_id,
                'description': item.description,
                'vendor_id': item.vendor_id,
                'warehouse_id': item.warehouse_id,
                'buyer_class': item.buyer_class.value if item.buyer_class else None,
                'system_class': item.system_class.value if item.system_class else None,
                'service_level_goal': service_level_goal,
                'service_level_attained': round(service_level_attained, 2),
                'service_level_gap': round(service_level_gap, 2),
                'total_shipped': total_shipped,
                'total_lost_sales': total_lost_sales,
                'total_demand': total_demand,
                'sstf': item.sstf,  # Safety Stock Time Factor
                'madp': item.madp,
                'lead_time': item.lead_time_forecast,
                'lead_time_variance': item.lead_time_variance
            }
            
            report_data.append(item_data)
        
        # Sort by service level gap (largest first)
        report_data.sort(key=lambda x: x['service_level_gap'], reverse=True)
        
        # Calculate summary
        items_with_data = [item for item in report_data if item['total_demand'] > 0]
        
        summary = {
            'total_items': len(report_data),
            'items_with_data': len(items_with_data),
            'average_service_level': statistics.mean([i['service_level_attained'] for i in items_with_data]) if items_with_data else 0,
            'service_level_goal': self.company_settings['service_level_goal'],
            'items_below_goal': len([i for i in items_with_data if i['service_level_attained'] < i['service_level_goal']]),
            'items_at_or_above_goal': len([i for i in items_with_data if i['service_level_attained'] >= i['service_level_goal']]),
            'worst_performers': [
                {'item_id': i['item_id'], 'service_level': i['service_level_attained']} 
                for i in sorted(items_with_data, key=lambda x: x['service_level_attained'])[:5]
            ] if items_with_data else []
        }
        
        # Create report
        report = {
            'report_name': "Service Level Report",
            'report_date': datetime.now().isoformat(),
            'period': {
                'from_date': from_date.isoformat(),
                'to_date': to_date.isoformat()
            },
            'filters': {
                'warehouse_id': warehouse_id,
                'vendor_id': vendor_id
            },
            'summary': summary,
            'data': report_data
        }
        
        return report
    
    def exception_summary_report(
        self,
        warehouse_id: Optional[int] = None,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        exception_types: Optional[List[str]] = None
    ) -> Dict:
        """Generate exception summary report.
        
        Args:
            warehouse_id: Optional warehouse ID filter
            from_date: Optional start date filter
            to_date: Optional end date filter
            exception_types: Optional list of exception types to include
            
        Returns:
            Dictionary with report data
        """
        # Set default dates if not provided
        if to_date is None:
            to_date = datetime.now()
            
        if from_date is None:
            from_date = to_date - timedelta(days=30)  # Last 30 days
        
        # Build query for exceptions
        exception_query = self.session.query(
            HistoryException.exception_type,
            func.count(HistoryException.id).label("count"),
            func.count(case([(HistoryException.is_resolved == True, 1)])).label("resolved_count")
        )
        
        # Apply filters
        exception_query = exception_query.filter(
            HistoryException.creation_date >= from_date,
            HistoryException.creation_date <= to_date
        )
        
        if warehouse_id:
            exception_query = exception_query.join(
                Item, HistoryException.item_id == Item.id
            ).filter(
                Item.warehouse_id == warehouse_id
            )
            
        if exception_types:
            exception_query = exception_query.filter(
                HistoryException.exception_type.in_(exception_types)
            )
        
        # Group by exception type
        exception_query = exception_query.group_by(HistoryException.exception_type)
        
        # Execute query
        exception_results = exception_query.all()
        
        # Calculate summary
        summary_data = []
        
        for result in exception_results:
            exception_type = result.exception_type
            count = result.count
            resolved_count = result.resolved_count
            resolution_rate = (resolved_count / count) * 100 if count > 0 else 0
            
            summary_data.append({
                'exception_type': exception_type,
                'count': count,
                'resolved_count': resolved_count,
                'unresolved_count': count - resolved_count,
                'resolution_rate': round(resolution_rate, 2)
            })
        
        # Sort by count (highest first)
        summary_data.sort(key=lambda x: x['count'], reverse=True)
        
        # Calculate totals
        total_count = sum(item['count'] for item in summary_data)
        total_resolved = sum(item['resolved_count'] for item in summary_data)
        total_resolution_rate = (total_resolved / total_count) * 100 if total_count > 0 else 0
        
        # Get recent exceptions
        recent_exceptions_query = self.session.query(
            HistoryException,
            Item.item_id.label("item_code")
        ).join(
            Item, HistoryException.item_id == Item.id
        ).filter(
            HistoryException.creation_date >= from_date,
            HistoryException.creation_date <= to_date,
            HistoryException.is_resolved == False
        )
        
        if warehouse_id:
            recent_exceptions_query = recent_exceptions_query.filter(
                Item.warehouse_id == warehouse_id
            )
            
        if exception_types:
            recent_exceptions_query = recent_exceptions_query.filter(
                HistoryException.exception_type.in_(exception_types)
            )
        
        # Order by creation date (most recent first)
        recent_exceptions_query = recent_exceptions_query.order_by(
            HistoryException.creation_date.desc()
        ).limit(10)
        
        recent_exceptions = recent_exceptions_query.all()
        
        recent_exceptions_data = []
        for row in recent_exceptions:
            exception = row.HistoryException
            item_code = row.item_code
            
            recent_exceptions_data.append({
                'id': exception.id,
                'exception_type': exception.exception_type,
                'item_id': exception.item_id,
                'item_code': item_code,
                'creation_date': exception.creation_date.isoformat(),
                'forecast_value': exception.forecast_value,
                'actual_value': exception.actual_value,
                'madp': exception.madp,
                'track': exception.track,
                'notes': exception.notes
            })
        
        # Create report
        report = {
            'report_name': "Exception Summary Report",
            'report_date': datetime.now().isoformat(),
            'period': {
                'from_date': from_date.isoformat(),
                'to_date': to_date.isoformat()
            },
            'filters': {
                'warehouse_id': warehouse_id,
                'exception_types': exception_types
            },
            'summary': {
                'total_exceptions': total_count,
                'total_resolved': total_resolved,
                'total_unresolved': total_count - total_resolved,
                'overall_resolution_rate': round(total_resolution_rate, 2)
            },
            'exception_summary': summary_data,
            'recent_unresolved_exceptions': recent_exceptions_data
        }
        
        return report
    
    def order_analysis_report(
        self,
        warehouse_id: Optional[int] = None,
        vendor_id: Optional[int] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> Dict:
        """Generate order analysis report.
        
        Args:
            warehouse_id: Optional warehouse ID filter
            vendor_id: Optional vendor ID filter
            from_date: Optional start date filter
            to_date: Optional end date filter
            
        Returns:
            Dictionary with report data
        """
        # Set default dates if not provided
        if to_date is None:
            to_date = date.today()
            
        if from_date is None:
            from_date = to_date - timedelta(days=90)  # Last 90 days
        
        # Build query for orders
        order_query = self.session.query(Order)
        
        # Apply filters
        if warehouse_id:
            order_query = order_query.filter(Order.warehouse_id == warehouse_id)
            
        if vendor_id:
            order_query = order_query.filter(Order.vendor_id == vendor_id)
            
        order_query = order_query.filter(
            Order.order_date >= from_date,
            Order.order_date <= to_date
        )
        
        orders = order_query.all()
        
        # Calculate summary metrics
        total_orders = len(orders)
        total_amount = sum(order.final_adj_amount for order in orders)
        
        # Count by status
        status_counts = {}
        for order in orders:
            status = order.status
            if status not in status_counts:
                status_counts[status] = 0
            status_counts[status] += 1
        
        # Get bracket distribution
        bracket_distribution = {}
        for order in orders:
            bracket = f"Bracket {order.current_bracket}"
            if bracket not in bracket_distribution:
                bracket_distribution[bracket] = {
                    'count': 0,
                    'amount': 0.0
                }
            bracket_distribution[bracket]['count'] += 1
            bracket_distribution[bracket]['amount'] += order.final_adj_amount
        
        # Calculate average order size
        avg_order_size = total_amount / total_orders if total_orders > 0 else 0
        
        # Get order items data
        order_items_data = {}
        for order in orders:
            # Get order items
            order_items = self.session.query(OrderItem).filter(
                OrderItem.order_id == order.id
            ).all()
            
            for order_item in order_items:
                item_id = order_item.item_id
                if item_id not in order_items_data:
                    order_items_data[item_id] = {
                        'count': 0,
                        'total_soq': 0.0
                    }
                
                order_items_data[item_id]['count'] += 1
                order_items_data[item_id]['total_soq'] += order_item.soq_units
        
        # Get top ordered items
        top_items = []
        for item_id, data in order_items_data.items():
            item = self.session.query(Item).get(item_id)
            if item:
                top_items.append({
                    'item_id': item.item_id,
                    'description': item.description,
                    'order_count': data['count'],
                    'total_quantity': data['total_soq'],
                    'average_quantity': data['total_soq'] / data['count'] if data['count'] > 0 else 0
                })
        
        # Sort by order count (highest first)
        top_items.sort(key=lambda x: x['order_count'], reverse=True)
        top_items = top_items[:10]  # Keep only top 10
        
        # Create report
        report = {
            'report_name': "Order Analysis Report",
            'report_date': datetime.now().isoformat(),
            'period': {
                'from_date': from_date.isoformat(),
                'to_date': to_date.isoformat()
            },
            'filters': {
                'warehouse_id': warehouse_id,
                'vendor_id': vendor_id
            },
            'summary': {
                'total_orders': total_orders,
                'total_amount': total_amount,
                'average_order_size': round(avg_order_size, 2),
                'status_distribution': status_counts,
                'bracket_distribution': bracket_distribution
            },
            'top_ordered_items': top_items
        }
        
        return report
    
    def export_report_to_csv(self, report: Dict) -> str:
        """Export a report to CSV.
        
        Args:
            report: Report dictionary
            
        Returns:
            CSV data as string
        """
        if 'data' not in report:
            raise ReportingError("Report has no data to export")
            
        data = report['data']
        if not data:
            return "No data to export"
            
        # Create CSV output
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        header = list(data[0].keys())
        writer.writerow(header)
        
        # Write data rows
        for row in data:
            writer.writerow([row.get(col, '') for col in header])
            
        return output.getvalue()
    
    def export_report_to_json(self, report: Dict) -> str:
        """Export a report to JSON.
        
        Args:
            report: Report dictionary
            
        Returns:
            JSON data as string
        """
        return json.dumps(report, default=str, indent=2)