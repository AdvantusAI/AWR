# warehouse_replenishment/services/ai_agent_service.py
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Union, Any
import sys
import os
from pathlib import Path
import json
import statistics

# Add the parent directory to the path
parent_dir = str(Path(__file__).parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from sqlalchemy import and_, func, desc, or_
from sqlalchemy.orm import Session

from warehouse_replenishment.models import (
    Item, Order, OrderItem, Vendor, Warehouse, DemandHistory,
    HistoryException, ManagementException, BuyerClassCode, SystemClassCode,
    AIAnalysis, AIAnalysisInsight, AIAnalysisRecommendation, AIAnalysisMetric
)
from warehouse_replenishment.exceptions import AIAgentError
from warehouse_replenishment.logging_setup import logger

logger = logging.getLogger(__name__)


class NightlyJobAnalyzer:
    """AI Agent for analyzing nightly job executions and providing recommendations."""
    
    def __init__(self, session: Session):
        """Initialize the AI agent with database session.
        
        Args:
            session: Database session
        """
        self.session = session
        self.analysis_date = datetime.now()
        self.recommendations = []
        self.insights = []
        
    def _serialize_for_json(self, obj: Any) -> Any:
        """Serialize objects for JSON storage.
        
        Args:
            obj: Object to serialize
            
        Returns:
            JSON-serializable object
        """
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return obj.total_seconds()
        elif isinstance(obj, dict):
            return {k: self._serialize_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._serialize_for_json(item) for item in obj]
        return obj

    def analyze_nightly_job_results(self, job_results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze the results of a nightly job execution.
        
        Args:
            job_results: Dictionary containing nightly job execution results
            
        Returns:
            Dictionary with analysis results and recommendations
        """
        analysis = {
            'analysis_date': self.analysis_date.isoformat(),
            'job_date': job_results.get('start_time', datetime.now()).date().isoformat(),
            'overall_health': 'HEALTHY',
            'insights': [],
            'recommendations': [],
            'detailed_analysis': {}
        }
        
        try:
            # 1. Analyze stock status updates
            self._analyze_stock_status(job_results, analysis)
            
            # 2. Analyze lost sales calculations
            self._analyze_lost_sales(job_results, analysis)
            
            # 3. Analyze safety stock updates
            self._analyze_safety_stock(job_results, analysis)
            
            # 4. Analyze time-based parameters
            self._analyze_time_based_parameters(job_results, analysis)
            
            # 5. Analyze order generation
            self._analyze_order_generation(job_results, analysis)
            
            # 6. Generate overall health status
            self._determine_overall_health(analysis)
            
            # 7. Generate specific recommendations
            self._generate_recommendations(analysis)
            
            # 8. Generate executive summary
            analysis['executive_summary'] = self._create_executive_summary(analysis)
            
            # 9. Save analysis to database
            analysis['db_id'] = self._save_analysis_to_database(analysis, job_results)
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error in nightly job analysis: {str(e)}")
            analysis['overall_health'] = 'ERROR'
            analysis['error'] = str(e)
            
            # Try to save error analysis to database
            try:
                analysis['db_id'] = self._save_analysis_to_database(analysis, job_results)
            except Exception as db_error:
                logger.error(f"Failed to save error analysis to database: {str(db_error)}")
            
            return analysis
    
    def _save_analysis_to_database(self, analysis: Dict[str, Any], job_results: Dict[str, Any]) -> int:
        """Save the analysis results to the database.
        
        Args:
            analysis: Analysis results dictionary
            job_results: Original job results
            
        Returns:
            ID of the saved analysis record
        """
        try:
            # Extract summary data
            lost_sales_details = analysis['detailed_analysis'].get('lost_sales', {})
            stock_status_details = analysis['detailed_analysis'].get('stock_status', {})
            order_generation_details = analysis['detailed_analysis'].get('order_generation', {})
            
            # Serialize job results for JSON storage
            serialized_job_results = self._serialize_for_json(job_results)
            
            # Create main analysis record
            ai_analysis = AIAnalysis(
                analysis_date=self.analysis_date,
                job_date=datetime.strptime(analysis['job_date'], '%Y-%m-%d').date(),
                overall_health=analysis['overall_health'],
                executive_summary=analysis.get('executive_summary', ''),
                total_items_processed=stock_status_details.get('total_items', 0),
                total_orders_generated=order_generation_details.get('generated_orders', 0),
                lost_sales_value=lost_sales_details.get('lost_sales_value', 0.0),
                out_of_stock_count=self._get_out_of_stock_count(),
                detailed_analysis=serialized_job_results,
                insights=self._serialize_for_json(analysis['insights']),
                recommendations=[rec['title'] for rec in analysis['recommendations']],
                processing_duration=job_results.get('duration', timedelta()).total_seconds() if job_results.get('duration') else None,
                error_message=analysis.get('error')
            )
            self.session.add(ai_analysis)
            self.session.flush()  # Get the ID but don't commit yet
            
            # Create insight records
            for insight in analysis.get('insights', []):
                insight_record = AIAnalysisInsight(
                    analysis_id=ai_analysis.id,
                    type=insight.get('type', 'INFO'),
                    category=insight.get('category', 'GENERAL'),
                    message=insight.get('message', ''),
                    priority=insight.get('priority', 'LOW'),
                    item_count=insight.get('item_count', 0),
                    financial_impact=insight.get('financial_impact', 0.0)
                )
                self.session.add(insight_record)
            
            # Create recommendation records
            for recommendation in analysis.get('recommendations', []):
                rec_record = AIAnalysisRecommendation(
                    analysis_id=ai_analysis.id,
                    title=recommendation.get('title', ''),
                    priority=recommendation.get('priority', 'LOW'),
                    category=recommendation.get('category', 'GENERAL'),
                    description=recommendation.get('description', ''),
                    action_items=recommendation.get('action_items', []),
                    status='PENDING',
                    estimated_savings=recommendation.get('estimated_savings', 0.0),
                    estimated_cost_to_implement=recommendation.get('estimated_cost_to_implement', 0.0)
                )
                self.session.add(rec_record)
            
            # Create metrics records
            metrics_data = {
                'lost_sales_value': lost_sales_details.get('lost_sales_value', 0.0),
                'out_of_stock_count': self._get_out_of_stock_count(),
                'orders_generated': order_generation_details.get('generated_orders', 0),
                'safety_stock_errors': analysis['detailed_analysis'].get('safety_stock', {}).get('errors', 0),
                'total_errors': sum(
                    details.get('errors', 0) 
                    for details in analysis['detailed_analysis'].values()
                    if isinstance(details, dict)
                )
            }
            
            for metric_name, metric_value in metrics_data.items():
                metric_record = AIAnalysisMetric(
                    analysis_id=ai_analysis.id,
                    metric_name=metric_name,
                    metric_value=metric_value,
                    metric_category='OPERATIONAL'
                )
                self.session.add(metric_record)
            
            # Commit all records
            self.session.commit()
            
            logger.info(f"Saved AI analysis to database with ID: {ai_analysis.id}")
            return ai_analysis.id
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error saving AI analysis to database: {str(e)}")
            raise

    # ... [rest of the existing methods from analyze_stock_status through generate_html_report remain unchanged] ...
    
    def _analyze_stock_status(self, job_results: Dict[str, Any], analysis: Dict[str, Any]):
        """Analyze stock status update results."""
        stock_status_results = job_results.get('processes', {}).get('update_stock_status', {})
        
        detailed_analysis = {
            'total_items': stock_status_results.get('total_items', 0),
            'updated_items': stock_status_results.get('updated_items', 0),
            'errors': stock_status_results.get('errors', 0),
            'success_rate': 0.0,
            'key_findings': []
        }
        
        if detailed_analysis['total_items'] > 0:
            detailed_analysis['success_rate'] = (
                detailed_analysis['updated_items'] / detailed_analysis['total_items'] * 100
            )
        
        # Generate insights
        if detailed_analysis['errors'] > 0:
            insight = {
                'type': 'CONCERN',
                'category': 'STOCK_STATUS',
                'message': f"Stock status update had {detailed_analysis['errors']} errors",
                'priority': 'HIGH' if detailed_analysis['success_rate'] < 90 else 'MEDIUM'
            }
            analysis['insights'].append(insight)
        
        # Generate specific findings
        if detailed_analysis['success_rate'] < 100:
            detailed_analysis['key_findings'].append(
                f"Only {detailed_analysis['success_rate']:.1f}% of items were successfully updated"
            )
        
        # Analyze out of stock items
        out_of_stock_count = self._get_out_of_stock_count()
        if out_of_stock_count > 0:
            detailed_analysis['key_findings'].append(
                f"{out_of_stock_count} items are currently out of stock"
            )
            
            analysis['insights'].append({
                'type': 'CONCERN',
                'category': 'STOCK_STATUS',
                'message': f"{out_of_stock_count} items are out of stock",
                'priority': 'HIGH' if out_of_stock_count > 50 else 'MEDIUM'
            })
        
        analysis['detailed_analysis']['stock_status'] = detailed_analysis
    
    def _analyze_lost_sales(self, job_results: Dict[str, Any], analysis: Dict[str, Any]):
        """Analyze lost sales calculation results."""
        lost_sales_results = job_results.get('processes', {}).get('calculate_lost_sales', {})
        
        detailed_analysis = {
            'total_items': lost_sales_results.get('total_items', 0),
            'updated_items': lost_sales_results.get('updated_items', 0),
            'calculated_lost_sales': lost_sales_results.get('calculated_lost_sales', 0),
            'errors': lost_sales_results.get('errors', 0),
            'key_findings': []
        }
        
        # Calculate lost sales value if possible
        if detailed_analysis['calculated_lost_sales'] > 0:
            lost_sales_value = self._estimate_lost_sales_value(detailed_analysis['calculated_lost_sales'])
            detailed_analysis['lost_sales_value'] = lost_sales_value
            
            detailed_analysis['key_findings'].append(
                f"Calculated {detailed_analysis['calculated_lost_sales']:.1f} units of lost sales"
            )
            detailed_analysis['key_findings'].append(
                f"Estimated lost sales value: ${lost_sales_value:.2f}"
            )
            
            # Generate insight for significant lost sales
            if lost_sales_value > 1000:  # Threshold for concern
                analysis['insights'].append({
                    'type': 'CONCERN',
                    'category': 'LOST_SALES',
                    'message': f"Significant lost sales: ${lost_sales_value:.2f}",
                    'priority': 'HIGH' if lost_sales_value > 5000 else 'MEDIUM'
                })
        
        # Check for items with frequent stockouts
        frequent_stockout_items = self._identify_frequent_stockout_items()
        if frequent_stockout_items:
            detailed_analysis['key_findings'].append(
                f"{len(frequent_stockout_items)} items have frequent stockouts"
            )
            detailed_analysis['frequent_stockout_items'] = frequent_stockout_items
            
            analysis['insights'].append({
                'type': 'CONCERN',
                'category': 'LOST_SALES',
                'message': f"{len(frequent_stockout_items)} items have frequent stockouts",
                'priority': 'HIGH'
            })
        
        analysis['detailed_analysis']['lost_sales'] = detailed_analysis
    
    def _analyze_safety_stock(self, job_results: Dict[str, Any], analysis: Dict[str, Any]):
        """Analyze safety stock update results."""
        safety_stock_results = job_results.get('processes', {}).get('update_safety_stock', {})
        
        detailed_analysis = {
            'total_items': safety_stock_results.get('total_items', 0),
            'updated_items': safety_stock_results.get('updated_items', 0),
            'errors': safety_stock_results.get('errors', 0),
            'key_findings': []
        }
        
        # Analyze items with insufficient safety stock
        low_ss_items = self._identify_low_safety_stock_items()
        if low_ss_items:
            detailed_analysis['key_findings'].append(
                f"{len(low_ss_items)} items have insufficient safety stock"
            )
            detailed_analysis['low_safety_stock_items'] = low_ss_items
            
            analysis['insights'].append({
                'type': 'CONCERN',
                'category': 'SAFETY_STOCK',
                'message': f"{len(low_ss_items)} items have insufficient safety stock",
                'priority': 'HIGH'
            })
        
        # Analyze items with excessive safety stock
        high_ss_items = self._identify_excessive_safety_stock_items()
        if high_ss_items:
            detailed_analysis['key_findings'].append(
                f"{len(high_ss_items)} items have excessive safety stock"
            )
            detailed_analysis['excessive_safety_stock_items'] = high_ss_items
            
            analysis['insights'].append({
                'type': 'OPPORTUNITY',
                'category': 'SAFETY_STOCK',
                'message': f"{len(high_ss_items)} items have excessive safety stock",
                'priority': 'MEDIUM'
            })
        
        analysis['detailed_analysis']['safety_stock'] = detailed_analysis
    
    def _analyze_time_based_parameters(self, job_results: Dict[str, Any], analysis: Dict[str, Any]):
        """Analyze time-based parameter processing results."""
        tbp_results = job_results.get('processes', {}).get('time_based_parameters', {})
        
        detailed_analysis = {
            'total_parameters': tbp_results.get('total_parameters', 0),
            'processed_parameters': tbp_results.get('processed_parameters', 0),
            'affected_items': tbp_results.get('affected_items', 0),
            'errors': tbp_results.get('errors', 0),
            'key_findings': []
        }
        
        if detailed_analysis['total_parameters'] > 0:
            success_rate = (detailed_analysis['processed_parameters'] / 
                          detailed_analysis['total_parameters'] * 100)
            detailed_analysis['success_rate'] = success_rate
            
            if success_rate < 100:
                detailed_analysis['key_findings'].append(
                    f"Only {success_rate:.1f}% of time-based parameters were processed successfully"
                )
                
                analysis['insights'].append({
                    'type': 'CONCERN',
                    'category': 'TIME_BASED_PARAMETERS',
                    'message': f"Time-based parameter processing had {detailed_analysis['errors']} errors",
                    'priority': 'MEDIUM'
                })
        
        analysis['detailed_analysis']['time_based_parameters'] = detailed_analysis
    
    def _analyze_order_generation(self, job_results: Dict[str, Any], analysis: Dict[str, Any]):
        """Analyze order generation results."""
        order_results = job_results.get('processes', {}).get('generate_orders', {})
        
        detailed_analysis = {
            'total_vendors': order_results.get('total_vendors', 0),
            'generated_orders': order_results.get('generated_orders', 0),
            'total_items': order_results.get('total_items', 0),
            'errors': order_results.get('errors', 0),
            'key_findings': []
        }
        
        # Analyze order generation efficiency
        if detailed_analysis['total_vendors'] > 0:
            order_rate = (detailed_analysis['generated_orders'] / 
                         detailed_analysis['total_vendors'] * 100)
            detailed_analysis['order_generation_rate'] = order_rate
            
            if order_rate < 50:  # Threshold for concern
                detailed_analysis['key_findings'].append(
                    f"Only {order_rate:.1f}% of vendors generated orders"
                )
                
                analysis['insights'].append({
                    'type': 'CONCERN',
                    'category': 'ORDER_GENERATION',
                    'message': f"Low order generation rate: {order_rate:.1f}%",
                    'priority': 'MEDIUM'
                })
        
        # Analyze order details
        if order_results.get('order_details'):
            total_order_value = 0
            bracket_distribution = {}
            
            for order_detail in order_results['order_details']:
                # Extract order info to calculate value (placeholder for now)
                # In real implementation, we'd query the order table
                pass
            
            detailed_analysis['total_order_value'] = total_order_value
            detailed_analysis['bracket_distribution'] = bracket_distribution
        
        # Check for vendors without orders
        vendors_without_orders = self._identify_vendors_without_orders()
        if vendors_without_orders:
            detailed_analysis['key_findings'].append(
                f"{len(vendors_without_orders)} active vendors didn't generate orders"
            )
            detailed_analysis['vendors_without_orders'] = vendors_without_orders
            
            if len(vendors_without_orders) > 5:  # Threshold for concern
                analysis['insights'].append({
                    'type': 'CONCERN',
                    'category': 'ORDER_GENERATION',
                    'message': f"{len(vendors_without_orders)} vendors didn't generate orders",
                    'priority': 'MEDIUM'
                })
        
        analysis['detailed_analysis']['order_generation'] = detailed_analysis
    
    def _determine_overall_health(self, analysis: Dict[str, Any]):
        """Determine overall health status based on analysis."""
        error_count = sum(
            result.get('errors', 0) 
            for result in analysis['detailed_analysis'].values()
        )
        
        high_priority_concerns = sum(
            1 for insight in analysis['insights'] 
            if insight.get('priority') == 'HIGH' and insight.get('type') == 'CONCERN'
        )
        
        if error_count > 0 or high_priority_concerns > 3:
            analysis['overall_health'] = 'NEEDS_ATTENTION'
        elif high_priority_concerns > 0:
            analysis['overall_health'] = 'FAIR'
        else:
            analysis['overall_health'] = 'HEALTHY'
    
    def _generate_recommendations(self, analysis: Dict[str, Any]):
        """Generate specific recommendations based on analysis."""
        recommendations = []
        
        # Recommendations for out of stock items
        out_of_stock_count = self._get_out_of_stock_count()
        if out_of_stock_count > 0:
            recommendations.append({
                'title': 'Address Out of Stock Items',
                'priority': 'HIGH',
                'category': 'STOCK_STATUS',
                'description': f"There are {out_of_stock_count} items currently out of stock.",
                'action_items': [
                    "Review items with zero inventory and expedite orders",
                    "Investigate root causes for stockouts",
                    "Consider emergency purchases for critical items"
                ]
            })
        
        # Recommendations for lost sales
        lost_sales_details = analysis['detailed_analysis'].get('lost_sales', {})
        if lost_sales_details.get('calculated_lost_sales', 0) > 0:
            recommendations.append({
                'title': 'Minimize Lost Sales',
                'priority': 'HIGH',
                'category': 'LOST_SALES',
                'description': f"Calculated lost sales: {lost_sales_details['calculated_lost_sales']:.1f} units",
                'action_items': [
                    "Increase safety stock for items with frequent stockouts",
                    "Review lead times and order cycles",
                    "Consider forecasting method adjustments"
                ]
            })
        
        # Recommendations for safety stock optimization
        low_ss_items = self._identify_low_safety_stock_items()
        if low_ss_items:
            recommendations.append({
                'title': 'Optimize Safety Stock',
                'priority': 'HIGH',
                'category': 'SAFETY_STOCK',
                'description': f"{len(low_ss_items)} items have insufficient safety stock",
                'action_items': [
                    "Review service level goals for critical items",
                    "Consider increasing safety stock for items with high demand variability",
                    "Review lead time reliability for these items"
                ]
            })
        
        # Recommendations for order generation issues
        order_details = analysis['detailed_analysis'].get('order_generation', {})
        if order_details.get('order_generation_rate', 100) < 50:
            recommendations.append({
                'title': 'Improve Order Generation',
                'priority': 'MEDIUM',
                'category': 'ORDER_GENERATION',
                'description': f"Low order generation rate: {order_details['order_generation_rate']:.1f}%",
                'action_items': [
                    "Review vendor order cycles",
                    "Check for deactivated vendors",
                    "Investigate why vendors aren't reaching order points"
                ]
            })
        
        # Recommendations for exceptions
        unresolved_exceptions = self._get_unresolved_exceptions_count()
        if unresolved_exceptions > 10:
            recommendations.append({
                'title': 'Address Unresolved Exceptions',
                'priority': 'MEDIUM',
                'category': 'EXCEPTIONS',
                'description': f"{unresolved_exceptions} unresolved exceptions in the system",
                'action_items': [
                    "Review and resolve tracking signal exceptions",
                    "Address demand filter exceptions",
                    "Consider automation for common exception types"
                ]
            })
        
        analysis['recommendations'] = recommendations
    
    def _create_executive_summary(self, analysis: Dict[str, Any]) -> str:
        """Create an executive summary of the analysis."""
        summary_parts = []
        
        # Overall health
        summary_parts.append(
            f"System Health: {analysis['overall_health']}"
        )
        
        # Key metrics
        stock_status = analysis['detailed_analysis'].get('stock_status', {})
        summary_parts.append(
            f"- {stock_status.get('updated_items', 0)} of {stock_status.get('total_items', 0)} "
            f"items updated successfully"
        )
        
        lost_sales = analysis['detailed_analysis'].get('lost_sales', {})
        if lost_sales.get('calculated_lost_sales', 0) > 0:
            summary_parts.append(
                f"- Lost sales: {lost_sales['calculated_lost_sales']:.1f} units "
                f"(${lost_sales.get('lost_sales_value', 0):.2f})"
            )
        
        order_gen = analysis['detailed_analysis'].get('order_generation', {})
        summary_parts.append(
            f"- Generated {order_gen.get('generated_orders', 0)} orders for "
            f"{order_gen.get('total_items', 0)} items"
        )
        
        # Top concerns
        high_priority_concerns = [
            insight for insight in analysis['insights']
            if insight.get('priority') == 'HIGH' and insight.get('type') == 'CONCERN'
        ]
        
        if high_priority_concerns:
            summary_parts.append("\nKey Concerns:")
            for concern in high_priority_concerns[:3]:  # Top 3 concerns
                summary_parts.append(f"- {concern['message']}")
        
        # Top recommendations
        if analysis['recommendations']:
            summary_parts.append("\nTop Recommendations:")
            for rec in analysis['recommendations'][:3]:  # Top 3 recommendations
                summary_parts.append(f"- {rec['title']}")
        
        return "\n".join(summary_parts)
    
    # Helper methods for data analysis
    
    def _get_out_of_stock_count(self) -> int:
        """Get count of items currently out of stock."""
        return self.session.query(Item).filter(
            Item.on_hand <= 0,
            Item.buyer_class.in_(['R', 'W'])
        ).count()
    
    def _estimate_lost_sales_value(self, lost_sales_units: float) -> float:
        """Estimate monetary value of lost sales."""
        # Get average sales price of items with lost sales
        avg_price = self.session.query(func.avg(Item.sales_price)).join(
            DemandHistory, Item.id == DemandHistory.item_id
        ).filter(
            DemandHistory.lost_sales > 0
        ).scalar()
        
        return lost_sales_units * (avg_price or 0)
    
    def _identify_frequent_stockout_items(self) -> List[Dict]:
        """Identify items with frequent stockouts."""
        # Items with multiple stockout days in recent periods
        results = []
        recent_periods = self.session.query(
            DemandHistory.item_id,
            func.sum(DemandHistory.out_of_stock_days).label('total_oos_days'),
            func.count().label('periods_checked')
        ).join(Item, DemandHistory.item_id == Item.id).filter(
            DemandHistory.period_year >= datetime.now().year - 1
        ).group_by(DemandHistory.item_id).having(
            func.sum(DemandHistory.out_of_stock_days) > 3
        ).all()
        
        for item_id, total_oos_days, periods_checked in recent_periods:
            item = self.session.query(Item).get(item_id)
            if item:
                results.append({
                    'item_id': item.item_id,
                    'description': item.description,
                    'total_oos_days': total_oos_days,
                    'periods_checked': periods_checked
                })
        
        return results
    
    def _identify_low_safety_stock_items(self) -> List[Dict]:
        """Identify items with insufficient safety stock."""
        result = []
        items = self.session.query(Item).filter(
            Item.buyer_class.in_(['R', 'W']),
            Item.service_level_attained < Item.service_level_goal
        ).all()
        
        for item in items:
            if item.sstf and item.sstf < 1:  # Safety stock less than 1 day
                result.append({
                    'item_id': item.item_id,
                    'description': item.description,
                    'current_sstf': item.sstf,
                    'service_level_goal': item.service_level_goal,
                    'service_level_attained': item.service_level_attained
                })
        
        return result
    
    def _identify_excessive_safety_stock_items(self) -> List[Dict]:
        """Identify items with excessive safety stock."""
        result = []
        items = self.session.query(Item).filter(
            Item.buyer_class.in_(['R', 'W']),
            Item.service_level_attained > Item.service_level_goal + 5  # 5% buffer
        ).all()
        
        for item in items:
            if item.sstf and item.sstf > 7:  # Safety stock more than 7 days
                result.append({
                    'item_id': item.item_id,
                    'description': item.description,
                    'current_sstf': item.sstf,
                    'service_level_goal': item.service_level_goal,
                    'service_level_attained': item.service_level_attained
                })
        
        return result
    
    def _identify_vendors_without_orders(self) -> List[Dict]:
        """Identify active vendors that didn't generate orders."""
        # Get vendors that should have generated orders but didn't
        today = date.today()
        vendors = self.session.query(Vendor).filter(
            Vendor.active_items_count > 0,
            Vendor.vendor_type == 'REGULAR',
            or_(Vendor.deactivate_until.is_(None), Vendor.deactivate_until < today)
        ).all()
        
        result = []
        for vendor in vendors:
            # Check if vendor generated any orders today
            order_count = self.session.query(func.count(Order.id)).filter(
                Order.vendor_id == vendor.id,
                func.date(Order.order_date) == today
            ).scalar()
            
            if not order_count:
                result.append({
                    'vendor_id': vendor.vendor_id,
                    'name': vendor.name,
                    'active_items': vendor.active_items_count,
                    'last_order_date': vendor.next_order_date
                })
        
        return result[:10]  # Limit to 10 for reporting purposes
    
    def _get_unresolved_exceptions_count(self) -> int:
        """Get count of unresolved exceptions."""
        return self.session.query(HistoryException).filter(
            HistoryException.is_resolved == False
        ).count()
    
    def generate_html_report(self, analysis: Dict[str, Any]) -> str:
        """Generate an HTML report from the analysis."""
        html = """
<!DOCTYPE html>
<html>
<head>
    <title>Nightly Job Analysis Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { background-color: #f0f0f0; padding: 20px; }
        .section { margin: 20px 0; }
        .insight { padding: 10px; margin: 5px 0; border-left: 4px solid; }
        .insight.HIGH { border-color: red; background-color: #ffe6e6; }
        .insight.MEDIUM { border-color: orange; background-color: #fff0e6; }
        .insight.LOW { border-color: green; background-color: #e6ffe6; }
        .recommendation { background-color: #f9f9f9; padding: 15px; margin: 10px 0; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .health-HEALTHY { color: green; }
        .health-FAIR { color: orange; }
        .health-NEEDS_ATTENTION { color: red; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Nightly Job Analysis Report</h1>
        <p>Date: {analysis_date}</p>
        <p>Overall Health: <span class="health-{overall_health}">{overall_health}</span></p>
    </div>
    
    <div class="section">
        <h2>Executive Summary</h2>
        <pre>{executive_summary}</pre>
    </div>
    
    <div class="section">
        <h2>Key Insights</h2>
        {insights_html}
    </div>
    
    <div class="section">
        <h2>Recommendations</h2>
        {recommendations_html}
    </div>
    
    <div class="section">
        <h2>Detailed Analysis</h2>
        {detailed_analysis_html}
    </div>
</body>
</html>
        """
        
        # Generate insights HTML
        insights_html = ""
        for insight in analysis.get('insights', []):
            insights_html += f"""
            <div class="insight {insight['priority']}">
                <strong>{insight['category']}</strong>: {insight['message']}
            </div>
            """
        
        # Generate recommendations HTML
        recommendations_html = ""
        for rec in analysis.get('recommendations', []):
            action_items = "\n".join([f"• {item}" for item in rec.get('action_items', [])])
            recommendations_html += f"""
            <div class="recommendation">
                <h3>{rec['title']} (Priority: {rec['priority']})</h3>
                <p>{rec['description']}</p>
                <p><strong>Action Items:</strong></p>
                <pre>{action_items}</pre>
            </div>
            """
        
        # Generate detailed analysis HTML
        detailed_analysis_html = ""
        for category, details in analysis.get('detailed_analysis', {}).items():
            detailed_analysis_html += f"""
            <h3>{category.replace('_', ' ').title()}</h3>
            <table>
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
            """
            
            for key, value in details.items():
                if isinstance(value, (list, dict)):
                    continue  # Skip complex objects for now
                detailed_analysis_html += f"""
                <tr>
                    <td>{key.replace('_', ' ').title()}</td>
                    <td>{value}</td>
                </tr>
                """
            
            detailed_analysis_html += "</table>"
        
        # Format the HTML
        return html.format(
            analysis_date=analysis['analysis_date'],
            overall_health=analysis['overall_health'],
            executive_summary=analysis['executive_summary'],
            insights_html=insights_html,
            recommendations_html=recommendations_html,
            detailed_analysis_html=detailed_analysis_html
        )