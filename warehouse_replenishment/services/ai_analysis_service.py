from sqlalchemy.orm import Session
from warehouse_replenishment.models import AIAnalysis
from warehouse_replenishment.models import Warehouse
from warehouse_replenishment.models import ForecastResult
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class AIAnalysisService:
    def __init__(self, session: Session):
        self.session = session

    def analyze_warehouse_performance(self, warehouse_id: int, period: str) -> AIAnalysis:
        """Analyze warehouse performance and create AI analysis record."""
        try:
            # Get warehouse data
            warehouse = self.session.query(Warehouse).get(warehouse_id)
            if not warehouse:
                raise ValueError(f"Warehouse {warehouse_id} not found")

            # Get forecast results for the period
            forecast_results = self._get_forecast_results(warehouse_id, period)
            
            # Calculate performance metrics
            metrics = self._calculate_performance_metrics(forecast_results)
            
            # Generate observations and recommendations
            observations = self._generate_observations(metrics, forecast_results)
            recommendations = self._generate_recommendations(metrics, forecast_results)
            
            # Create AI analysis record
            analysis = AIAnalysis(
                warehouse_id=warehouse_id,
                period=period,
                accuracy_rate=metrics['accuracy_rate'],
                mape=metrics['mape'],
                wape=metrics['wape'],
                processed_items=metrics['processed_items'],
                within_tolerance=metrics['within_tolerance'],
                under_forecast=metrics['under_forecast'],
                over_forecast=metrics['over_forecast'],
                key_observations=observations,
                recommendations=recommendations
            )
            
            self.session.add(analysis)
            self.session.commit()
            
            logger.info(f"Created AI analysis for warehouse {warehouse_id} period {period}")
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing warehouse performance: {str(e)}")
            self.session.rollback()
            raise

    def _get_forecast_results(self, warehouse_id: int, period: str) -> dict:
        """Get forecast results for the warehouse and period."""
        # Query forecast results from the database
        results = self.session.query(ForecastResult).filter(
            ForecastResult.warehouse_id == warehouse_id,
            ForecastResult.period == period
        ).all()
        
        if not results:
            return {
                'total_items': 0,
                'accuracy_rate': 0.0,
                'mape': 0.0,
                'wape': 0.0,
                'error_distribution': {
                    'within_tolerance': 0,
                    'under_forecast': 0,
                    'over_forecast': 0
                },
                'top_missed_forecasts': []
            }
        
        # Calculate metrics
        total_items = len(results)
        within_tolerance = sum(1 for r in results if r.forecast_error <= r.tolerance)
        under_forecast = sum(1 for r in results if r.forecast_error < -r.tolerance)
        over_forecast = sum(1 for r in results if r.forecast_error > r.tolerance)
        
        # Calculate accuracy rate
        accuracy_rate = (within_tolerance / total_items) * 100 if total_items > 0 else 0
        
        # Calculate MAPE and WAPE
        total_absolute_percentage_error = sum(abs(r.forecast_error) for r in results)
        total_actual_demand = sum(r.actual_demand for r in results)
        
        mape = (total_absolute_percentage_error / total_items) * 100 if total_items > 0 else 0
        wape = (total_absolute_percentage_error / total_actual_demand) * 100 if total_actual_demand > 0 else 0
        
        # Get top missed forecasts
        top_missed = sorted(
            results,
            key=lambda r: abs(r.forecast_error),
            reverse=True
        )[:5]
        
        return {
            'total_items': total_items,
            'accuracy_rate': accuracy_rate,
            'mape': mape,
            'wape': wape,
            'error_distribution': {
                'within_tolerance': within_tolerance,
                'under_forecast': under_forecast,
                'over_forecast': over_forecast
            },
            'top_missed_forecasts': [
                {
                    'item_id': r.item_id,
                    'error_percentage': abs(r.forecast_error)
                }
                for r in top_missed
            ]
        }

    def _calculate_performance_metrics(self, forecast_results: dict) -> dict:
        """Calculate performance metrics from forecast results."""
        return {
            'accuracy_rate': forecast_results['accuracy_rate'],
            'mape': forecast_results['mape'],
            'wape': forecast_results['wape'],
            'processed_items': forecast_results['total_items'],
            'within_tolerance': forecast_results['error_distribution']['within_tolerance'],
            'under_forecast': forecast_results['error_distribution']['under_forecast'],
            'over_forecast': forecast_results['error_distribution']['over_forecast']
        }

    def _generate_observations(self, metrics: dict, forecast_results: dict) -> str:
        """Generate key observations based on performance metrics."""
        observations = []
        
        # Analyze accuracy
        if metrics['accuracy_rate'] < 80:
            observations.append(f"Low accuracy rate ({metrics['accuracy_rate']:.2f}%) indicates forecasting challenges.")
        
        # Analyze error distribution
        if metrics['over_forecast'] > metrics['under_forecast']:
            observations.append("System tends to over-forecast demand.")
        elif metrics['under_forecast'] > metrics['over_forecast']:
            observations.append("System tends to under-forecast demand.")
        
        # Analyze MAPE
        if metrics['mape'] > 50:
            observations.append(f"High MAPE ({metrics['mape']:.2f}%) suggests significant forecast errors.")
        
        # Add observations about top missed forecasts
        if forecast_results['top_missed_forecasts']:
            observations.append("Top forecast errors:")
            for item in forecast_results['top_missed_forecasts']:
                observations.append(f"- Item {item['item_id']}: {item['error_percentage']:.1f}% error")
        
        return "\n".join(observations)

    def _generate_recommendations(self, metrics: dict, forecast_results: dict) -> str:
        """Generate recommendations based on performance metrics."""
        recommendations = []
        
        # Recommendations based on accuracy
        if metrics['accuracy_rate'] < 80:
            recommendations.append("Review and adjust forecasting parameters for low-accuracy items.")
        
        # Recommendations based on error distribution
        if metrics['over_forecast'] > metrics['under_forecast']:
            recommendations.append("Consider reducing safety stock levels for items with consistent over-forecasting.")
        elif metrics['under_forecast'] > metrics['over_forecast']:
            recommendations.append("Consider increasing safety stock levels for items with consistent under-forecasting.")
        
        # Recommendations based on MAPE
        if metrics['mape'] > 50:
            recommendations.append("Investigate and address root causes of high forecast errors.")
        
        # Specific recommendations for top missed forecasts
        if forecast_results['top_missed_forecasts']:
            recommendations.append("Focus on improving forecasts for items with highest errors:")
            for item in forecast_results['top_missed_forecasts']:
                recommendations.append(f"- Item {item['item_id']}: Review demand patterns and adjust forecasting model")
        
        return "\n".join(recommendations) 