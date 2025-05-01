#!/usr/bin/env python
# warehouse_replenishment/scripts/query_ai_analysis.py
import sys
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.db import session_scope
from warehouse_replenishment.models.ai_analysis import (
    AIAnalysis, AIAnalysisInsight, AIAnalysisRecommendation, AIAnalysisMetric
)
from warehouse_replenishment.logging_setup import get_logger

logger = get_logger('ai_analysis_query')


def query_latest_analysis(limit: int = 1) -> list:
    """Query the latest AI analysis results.
    
    Args:
        limit: Number of latest analyses to retrieve
        
    Returns:
        List of analysis results
    """
    with session_scope() as session:
        analyses = session.query(AIAnalysis).order_by(
            AIAnalysis.analysis_date.desc()
        ).limit(limit).all()
        
        results = []
        for analysis in analyses:
            insights = session.query(AIAnalysisInsight).filter(
                AIAnalysisInsight.analysis_id == analysis.id
            ).all()
            
            recommendations = session.query(AIAnalysisRecommendation).filter(
                AIAnalysisRecommendation.analysis_id == analysis.id
            ).all()
            
            metrics = session.query(AIAnalysisMetric).filter(
                AIAnalysisMetric.analysis_id == analysis.id
            ).all()
            
            results.append({
                'analysis': {
                    'id': analysis.id,
                    'analysis_date': analysis.analysis_date.isoformat(),
                    'job_date': analysis.job_date.isoformat(),
                    'overall_health': analysis.overall_health,
                    'executive_summary': analysis.executive_summary,
                    'total_items_processed': analysis.total_items_processed,
                    'total_orders_generated': analysis.total_orders_generated,
                    'lost_sales_value': analysis.lost_sales_value,
                    'out_of_stock_count': analysis.out_of_stock_count
                },
                'insights': [
                    {
                        'type': insight.type,
                        'category': insight.category,
                        'message': insight.message,
                        'priority': insight.priority,
                        'item_count': insight.item_count,
                        'financial_impact': insight.financial_impact
                    }
                    for insight in insights
                ],
                'recommendations': [
                    {
                        'title': rec.title,
                        'priority': rec.priority,
                        'category': rec.category,
                        'description': rec.description,
                        'action_items': rec.action_items,
                        'status': rec.status,
                        'estimated_savings': rec.estimated_savings,
                        'estimated_cost_to_implement': rec.estimated_cost_to_implement
                    }
                    for rec in recommendations
                ],
                'metrics': [
                    {
                        'metric_name': metric.metric_name,
                        'metric_value': metric.metric_value,
                        'metric_category': metric.metric_category,
                        'trend': metric.trend
                    }
                    for metric in metrics
                ]
            })
        
        return results


def query_analysis_by_date_range(start_date: datetime, end_date: datetime) -> list:
    """Query AI analysis results within a date range.
    
    Args:
        start_date: Start date
        end_date: End date
        
    Returns:
        List of analysis results
    """
    with session_scope() as session:
        analyses = session.query(AIAnalysis).filter(
            AIAnalysis.analysis_date >= start_date,
            AIAnalysis.analysis_date <= end_date
        ).order_by(AIAnalysis.analysis_date.desc()).all()
        
        results = []
        for analysis in analyses:
            # Simple version for now
            results.append({
                'id': analysis.id,
                'analysis_date': analysis.analysis_date.isoformat(),
                'job_date': analysis.job_date.isoformat(),
                'overall_health': analysis.overall_health,
                'lost_sales_value': analysis.lost_sales_value,
                'out_of_stock_count': analysis.out_of_stock_count,
                'recommendation_count': session.query(AIAnalysisRecommendation).filter(
                    AIAnalysisRecommendation.analysis_id == analysis.id
                ).count()
            })
        
        return results


def query_health_trends(days: int = 30) -> dict:
    """Query health status trends over time.
    
    Args:
        days: Number of days to analyze
        
    Returns:
        Dictionary with trend analysis
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    with session_scope() as session:
        analyses = session.query(AIAnalysis).filter(
            AIAnalysis.analysis_date >= start_date
        ).order_by(AIAnalysis.analysis_date).all()
        
        health_counts = {}
        daily_metrics = {}
        
        for analysis in analyses:
            # Count by health status
            health = analysis.overall_health
            health_counts[health] = health_counts.get(health, 0) + 1
            
            # Track daily metrics
            day = analysis.analysis_date.date().isoformat()
            if day not in daily_metrics:
                daily_metrics[day] = {
                    'lost_sales': 0,
                    'out_of_stock': 0,
                    'recommendations': 0
                }
            
            daily_metrics[day]['lost_sales'] += analysis.lost_sales_value or 0
            daily_metrics[day]['out_of_stock'] += analysis.out_of_stock_count or 0
            daily_metrics[day]['recommendations'] += session.query(
                AIAnalysisRecommendation
            ).filter(AIAnalysisRecommendation.analysis_id == analysis.id).count()
        
        return {
            'health_distribution': health_counts,
            'daily_metrics': daily_metrics,
            'total_analyses': len(analyses),
            'date_range': {
                'start': start_date.isoformat(),
                'end': end_date.isoformat()
            }
        }


def export_analysis_to_file(output_path: str, format: str = 'json', limit: int = 10):
    """Export AI analysis results to a file.
    
    Args:
        output_path: Output file path
        format: Output format ('json' or 'csv')
        limit: Number of latest analyses to export
    """
    analyses = query_latest_analysis(limit)
    
    if format == 'json':
        with open(output_path, 'w') as f:
            json.dump(analyses, f, indent=2, default=str)
    elif format == 'csv':
        import csv
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow([
                'analysis_id', 'analysis_date', 'job_date', 'overall_health',
                'total_items_processed', 'total_orders_generated', 'lost_sales_value',
                'out_of_stock_count', 'recommendation_count'
            ])
            
            # Write data
            for analysis in analyses:
                writer.writerow([
                    analysis['analysis']['id'],
                    analysis['analysis']['analysis_date'],
                    analysis['analysis']['job_date'],
                    analysis['analysis']['overall_health'],
                    analysis['analysis']['total_items_processed'],
                    analysis['analysis']['total_orders_generated'],
                    analysis['analysis']['lost_sales_value'],
                    analysis['analysis']['out_of_stock_count'],
                    len(analysis['recommendations'])
                ])
    else:
        raise ValueError(f"Unsupported format: {format}")
    
    logger.info(f"Exported {len(analyses)} analyses to {output_path}")


def print_summary(analysis: dict):
    """Print a summary of the analysis."""
    print("\n=== AI Analysis Summary ===")
    print(f"Date: {analysis['analysis']['analysis_date']}")
    print(f"Overall Health: {analysis['analysis']['overall_health']}")
    print(f"Items Processed: {analysis['analysis']['total_items_processed']}")
    print(f"Orders Generated: {analysis['analysis']['total_orders_generated']}")
    print(f"Lost Sales Value: ${analysis['analysis']['lost_sales_value']:.2f}")
    print(f"Out of Stock Items: {analysis['analysis']['out_of_stock_count']}")
    
    print("\n=== Top Insights ===")
    insights = sorted(analysis['insights'], key=lambda x: x['priority'], reverse=True)
    for i, insight in enumerate(insights[:5], 1):
        print(f"{i}. [{insight['priority']}] {insight['message']}")
    
    print("\n=== Top Recommendations ===")
    recommendations = sorted(analysis['recommendations'], 
                          key=lambda x: x['priority'], reverse=True)
    for i, rec in enumerate(recommendations[:5], 1):
        print(f"{i}. [{rec['priority']}] {rec['title']}")
        print(f"   {rec['description']}")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Query AI analysis results')
    
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # Latest analysis command
    latest_parser = subparsers.add_parser('latest', help='Get latest analyses')
    latest_parser.add_argument('--limit', type=int, default=1, help='Number of analyses')
    
    # Date range command
    range_parser = subparsers.add_parser('range', help='Get analyses in date range')
    range_parser.add_argument('--start', type=str, required=True, help='Start date (YYYY-MM-DD)')
    range_parser.add_argument('--end', type=str, required=True, help='End date (YYYY-MM-DD)')
    
    # Health trends command
    trends_parser = subparsers.add_parser('trends', help='Get health trends')
    trends_parser.add_argument('--days', type=int, default=30, help='Number of days')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export analyses to file')
    export_parser.add_argument('--output', type=str, required=True, help='Output file path')
    export_parser.add_argument('--format', choices=['json', 'csv'], default='json', help='Output format')
    export_parser.add_argument('--limit', type=int, default=10, help='Number of analyses')
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    if args.command == 'latest':
        analyses = query_latest_analysis(args.limit)
        for analysis in analyses:
            print_summary(analysis)
    
    elif args.command == 'range':
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
        end_date = datetime.strptime(args.end, '%Y-%m-%d')
        analyses = query_analysis_by_date_range(start_date, end_date)
        
        print(f"\n=== Analysis Results ({len(analyses)} found) ===")
        for analysis in analyses:
            print(f"{analysis['analysis_date']}: {analysis['overall_health']} - "
                  f"Lost Sales: ${analysis['lost_sales_value']:.2f}")
    
    elif args.command == 'trends':
        trends = query_health_trends(args.days)
        print(f"\n=== Health Trends ({args.days} days) ===")
        print(f"Total Analyses: {trends['total_analyses']}")
        print("\nHealth Distribution:")
        for health, count in trends['health_distribution'].items():
            print(f"  {health}: {count}")
    
    elif args.command == 'export':
        export_analysis_to_file(args.output, args.format, args.limit)
    
    else:
        print("Please specify a command. Use -h for help.")


if __name__ == "__main__":
    main()