#!/usr/bin/env python
# test_ai_agent_integration.py - Test script to verify AI agent integration

import sys
from datetime import datetime, timedelta
from pathlib import Path
import json

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from .db import db, session_scope
from .batch.nightly_job import run_nightly_job
from .services.ai_agent_service import NightlyJobAnalyzer
from .scripts.query_ai_analysis import query_latest_analysis
from .logging_setup import get_logger

logger = get_logger('test_ai_agent')

def create_test_job_results():
    """Create test job results for AI analysis."""
    job_results = {
        'start_time': datetime.now() - timedelta(hours=2),
        'end_time': datetime.now(),
        'duration': timedelta(hours=2),
        'success': True,
        'processes': {
            'update_stock_status': {
                'total_items': 100,
                'updated_items': 95,
                'errors': 5
            },
            'calculate_lost_sales': {
                'total_items': 100,
                'updated_items': 90,
                'calculated_lost_sales': 150.0,
                'errors': 2
            },
            'update_safety_stock': {
                'total_items': 100,
                'updated_items': 98,
                'errors': 2
            },
            'time_based_parameters': {
                'total_parameters': 5,
                'processed_parameters': 5,
                'affected_items': 20,
                'errors': 0
            },
            'generate_orders': {
                'total_vendors': 20,
                'generated_orders': 15,
                'total_items': 75,
                'errors': 1,
                'order_details': []
            },
            'purge_accepted_orders': {
                'total_orders': 30,
                'purged_orders': 25,
                'errors': 0
            }
        }
    }
    return job_results

def test_ai_agent_standalone():
    """Test the AI agent in standalone mode."""
    logger.info("Testing AI agent in standalone mode...")
    
    try:
        with session_scope() as session:
            analyzer = NightlyJobAnalyzer(session)
            test_results = create_test_job_results()
            
            # Run analysis
            analysis = analyzer.analyze_nightly_job_results(test_results)
            
            # Print results
            print("\n=== AI Analysis Results ===")
            print(f"Overall Health: {analysis.get('overall_health', 'UNKNOWN')}")
            print(f"Database ID: {analysis.get('db_id', 'NOT_SAVED')}")
            
            # Print executive summary
            if 'executive_summary' in analysis:
                print("\nExecutive Summary:")
                print(analysis['executive_summary'])
            
            # Print insights
            if 'insights' in analysis:
                print(f"\nInsights: {len(analysis['insights'])}")
                for i, insight in enumerate(analysis['insights'][:5], 1):
                    print(f"  {i}. [{insight.get('priority', 'UNKNOWN')}] {insight.get('message', '')}")
            
            # Print recommendations
            if 'recommendations' in analysis:
                print(f"\nRecommendations: {len(analysis['recommendations'])}")
                for i, rec in enumerate(analysis['recommendations'][:5], 1):
                    print(f"  {i}. [{rec.get('priority', 'UNKNOWN')}] {rec.get('title', '')}")
            
            return True
            
    except Exception as e:
        logger.error(f"Error testing AI agent: {str(e)}")
        return False

def test_nightly_job_with_ai():
    """Test the full nightly job with AI integration."""
    logger.info("Testing nightly job with AI integration...")
    
    try:
        # Run nightly job
        job_results = run_nightly_job()
        
        # Check if AI analysis was executed
        if 'ai_analysis' in job_results:
            print("\n=== Nightly Job with AI Results ===")
            print(f"Job Success: {job_results.get('success', False)}")
            print(f"Job Duration: {job_results.get('duration', 'UNKNOWN')}")
            
            ai_analysis = job_results['ai_analysis']
            print(f"AI Analysis Success: {ai_analysis.get('overall_health', 'UNKNOWN')}")
            
            if 'executive_summary' in ai_analysis:
                print("\nExecutive Summary:")
                print(ai_analysis['executive_summary'])
            
            return True
        else:
            print("Error: AI analysis not found in job results")
            return False
            
    except Exception as e:
        logger.error(f"Error testing nightly job with AI: {str(e)}")
        return False

def verify_database_storage():
    """Verify that AI analysis is stored in the database."""
    logger.info("Verifying AI analysis storage in database...")
    
    try:
        # Query latest analysis
        analyses = query_latest_analysis(limit=1)
        
        if analyses:
            analysis = analyses[0]
            print("\n=== Database Verification ===")
            print(f"Found analysis ID: {analysis['analysis'].get('id', 'UNKNOWN')}")
            print(f"Analysis Date: {analysis['analysis'].get('analysis_date', 'UNKNOWN')}")
            print(f"Overall Health: {analysis['analysis'].get('overall_health', 'UNKNOWN')}")
            
            print(f"\nInsights: {len(analysis.get('insights', []))}")
            print(f"Recommendations: {len(analysis.get('recommendations', []))}")
            
            return True
        else:
            print("Error: No AI analysis found in database")
            return False
            
    except Exception as e:
        logger.error(f"Error verifying database storage: {str(e)}")
        return False

def main():
    """Main test function."""
    print("=== Testing AI Agent Integration ===\n")
    
    # Initialize database
    try:
        db.initialize()
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        return 1
    
    # Run tests
    test_results = []
    
    # Test 1: Standalone AI agent
    print("\n--- Test 1: Standalone AI Agent ---")
    test_results.append(test_ai_agent_standalone())
    
    # Test 2: Nightly job with AI integration
    print("\n--- Test 2: Nightly Job with AI Integration ---")
    test_results.append(test_nightly_job_with_ai())
    
    # Test 3: Database storage verification
    print("\n--- Test 3: Database Storage Verification ---")
    test_results.append(verify_database_storage())
    
    # Summary
    print("\n=== Test Summary ===")
    passed = sum(test_results)
    total = len(test_results)
    print(f"Tests Passed: {passed}/{total}")
    
    if passed == total:
        print("✅ All tests passed successfully!")
        return 0
    else:
        print("❌ Some tests failed. Check logs for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main())