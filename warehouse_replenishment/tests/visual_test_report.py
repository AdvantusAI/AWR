# warehouse_replenishment/tests/run_supabase_tests.py
#!/usr/bin/env python
"""
Comprehensive Supabase test runner that executes all tests and generates detailed reports.
"""

import sys
import os
import logging
import json
import argparse
from datetime import datetime
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from tests.report_generator import generate_html_report, save_test_results

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('supabase_test_runner')

class SupabaseTestRunner:
    """Comprehensive test runner for Supabase integration."""
    
    def __init__(self):
        self.results = {}
        self.start_time = None
        self.end_time = None
    
    def run_all_tests(self):
        """Run all Supabase tests and collect results."""
        self.start_time = datetime.now()
        
        # Test 1: Configuration Check
        self.results['configuration'] = self.test_configuration()
        
        # Test 2: Direct Connection
        self.results['direct_connection'] = self.test_direct_connection()
        
        # Test 3: Database Layer Connection
        self.results['database_layer'] = self.test_database_layer()
        
        # Test 4: Basic Query Operations
        self.results['basic_queries'] = self.test_basic_queries()
        
        # Test 5: Database Interface
        self.results['database_interface'] = self.test_database_interface()
        
        # Test 6: Database Adapter
        self.results['database_adapter'] = self.test_database_adapter()
        
        # Test 7: CRUD Operations
        self.results['crud_operations'] = self.test_crud_operations()
        
        # Test 8: Service Integration
        self.results['service_integration'] = self.test_service_integration()
        
        self.end_time = datetime.now()
        
        return self.results
    
    def test_configuration(self):
        """Test if Supabase configuration is properly set up."""
        result = {'passed': False, 'details': '', 'error': ''}
        
        try:
            from config import config
            
            # Check database type
            db_type = config.get('DATABASE', 'type', default='postgresql')
            # Remove any comments from the value
            db_type = db_type.split('#')[0].strip()
            if db_type != 'supabase':
                result['details'] = f"Database type is '{db_type}', not 'supabase'"
                return result
            
            # Check Supabase credentials
            env_url = os.getenv('SUPABASE_URL')
            env_key = os.getenv('SUPABASE_KEY')
            config_url = config.get('SUPABASE', 'url', default='')
            config_key = config.get('SUPABASE', 'key', default='')
            
            url_source = None
            key_source = None
            
            if env_url:
                url_source = "environment variable"
            elif config_url:
                url_source = "config file"
            
            if env_key:
                key_source = "environment variable"
            elif config_key:
                key_source = "config file"
            
            if not url_source or not key_source:
                result['error'] = f"Missing credentials: URL from {url_source}, KEY from {key_source}"
                return result
            
            result['passed'] = True
            result['details'] = f"Supabase credentials found - URL: {url_source}, KEY: {key_source}"
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def test_direct_connection(self):
        """Test direct connection to Supabase."""
        result = {'passed': False, 'details': '', 'error': ''}
        
        try:
            from supabase import create_client, Client
            
            url = os.getenv('SUPABASE_URL')
            key = os.getenv('SUPABASE_KEY')
            
            if not (url and key):
                from config import config
                url = config.get('SUPABASE', 'url', default='')
                key = config.get('SUPABASE', 'key', default='')
            
            supabase: Client = create_client(url, key)
            
            # Test connection
            result_data = supabase.table('company').select('id').limit(1).execute()
            
            result['passed'] = True
            result['details'] = f"Connected successfully. Tables accessible: {result_data is not None}"
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def test_database_layer(self):
        """Test database layer initialization."""
        result = {'passed': False, 'details': '', 'error': ''}
        
        try:
            from db import db, initialize, get_db_type
            
            initialize()
            db_type = get_db_type()
            
            if db_type != "supabase":
                result['error'] = f"Expected 'supabase' but got '{db_type}'"
                return result
            
            client = db.get_supabase()
            if client is None:
                result['error'] = "Failed to get Supabase client"
                return result
            
            result['passed'] = True
            result['details'] = "Database layer initialized successfully"
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def test_basic_queries(self):
        """Test basic query operations."""
        result = {'passed': False, 'details': '', 'error': ''}
        
        try:
            from db import db
            client = db.get_supabase()
            
            # Test query
            query_result = client.table('company').select('*').limit(5).execute()
            
            if hasattr(query_result, 'error') and query_result.error:
                result['error'] = f"Query error: {query_result.error}"
                return result
            
            result['passed'] = True
            result['details'] = f"Query successful. Found {len(query_result.data)} records"
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def test_database_interface(self):
        """Test database interface abstraction."""
        result = {'passed': False, 'details': '', 'error': ''}
        
        try:
            from db import get_db_interface
            interface = get_db_interface()
            
            # Test query method
            data = interface.query('company', limit=3)
            
            result['passed'] = True
            result['details'] = f"Interface query successful. Got {len(data)} records"
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def test_database_adapter(self):
        """Test database adapter."""
        result = {'passed': False, 'details': '', 'error': ''}
        
        try:
            from db import database_adapter
            from models import Company
            
            # Test query_all
            companies = database_adapter.query_all(Company, limit=3)
            
            # Test get_by_id if companies exist
            if companies:
                company = database_adapter.get_by_id(Company, companies[0].id)
                if company is None:
                    result['error'] = "get_by_id returned None"
                    return result
            
            result['passed'] = True
            result['details'] = f"Adapter working. Found {len(companies)} companies"
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def test_crud_operations(self):
        """Test CRUD operations with test data."""
        result = {'passed': False, 'details': '', 'error': ''}
        
        try:
            from db import database_adapter
            from models import Company
            
            # Create test company
            test_name = f"Test Company {datetime.now().strftime('%Y%m%d%H%M%S')}"
            test_company = Company(
                name=test_name,
                basic_alpha_factor=10.0,
                demand_from_days_out=1,
                service_level_goal=95.0
            )
            
            # Save
            saved = database_adapter.save(test_company)
            
            # Read
            read_back = database_adapter.get_by_id(Company, saved.id)
            
            # Update
            read_back.name = f"Updated {test_name}"
            updated = database_adapter.save(read_back)
            
            # Clean up (delete the test record)
            database_adapter.delete(updated)
            
            result['passed'] = True
            result['details'] = "CRUD operations successful (Create, Read, Update, Delete)"
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def test_service_integration(self):
        """Test service integration with Supabase."""
        result = {'passed': False, 'details': '', 'error': ''}
        
        try:
            from services.item_service import ItemService
            from db import db
            
            # Create service instance
            if db.db_type == "postgresql":
                service = ItemService(db.get_session())
            else:
                service = ItemService(db.get_supabase())
            
            # Test getting items
            items = service.get_items()
            
            result['passed'] = True
            result['details'] = f"Service integration successful. Found {len(items)} items"
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def generate_reports(self, output_dir=None):
        """Generate test reports."""
        if output_dir is None:
            output_dir = Path(__file__).parent
        else:
            output_dir = Path(output_dir)
        
        # Generate HTML report
        html_file = output_dir / "supabase_test_report.html"
        generate_html_report(self.results, str(html_file))
        
        # Generate JSON report
        json_file = output_dir / "supabase_test_results.json"
        save_test_results(self.results, str(json_file))
        
        # Add metadata to results
        test_metadata = {
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration': str(self.end_time - self.start_time) if self.start_time and self.end_time else None,
            'python_version': sys.version,
            'platform': sys.platform
        }
        
        # Save detailed results with metadata
        detailed_results = {
            'metadata': test_metadata,
            'test_results': self.results
        }
        
        detailed_json_file = output_dir / "supabase_test_results_detailed.json"
        save_test_results(detailed_results, str(detailed_json_file))
        
        return {
            'html_report': str(html_file),
            'json_report': str(json_file),
            'detailed_json': str(detailed_json_file)
        }

def main():
    """Main entry point for test runner."""
    parser = argparse.ArgumentParser(description='Run comprehensive Supabase tests')
    parser.add_argument('--url', help='Override Supabase URL')
    parser.add_argument('--key', help='Override Supabase API key')
    parser.add_argument('--output', '-o', help='Output directory for reports')
    parser.add_argument('--quick', action='store_true', help='Run quick tests only')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    # Override environment variables if provided
    if args.url:
        os.environ['SUPABASE_URL'] = args.url
    if args.key:
        os.environ['SUPABASE_KEY'] = args.key
    
    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    print("=" * 50)
    print("RUNNING COMPREHENSIVE SUPABASE TESTS")
    print("=" * 50)
    
    # Create and run test runner
    runner = SupabaseTestRunner()
    results = runner.run_all_tests()
    
    # Generate reports
    reports = runner.generate_reports(args.output if args.output else None)
    
    # Print summary
    print("\n" + "=" * 50)
    print("TEST SUMMARY")
    print("=" * 50)
    
    total_tests = len(results)
    passed_tests = sum(1 for r in results.values() if r.get('passed', False))
    
    for test_name, test_result in results.items():
        status = "PASSED" if test_result.get('passed', False) else "FAILED"
        symbol = "✅" if test_result.get('passed', False) else "❌"
        print(f"{symbol} {test_name.replace('_', ' ').title()}: {status}")
    
    print(f"\nTotal: {passed_tests}/{total_tests} tests passed")
    
    # Print report locations
    print("\nReports generated:")
    for report_type, report_path in reports.items():
        print(f"- {report_type}: {report_path}")
    
    # Final verdict
    if passed_tests == total_tests:
        print("\n🎉 All tests passed! Supabase integration is working perfectly. 🎉")
        return 0
    else:
        print("\n⚠️ Some tests failed. Please check the detailed reports for more information.")
        return 1

if __name__ == "__main__":
    sys.exit(main())