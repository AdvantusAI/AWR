# warehouse_replenishment/tests/quick_supabase_test.py
#!/usr/bin/env python
"""
Quick test script to verify Supabase connectivity.
Run this first to ensure Supabase is properly configured.
"""

import os
import sys
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

def check_environment():
    """Check if Supabase environment variables are set."""
    print("Checking Supabase environment variables...")
    
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')
    
    if url:
        print(f"✅ SUPABASE_URL is set: {url}")
    else:
        print("❌ SUPABASE_URL is not set")
    
    if key:
        print(f"✅ SUPABASE_KEY is set: {'*' * (len(key)-4)}{key[-4:]}")
    else:
        print("❌ SUPABASE_KEY is not set")
    
    return bool(url and key)

def test_direct_connection():
    """Test direct connection to Supabase."""
    print("\nTesting direct Supabase connection...")
    
    try:
        from supabase import create_client
        
        # Try environment variables first
        if os.getenv('SUPABASE_URL') and os.getenv('SUPABASE_KEY'):
            url = os.getenv('SUPABASE_URL')
            key = os.getenv('SUPABASE_KEY')
        else:
            # Fall back to config file
            from config import config
            url = config.get('SUPABASE', 'url', default='')
            key = config.get('SUPABASE', 'key', default='')

        if not url or not key:
            print("Error: Supabase URL and key must be provided either in environment variables or config file")
            return False

        try:
            # Create Supabase client
            supabase: Client = create_client(url, key)
            
            # Test connection by querying any table
            result = supabase.table('company').select('*').limit(1).execute()
            
            if result.data is not None:
                print("✅ Supabase connection test successful!")
                return True
            else:
                print("❌ Supabase connection test failed: No data returned")
                return False
            
        except Exception as e:
            print(f"❌ Supabase connection test failed: {str(e)}")
            return False

    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        return False

def test_warehouse_db_connection():
    """Test connection through warehouse replenishment database layer."""
    print("\nTesting warehouse DB connection...")
    
    try:
        # First, check configuration
        from config import config
        db_type = config.get('DATABASE', 'type', default='postgresql')
        print(f"Database type in config: {db_type}")
        
        if db_type != 'supabase':
            print("⚠️ WARNING: Database type is not set to 'supabase'")
            print("   To test Supabase, set DATABASE.type = supabase in settings.ini")
            return False
        
        from db import db, initialize, get_db_type
        
        # Initialize connection
        initialize()
        
        # Check database type
        current_type = get_db_type()
        print(f"Detected database type: {current_type}")
        
        if current_type != "supabase":
            print(f"❌ Expected 'supabase' but got '{current_type}'")
            return False
        
        # Get Supabase client through database layer
        client = db.get_supabase()
        print("✅ Successfully retrieved Supabase client")
        
        # Test query through database layer
        result = client.table('company').select('*').limit(1).execute()
        print("✅ Successfully queried through database layer")
        
        return True
        
    except Exception as e:
        print(f"❌ Database layer test failed: {str(e)}")
        return False

def main():
    """Run quick tests and provide diagnostic output."""
    print("=" * 50)
    print("QUICK SUPABASE CONNECTION TEST")
    print("=" * 50)
    
    # Check if we have environment variables
    env_ok = check_environment()
    
    # Test direct connection
    direct_ok = test_direct_connection()
    
    # Test through warehouse DB layer
    db_ok = test_warehouse_db_connection()
    
    print()
    print("=" * 50)
    print("TEST SUMMARY")
    print("=" * 50)
    
    if env_ok and direct_ok and db_ok:
        print("🎉 All tests passed! Supabase is properly configured and connected. 🎉")
    else:
        print("\n⚠️ Some tests failed. Please check your configuration:")
        
        if not env_ok:
            print("\n1. Set Supabase environment variables:")
            print("   export SUPABASE_URL=https://your-project.supabase.co")
            print("   export SUPABASE_KEY=your-supabase-key")
        
        if not db_ok:
            print("\n2. Ensure settings.ini is configured:")
            print("   [DATABASE]")
            print("   type = supabase")
            print("   ")
            print("   [SUPABASE]")
            print("   url = https://your-project.supabase.co")
            print("   key = your-supabase-key")

if __name__ == "__main__":
    main()