
"""
E-Commerce ETL Pipeline - Master Script
Run the complete ETL pipeline without Airflow
"""

import sys
import os
from datetime import datetime

# Add project root to Python path
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)

def run_pipeline():
    """Execute the complete ETL pipeline"""
    
    print("="*70)
    print("E-COMMERCE DATA WAREHOUSE ETL PIPELINE")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # Step 1: Load staging data
        print("\n" + "="*70)
        print("STEP 1: Loading data to staging tables")
        print("="*70)
        from src.extract.load_to_staging import StagingLoader
        staging_loader = StagingLoader()
        staging_loader.load_all_sources()
        
        # Step 2: Populate dimensions
        print("\n" + "="*70)
        print("STEP 2: Populating dimension tables")
        print("="*70)
        from src.load.populate_dimensions import DimensionLoader
        dim_loader = DimensionLoader()
        dim_loader.populate_all_dimensions()
        
        # Step 3: Populate facts
        print("\n" + "="*70)
        print("STEP 3: Populating fact tables")
        print("="*70)
        from src.load.populate_facts import FactLoader
        fact_loader = FactLoader()
        fact_loader.populate_all_facts()
        
        # Step 4: Run data quality checks
        print("\n" + "="*70)
        print("STEP 4: Running data quality checks")
        print("="*70)
        from src.utils.data_quality import DataQualityChecker
        quality_checker = DataQualityChecker()
        all_passed = quality_checker.run_all_checks()
        
        # Summary
        print("\n" + "="*70)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*70)
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if all_passed:
            print("Status: ✓ SUCCESS - All checks passed")
            return 0
        else:
            print("Status: ⚠ WARNING - Some quality checks failed")
            return 1
            
    except Exception as e:
        print("\n" + "="*70)
        print("PIPELINE EXECUTION FAILED")
        print("="*70)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = run_pipeline()
    sys.exit(exit_code)