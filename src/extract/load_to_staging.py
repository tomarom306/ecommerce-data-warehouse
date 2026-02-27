import sys
import os

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

import pandas as pd
from sqlalchemy import create_engine
from config.database import db_config
import glob

class StagingLoader:
    """Load raw CSV data to staging tables"""
    
    def __init__(self):
        self.engine = db_config.get_engine()
    
    def load_csv_to_staging(self, csv_path, table_name):
        """Load CSV file to staging table"""
        try:
            print(f"Loading {csv_path}...")
            df = pd.read_csv(csv_path)
            
            # Convert datetime columns if they exist
            datetime_columns = ['order_date', 'created_at', 'updated_at', 'registration_date', 'created_date']
            for col in datetime_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Load to staging schema
            df.to_sql(
                table_name,
                self.engine,
                schema='staging',
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            print(f"  ✓ Loaded {len(df)} rows to staging.{table_name}")
            return len(df)
            
        except Exception as e:
            print(f"  ✗ Error loading {csv_path}: {e}")
            return 0
    
    def load_all_sources(self, data_dir='./data/raw'):
        """Load all CSV files from data directory"""
        # Make path absolute
        abs_data_dir = os.path.join(project_root, data_dir)
        
        if not os.path.exists(abs_data_dir):
            print(f"✗ Directory not found: {abs_data_dir}")
            print(f"Please create it and add CSV files, or run data generator first.")
            return
        
        csv_files = glob.glob(os.path.join(abs_data_dir, "*.csv"))
        
        if not csv_files:
            print(f"✗ No CSV files found in {abs_data_dir}")
            print(f"Please run data generator first: python -m src.utils.data_generator")
            return
        
        print(f"Found {len(csv_files)} CSV files\n")
        
        total_rows = 0
        for csv_file in csv_files:
            # Get table name from filename
            table_name = os.path.basename(csv_file).replace('.csv', '')
            rows = self.load_csv_to_staging(csv_file, table_name)
            total_rows += rows
        
        print(f"\n{'='*60}")
        print(f"✓ Successfully loaded {total_rows} total rows to staging")
        print(f"{'='*60}")

if __name__ == "__main__":
    print("="*60)
    print("Loading Data to Staging Tables")
    print("="*60 + "\n")
    
    loader = StagingLoader()
    loader.load_all_sources()