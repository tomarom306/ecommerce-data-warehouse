import sys
import os

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from config.database import db_config

class DimensionLoader:
    """Populate dimension tables"""
    
    def __init__(self):
        self.engine = db_config.get_engine()
    
    def populate_date_dimension(self, start_date='2022-01-01', end_date='2025-12-31'):
        """Generate and load date dimension"""
        print("Loading date dimension...")
        
        # Check if data already exists
        with self.engine.begin() as conn:  # Changed to begin() for auto-commit
            result = conn.execute(text("SELECT COUNT(*) FROM warehouse.dim_date"))
            count = result.scalar()
            
            if count > 0:
                print(f"  ℹ Date dimension already has {count} rows, skipping...")
                return
        
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        date_dim = pd.DataFrame({
            'date_key': [int(d.strftime('%Y%m%d')) for d in dates],
            'date': dates,
            'day_of_week': dates.dayofweek,
            'day_name': dates.day_name(),
            'day_of_month': dates.day,
            'day_of_year': dates.dayofyear,
            'week_of_year': dates.isocalendar().week,
            'month': dates.month,
            'month_name': dates.month_name(),
            'quarter': dates.quarter,
            'year': dates.year,
            'is_weekend': dates.dayofweek.isin([5, 6]),
            'is_holiday': False
        })
        
        date_dim.to_sql(
            'dim_date',
            self.engine,
            schema='warehouse',
            if_exists='append',
            index=False
        )
        
        print(f"  ✓ Loaded {len(date_dim)} dates to dim_date")
    
    def populate_customer_dimension(self):
        """Populate customer dimension with SCD Type 2"""
        print("Loading customer dimension...")
        
        query = text("""
        INSERT INTO warehouse.dim_customer 
        (customer_id, first_name, last_name, email, phone, address, 
         city, state, zip_code, country, customer_segment, is_active, 
         registration_date, effective_date, end_date, is_current)
        SELECT 
            customer_id,
            first_name,
            last_name,
            email,
            phone,
            address,
            city,
            state,
            zip_code,
            country,
            customer_segment,
            is_active,
            registration_date,
            registration_date as effective_date,
            NULL as end_date,
            TRUE as is_current
        FROM staging.customers
        WHERE customer_id NOT IN (
            SELECT customer_id 
            FROM warehouse.dim_customer 
            WHERE is_current = TRUE
        )
        """)
        
        with self.engine.begin() as conn:  # Changed to begin() for auto-commit
            result = conn.execute(query)
            rows = result.rowcount
        
        print(f"  ✓ Loaded {rows} customers to dim_customer")
    
    def populate_product_dimension(self):
        """Populate product dimension with SCD Type 2"""
        print("Loading product dimension...")
        
        query = text("""
        INSERT INTO warehouse.dim_product 
        (product_id, product_name, category, sub_category, brand, 
         price, cost, effective_date, end_date, is_current)
        SELECT 
            product_id,
            product_name,
            category,
            sub_category,
            brand,
            price,
            cost,
            created_date as effective_date,
            NULL as end_date,
            TRUE as is_current
        FROM staging.products
        WHERE product_id NOT IN (
            SELECT product_id 
            FROM warehouse.dim_product 
            WHERE is_current = TRUE
        )
        """)
        
        with self.engine.begin() as conn:  # Changed to begin() for auto-commit
            result = conn.execute(query)
            rows = result.rowcount
        
        print(f"  ✓ Loaded {rows} products to dim_product")
    
    def populate_payment_methods(self):
        """Populate payment method dimension"""
        print("Loading payment methods...")
        
        # Check if already populated
        with self.engine.begin() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM warehouse.dim_payment_method"))
            count = result.scalar()
            
            if count > 0:
                print(f"  ℹ Payment methods already loaded ({count} rows), skipping...")
                return
        
        payment_methods = pd.DataFrame({
            'payment_method': ['Credit Card', 'PayPal', 'Debit Card', 'Gift Card'],
            'payment_type': ['Card', 'Digital', 'Card', 'Card'],
            'processing_fee_pct': [2.5, 3.0, 2.0, 0.0]
        })
        
        payment_methods.to_sql(
            'dim_payment_method',
            self.engine,
            schema='warehouse',
            if_exists='append',
            index=False
        )
        
        print(f"  ✓ Loaded {len(payment_methods)} payment methods")
    
    def populate_shipping_methods(self):
        """Populate shipping method dimension"""
        print("Loading shipping methods...")
        
        # Check if already populated
        with self.engine.begin() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM warehouse.dim_shipping_method"))
            count = result.scalar()
            
            if count > 0:
                print(f"  ℹ Shipping methods already loaded ({count} rows), skipping...")
                return
        
        shipping_methods = pd.DataFrame({
            'shipping_method': ['Standard', 'Express', 'Next Day'],
            'estimated_days': [5, 3, 1],
            'base_cost': [5.99, 12.99, 24.99]
        })
        
        shipping_methods.to_sql(
            'dim_shipping_method',
            self.engine,
            schema='warehouse',
            if_exists='append',
            index=False
        )
        
        print(f"  ✓ Loaded {len(shipping_methods)} shipping methods")
    
    def populate_all_dimensions(self):
        """Populate all dimension tables"""
        print("="*60)
        print("Populating Dimension Tables")
        print("="*60 + "\n")
        
        try:
            self.populate_date_dimension()
            self.populate_payment_methods()
            self.populate_shipping_methods()
            self.populate_customer_dimension()
            self.populate_product_dimension()
            
            print("\n" + "="*60)
            print("✓ All dimension tables populated successfully!")
            print("="*60)
            
        except Exception as e:
            print(f"\n✗ Error: {e}")
            import traceback
            traceback.print_exc()
            raise

if __name__ == "__main__":
    loader = DimensionLoader()
    loader.populate_all_dimensions()