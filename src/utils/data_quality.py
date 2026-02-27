import sys
import os

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

import pandas as pd
from sqlalchemy import create_engine, text
from config.database import db_config

class DataQualityChecker:
    """Data quality validation - simplified version without Great Expectations"""
    
    def __init__(self):
        self.engine = db_config.get_engine()
    
    def check_staging_customers(self):
        """Validate customer staging data"""
        print("Checking staging.customers...")
        df = pd.read_sql("SELECT * FROM staging.customers", self.engine)
        
        checks = {
            'total_rows': len(df),
            'null_customer_ids': df['customer_id'].isnull().sum(),
            'duplicate_customer_ids': df['customer_id'].duplicated().sum(),
            'invalid_emails': df[~df['email'].str.contains('@', na=False)].shape[0],
            'valid_segments': df['customer_segment'].isin(['Premium', 'Standard', 'Basic']).sum(),
            'total_segments': len(df)
        }
        
        print(f"  Total Rows: {checks['total_rows']}")
        print(f"  ✓ Null Customer IDs: {checks['null_customer_ids']} (should be 0)")
        print(f"  ✓ Duplicate Customer IDs: {checks['duplicate_customer_ids']} (should be 0)")
        print(f"  ✓ Invalid Emails: {checks['invalid_emails']} (should be 0)")
        print(f"  ✓ Valid Segments: {checks['valid_segments']}/{checks['total_segments']}")
        
        passed = (
            checks['null_customer_ids'] == 0 and
            checks['duplicate_customer_ids'] == 0 and
            checks['invalid_emails'] == 0 and
            checks['valid_segments'] == checks['total_segments']
        )
        
        return passed
    
    def check_staging_products(self):
        """Validate product staging data"""
        print("\nChecking staging.products...")
        df = pd.read_sql("SELECT * FROM staging.products", self.engine)
        
        checks = {
            'total_rows': len(df),
            'null_product_ids': df['product_id'].isnull().sum(),
            'duplicate_product_ids': df['product_id'].duplicated().sum(),
            'negative_prices': (df['price'] < 0).sum(),
            'negative_costs': (df['cost'] < 0).sum(),
            'price_gt_cost': (df['price'] >= df['cost']).sum()
        }
        
        print(f"  Total Rows: {checks['total_rows']}")
        print(f"  ✓ Null Product IDs: {checks['null_product_ids']} (should be 0)")
        print(f"  ✓ Duplicate Product IDs: {checks['duplicate_product_ids']} (should be 0)")
        print(f"  ✓ Negative Prices: {checks['negative_prices']} (should be 0)")
        print(f"  ✓ Negative Costs: {checks['negative_costs']} (should be 0)")
        print(f"  ✓ Price >= Cost: {checks['price_gt_cost']}/{checks['total_rows']}")
        
        passed = (
            checks['null_product_ids'] == 0 and
            checks['duplicate_product_ids'] == 0 and
            checks['negative_prices'] == 0 and
            checks['negative_costs'] == 0
        )
        
        return passed
    
    def check_fact_orders(self):
        """Validate fact orders"""
        print("\nChecking warehouse.fact_orders...")
        
        with self.engine.begin() as conn:
            # Get basic counts
            result = conn.execute(text("SELECT COUNT(*) FROM warehouse.fact_orders"))
            total = result.scalar()
            
            # Check for nulls in critical fields
            result = conn.execute(text("""
                SELECT COUNT(*) FROM warehouse.fact_orders 
                WHERE order_id IS NULL OR order_date_key IS NULL
            """))
            null_critical = result.scalar()
            
            # Check for negative amounts
            result = conn.execute(text("""
                SELECT COUNT(*) FROM warehouse.fact_orders 
                WHERE total_amount < 0
            """))
            negative_amounts = result.scalar()
            
            # Check order statuses
            result = conn.execute(text("""
                SELECT COUNT(*) FROM warehouse.fact_orders 
                WHERE order_status NOT IN ('Completed', 'Pending', 'Cancelled', 'Returned')
            """))
            invalid_status = result.scalar()
            
            # Check referential integrity
            result = conn.execute(text("""
                SELECT COUNT(*) FROM warehouse.fact_orders 
                WHERE customer_key IS NULL 
                   OR payment_method_key IS NULL 
                   OR shipping_method_key IS NULL
            """))
            missing_references = result.scalar()
        
        print(f"  Total Rows: {total}")
        print(f"  ✓ Null Critical Fields: {null_critical} (should be 0)")
        print(f"  ✓ Negative Amounts: {negative_amounts} (should be 0)")
        print(f"  ✓ Invalid Status: {invalid_status} (should be 0)")
        print(f"  ✓ Missing References: {missing_references} (should be 0)")
        
        passed = (
            null_critical == 0 and
            negative_amounts == 0 and
            invalid_status == 0 and
            missing_references == 0
        )
        
        return passed
    
    def check_fact_order_items(self):
        """Validate fact order items"""
        print("\nChecking warehouse.fact_order_items...")
        
        with self.engine.begin() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM warehouse.fact_order_items"))
            total = result.scalar()
            
            # Check for negative quantities
            result = conn.execute(text("""
                SELECT COUNT(*) FROM warehouse.fact_order_items 
                WHERE quantity <= 0
            """))
            invalid_qty = result.scalar()
            
            # Check for negative prices
            result = conn.execute(text("""
                SELECT COUNT(*) FROM warehouse.fact_order_items 
                WHERE unit_price < 0
            """))
            negative_price = result.scalar()
            
            # Check line total calculation
            result = conn.execute(text("""
                SELECT COUNT(*) FROM warehouse.fact_order_items 
                WHERE ABS(line_total - (quantity * unit_price)) > 0.01
            """))
            incorrect_total = result.scalar()
        
        print(f"  Total Rows: {total}")
        print(f"  ✓ Invalid Quantities: {invalid_qty} (should be 0)")
        print(f"  ✓ Negative Prices: {negative_price} (should be 0)")
        print(f"  ✓ Incorrect Line Totals: {incorrect_total} (should be 0)")
        
        passed = (
            invalid_qty == 0 and
            negative_price == 0 and
            incorrect_total == 0
        )
        
        return passed
    
    def check_dimension_integrity(self):
        """Check dimension table integrity"""
        print("\nChecking dimension table integrity...")
        
        with self.engine.begin() as conn:
            # Check for multiple current records (SCD Type 2)
            result = conn.execute(text("""
                SELECT customer_id, COUNT(*) 
                FROM warehouse.dim_customer 
                WHERE is_current = TRUE 
                GROUP BY customer_id 
                HAVING COUNT(*) > 1
            """))
            duplicate_customers = len(result.fetchall())
            
            result = conn.execute(text("""
                SELECT product_id, COUNT(*) 
                FROM warehouse.dim_product 
                WHERE is_current = TRUE 
                GROUP BY product_id 
                HAVING COUNT(*) > 1
            """))
            duplicate_products = len(result.fetchall())
            
            # Check date dimension coverage
            result = conn.execute(text("""
                SELECT COUNT(DISTINCT order_date_key) 
                FROM warehouse.fact_orders
            """))
            distinct_dates = result.scalar()
            
            result = conn.execute(text("""
                SELECT COUNT(*) FROM warehouse.dim_date
            """))
            total_dates = result.scalar()
        
        print(f"  ✓ Duplicate Current Customers: {duplicate_customers} (should be 0)")
        print(f"  ✓ Duplicate Current Products: {duplicate_products} (should be 0)")
        print(f"  ✓ Date Coverage: {distinct_dates} order dates, {total_dates} total dates")
        
        passed = (
            duplicate_customers == 0 and
            duplicate_products == 0
        )
        
        return passed
    
    def run_all_checks(self):
        """Run all data quality checks"""
        print("="*60)
        print("Data Quality Checks")
        print("="*60 + "\n")
        
        results = {}
        
        try:
            results['staging_customers'] = self.check_staging_customers()
            results['staging_products'] = self.check_staging_products()
            results['fact_orders'] = self.check_fact_orders()
            results['fact_order_items'] = self.check_fact_order_items()
            results['dimension_integrity'] = self.check_dimension_integrity()
            
            print("\n" + "="*60)
            print("Summary")
            print("="*60)
            
            for check, passed in results.items():
                status = "✓ PASS" if passed else "✗ FAIL"
                print(f"{check}: {status}")
            
            all_passed = all(results.values())
            
            print("\n" + "="*60)
            if all_passed:
                print("✓ All data quality checks passed!")
            else:
                print("✗ Some data quality checks failed")
            print("="*60)
            
            return all_passed
            
        except Exception as e:
            print(f"\n✗ Error running checks: {e}")
            import traceback
            traceback.print_exc()
            return False

if __name__ == "__main__":
    checker = DataQualityChecker()
    checker.run_all_checks()