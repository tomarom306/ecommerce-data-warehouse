import sys
import os

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

from sqlalchemy import create_engine, text
from config.database import db_config

class FactLoader:
    """Populate fact tables"""
    
    def __init__(self):
        self.engine = db_config.get_engine()
    
    def check_and_add_missing_dates(self):
        """Check for missing dates and add them to dim_date"""
        print("Checking for missing dates...")
        
        query = text("""
        SELECT DISTINCT TO_CHAR(order_date, 'YYYYMMDD')::INTEGER as date_key
        FROM staging.orders
        WHERE TO_CHAR(order_date, 'YYYYMMDD')::INTEGER NOT IN (
            SELECT date_key FROM warehouse.dim_date
        )
        ORDER BY date_key
        """)
        
        with self.engine.begin() as conn:
            result = conn.execute(query)
            missing_dates = [row[0] for row in result]
        
        if missing_dates:
            print(f"  ⚠ Found {len(missing_dates)} missing dates, adding them...")
            
            # Insert missing dates
            for date_key in missing_dates:
                # Convert date_key back to date (e.g., 20260108 -> 2026-01-08)
                date_str = str(date_key)
                year = int(date_str[0:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                
                from datetime import date
                d = date(year, month, day)
                
                insert_query = text("""
                INSERT INTO warehouse.dim_date 
                (date_key, date, day_of_week, day_name, day_of_month, 
                 day_of_year, week_of_year, month, month_name, quarter, 
                 year, is_weekend, is_holiday)
                VALUES 
                (:date_key, :date, :day_of_week, :day_name, :day_of_month,
                 :day_of_year, :week_of_year, :month, :month_name, :quarter,
                 :year, :is_weekend, :is_holiday)
                ON CONFLICT (date_key) DO NOTHING
                """)
                
                with self.engine.begin() as conn:
                    conn.execute(insert_query, {
                        'date_key': date_key,
                        'date': d,
                        'day_of_week': d.weekday(),
                        'day_name': d.strftime('%A'),
                        'day_of_month': d.day,
                        'day_of_year': d.timetuple().tm_yday,
                        'week_of_year': d.isocalendar()[1],
                        'month': d.month,
                        'month_name': d.strftime('%B'),
                        'quarter': (d.month - 1) // 3 + 1,
                        'year': d.year,
                        'is_weekend': d.weekday() in [5, 6],
                        'is_holiday': False
                    })
            
            print(f"  ✓ Added {len(missing_dates)} missing dates")
        else:
            print("  ✓ All dates present in dim_date")
    
    def populate_fact_orders(self):
        """Populate order fact table"""
        print("Loading fact_orders...")
        
        query = text("""
        INSERT INTO warehouse.fact_orders 
        (order_id, order_date_key, customer_key, payment_method_key, 
         shipping_method_key, order_quantity, subtotal_amount, 
         shipping_cost, tax_amount, discount_amount, total_amount, order_status)
        SELECT 
            o.order_id,
            TO_CHAR(o.order_date, 'YYYYMMDD')::INTEGER as order_date_key,
            dc.customer_key,
            dpm.payment_method_key,
            dsm.shipping_method_key,
            COUNT(oi.order_item_id) as order_quantity,
            COALESCE(SUM(oi.line_total), 0) as subtotal_amount,
            o.shipping_cost,
            o.tax_amount,
            o.discount_amount,
            o.total_amount,
            o.order_status
        FROM staging.orders o
        LEFT JOIN staging.order_items oi ON o.order_id = oi.order_id
        LEFT JOIN warehouse.dim_customer dc ON o.customer_id = dc.customer_id AND dc.is_current = TRUE
        LEFT JOIN warehouse.dim_payment_method dpm ON o.payment_method = dpm.payment_method
        LEFT JOIN warehouse.dim_shipping_method dsm ON o.shipping_method = dsm.shipping_method
        WHERE o.order_id NOT IN (SELECT order_id FROM warehouse.fact_orders)
          AND TO_CHAR(o.order_date, 'YYYYMMDD')::INTEGER IN (SELECT date_key FROM warehouse.dim_date)
        GROUP BY o.order_id, o.order_date, dc.customer_key, 
                 dpm.payment_method_key, dsm.shipping_method_key,
                 o.shipping_cost, o.tax_amount, o.discount_amount, 
                 o.total_amount, o.order_status
        """)
        
        with self.engine.begin() as conn:
            result = conn.execute(query)
            rows = result.rowcount
        
        print(f"  ✓ Loaded {rows} orders to fact_orders")
        return rows
    
    def populate_fact_order_items(self):
        """Populate order items fact table"""
        print("Loading fact_order_items...")
        
        query = text("""
        INSERT INTO warehouse.fact_order_items 
        (order_key, product_key, order_date_key, quantity, unit_price, 
         unit_cost, line_total, discount_amount, profit)
        SELECT 
            fo.order_key,
            dp.product_key,
            fo.order_date_key,
            oi.quantity,
            oi.unit_price,
            COALESCE(dp.cost, 0) as unit_cost,
            oi.line_total,
            oi.discount_amount,
            (oi.line_total - (COALESCE(dp.cost, 0) * oi.quantity)) as profit
        FROM staging.order_items oi
        JOIN warehouse.fact_orders fo ON oi.order_id = fo.order_id
        JOIN warehouse.dim_product dp ON oi.product_id = dp.product_id AND dp.is_current = TRUE
        WHERE NOT EXISTS (
            SELECT 1 FROM warehouse.fact_order_items foi 
            WHERE foi.order_key = fo.order_key 
            AND foi.product_key = dp.product_key
        )
        """)
        
        with self.engine.begin() as conn:
            result = conn.execute(query)
            rows = result.rowcount
        
        print(f"  ✓ Loaded {rows} order items to fact_order_items")
        return rows
    
    def populate_all_facts(self):
        """Populate all fact tables"""
        print("="*60)
        print("Populating Fact Tables")
        print("="*60 + "\n")
        
        try:
            # First check and add any missing dates
            self.check_and_add_missing_dates()
            print()
            
            # Then load facts
            orders = self.populate_fact_orders()
            items = self.populate_fact_order_items()
            
            print("\n" + "="*60)
            print(f"✓ Loaded {orders} orders and {items} order items")
            print("="*60)
            
        except Exception as e:
            print(f"\n✗ Error: {e}")
            print("\nMake sure you've run:")
            print("  1. python -m src.utils.database_setup")
            print("  2. python -m src.extract.load_to_staging")
            print("  3. python -m src.load.populate_dimensions")
            import traceback
            traceback.print_exc()
            raise

if __name__ == "__main__":
    loader = FactLoader()
    loader.populate_all_facts()