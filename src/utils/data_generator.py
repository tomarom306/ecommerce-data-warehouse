import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import numpy as np

fake = Faker()
Faker.seed(42)
random.seed(42)

class EcommerceDataGenerator:
    """Generate realistic e-commerce sample data"""
    
    def __init__(self, num_customers=5000, num_products=500):
        self.num_customers = num_customers
        self.num_products = num_products
        
    def generate_customers(self):
        """Generate customer data"""
        customers = []
        
        for customer_id in range(1, self.num_customers + 1):
            customers.append({
                'customer_id': customer_id,
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'address': fake.street_address(),
                'city': fake.city(),
                'state': fake.state(),
                'zip_code': fake.zipcode(),
                'country': 'USA',
                'registration_date': fake.date_between(start_date='-2y', end_date='today'),
                'customer_segment': random.choice(['Premium', 'Standard', 'Basic']),
                'is_active': random.choice([True, True, True, False])  # 75% active
            })
        
        return pd.DataFrame(customers)
    
    def generate_products(self):
        """Generate product catalog"""
        categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys']
        products = []
        
        for product_id in range(1, self.num_products + 1):
            category = random.choice(categories)
            
            products.append({
                'product_id': product_id,
                'product_name': f"{fake.word().title()} {fake.word().title()}",
                'category': category,
                'sub_category': f"{category} - {fake.word().title()}",
                'brand': fake.company(),
                'price': round(random.uniform(9.99, 999.99), 2),
                'cost': round(random.uniform(5.0, 500.0), 2),
                'stock_quantity': random.randint(0, 1000),
                'supplier_id': random.randint(1, 50),
                'created_date': fake.date_between(start_date='-3y', end_date='-1y')
            })
        
        return pd.DataFrame(products)
    
    def generate_orders(self, customers_df, products_df, num_orders=20000):
        """Generate order transactions"""
        orders = []
        order_items = []
        
        for order_id in range(1, num_orders + 1):
            customer = customers_df.sample(1).iloc[0]
            order_date = fake.date_time_between(start_date='-1y', end_date='now')
            
            # Order header
            num_items = random.randint(1, 5)
            order_status = random.choices(
                ['Completed', 'Pending', 'Cancelled', 'Returned'],
                weights=[0.7, 0.15, 0.1, 0.05]
            )[0]
            
            orders.append({
                'order_id': order_id,
                'customer_id': customer['customer_id'],
                'order_date': order_date,
                'order_status': order_status,
                'payment_method': random.choice(['Credit Card', 'PayPal', 'Debit Card', 'Gift Card']),
                'shipping_method': random.choice(['Standard', 'Express', 'Next Day']),
                'shipping_cost': round(random.uniform(0, 25), 2),
                'tax_amount': 0,  # Will calculate later
                'discount_amount': round(random.uniform(0, 50), 2) if random.random() > 0.7 else 0,
                'total_amount': 0,  # Will calculate later
                'created_at': order_date,
                'updated_at': order_date
            })
            
            # Order items
            selected_products = products_df.sample(num_items)
            item_number = 1
            
            for _, product in selected_products.iterrows():
                quantity = random.randint(1, 3)
                unit_price = product['price']
                line_total = quantity * unit_price
                
                order_items.append({
                    'order_item_id': f"{order_id}_{item_number}",
                    'order_id': order_id,
                    'product_id': product['product_id'],
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'line_total': line_total,
                    'discount_amount': 0
                })
                
                item_number += 1
        
        orders_df = pd.DataFrame(orders)
        order_items_df = pd.DataFrame(order_items)
        
        # Calculate order totals
        order_totals = order_items_df.groupby('order_id')['line_total'].sum().reset_index()
        orders_df = orders_df.merge(order_totals, on='order_id', how='left', suffixes=('', '_items'))
        orders_df['tax_amount'] = (orders_df['line_total'] * 0.08).round(2)
        orders_df['total_amount'] = (
            orders_df['line_total'] + 
            orders_df['tax_amount'] + 
            orders_df['shipping_cost'] - 
            orders_df['discount_amount']
        ).round(2)
        orders_df.drop('line_total', axis=1, inplace=True)
        
        return orders_df, order_items_df
    
    def generate_all_data(self):
        """Generate complete dataset"""
        print("Generating customers...")
        customers = self.generate_customers()
        
        print("Generating products...")
        products = self.generate_products()
        
        print("Generating orders...")
        orders, order_items = self.generate_orders(customers, products)
        
        return {
            'customers': customers,
            'products': products,
            'orders': orders,
            'order_items': order_items
        }

# Generate and save data
if __name__ == "__main__":
    generator = EcommerceDataGenerator(num_customers=5000, num_products=500)
    data = generator.generate_all_data()
    
    # Save to CSV files
    for name, df in data.items():
        filepath = f"./data/raw/{name}.csv"
        df.to_csv(filepath, index=False)
        print(f"Saved {name}: {len(df)} rows to {filepath}")