"""
Data Generator for Customer and Order Data
Generates realistic test data based on existing patterns for comprehensive analysis.
"""

import csv
import random
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import sys
from pathlib import Path


class DataGenerator:
    """Generate realistic customer and order data for testing."""
    
    def __init__(self):
        # Indian regions for better coverage
        self.regions = ['North', 'South', 'East', 'West', 'Central', 'Northeast']
        
        # Common Indian names for realistic data
        self.first_names = [
            'Aarav', 'Neha', 'Rohan', 'Priya', 'Kabir', 'Ananya', 'Arjun', 'Isha',
            'Vikram', 'Kavya', 'Aditya', 'Shreya', 'Rahul', 'Pooja', 'Siddharth',
            'Riya', 'Karan', 'Meera', 'Varun', 'Tanya'
        ]
        
        self.last_names = [
            'Sharma', 'Gupta', 'Singh', 'Iyer', 'Mehta', 'Agarwal', 'Jain', 'Reddy',
            'Kumar', 'Verma', 'Patel', 'Shah', 'Bansal', 'Malhotra', 'Chopra',
            'Nair', 'Rao', 'Joshi', 'Thakur', 'Bhat'
        ]
        
        # SKU patterns based on existing data
        self.sku_categories = {
            'Electronics': ['SKU-1001', 'SKU-1002', 'SKU-1003', 'SKU-1004', 'SKU-1005'],
            'Clothing': ['SKU-2001', 'SKU-2002', 'SKU-2003', 'SKU-2004', 'SKU-2005'],
            'Books': ['SKU-3001', 'SKU-3002', 'SKU-3003', 'SKU-3004', 'SKU-3005'],
            'Home': ['SKU-4001', 'SKU-4002', 'SKU-4003', 'SKU-4004', 'SKU-4005']
        }
        
        # Price ranges by category (in INR)
        self.price_ranges = {
            'Electronics': (5000, 50000),
            'Clothing': (500, 5000), 
            'Books': (200, 1500),
            'Home': (1000, 15000)
        }
        
        # Mobile number prefixes for different regions
        self.mobile_prefixes = ['91', '92', '93', '94', '95', '96', '97', '98', '99']
    
    def generate_mobile_number(self) -> str:
        """Generate a realistic Indian mobile number."""
        prefix = random.choice(self.mobile_prefixes)
        suffix = ''.join([str(random.randint(0, 9)) for _ in range(8)])
        return f"{prefix}{suffix}"
    
    def generate_customers(self, count: int = 20) -> List[Dict]:
        """Generate customer data with good regional distribution."""
        customers = []
        
        # Ensure at least 2-3 customers per region
        customers_per_region = count // len(self.regions)
        remaining = count % len(self.regions)
        
        customer_id = 1
        
        for region in self.regions:
            region_customers = customers_per_region
            if remaining > 0:
                region_customers += 1
                remaining -= 1
            
            for _ in range(region_customers):
                customer = {
                    'customer_id': f"CUST-{customer_id:03d}",
                    'customer_name': f"{random.choice(self.first_names)} {random.choice(self.last_names)}",
                    'mobile_number': self.generate_mobile_number(),
                    'region': region
                }
                customers.append(customer)
                customer_id += 1
        
        # Sort by customer_id for consistent ordering
        customers.sort(key=lambda x: x['customer_id'])
        return customers[:count]
    
    def generate_orders_for_customers(
        self, 
        customers: List[Dict], 
        orders_per_customer_range: Tuple[int, int] = (1, 5),
        date_range_days: int = 90
    ) -> List[Dict]:
        """Generate orders for customers with realistic patterns."""
        orders = []
        order_id = 1
        
        # Create date range for orders (last N days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=date_range_days)
        
        for customer in customers:
            # Some customers are repeat customers, others are not
            num_orders = random.randint(*orders_per_customer_range)
            
            # Generate orders for this customer
            customer_orders = []
            for _ in range(num_orders):
                # Random date within range
                days_offset = random.randint(0, date_range_days)
                order_date = start_date + timedelta(days=days_offset)
                
                # Add some time variation
                hours = random.randint(0, 23)
                minutes = random.randint(0, 59)
                seconds = random.randint(0, 59)
                order_datetime = order_date.replace(hour=hours, minute=minutes, second=seconds)
                
                # Select category and SKU
                category = random.choice(list(self.sku_categories.keys()))
                sku_id = random.choice(self.sku_categories[category])
                
                # Generate quantity and price
                sku_count = random.randint(1, 5)
                price_min, price_max = self.price_ranges[category]
                unit_price = random.randint(price_min, price_max)
                total_amount = unit_price * sku_count
                
                order = {
                    'order_id': f"ORD-2025-{order_id:04d}",
                    'mobile_number': customer['mobile_number'],
                    'order_date_time': order_datetime.strftime('%Y-%m-%dT%H:%M:%S'),
                    'sku_id': sku_id,
                    'sku_count': sku_count,
                    'total_amount': total_amount
                }
                customer_orders.append(order)
                order_id += 1
            
            # Sort customer orders by date
            customer_orders.sort(key=lambda x: x['order_date_time'])
            orders.extend(customer_orders)
        
        # Sort all orders by order_id for consistent output
        orders.sort(key=lambda x: x['order_id'])
        return orders
    
    def save_customers_csv(self, customers: List[Dict], filepath: str):
        """Save customers to CSV file."""
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['customer_id', 'customer_name', 'mobile_number', 'region']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(customers)
        
        print(f"Saved {len(customers)} customers to {filepath}")
    
    def save_orders_xml(self, orders: List[Dict], filepath: str):
        """Save orders to XML file."""
        root = ET.Element('orders')
        
        # Sort orders by order_id for consistent XML output
        sorted_orders = sorted(orders, key=lambda x: x['order_id'])
        
        for order in sorted_orders:
            order_elem = ET.SubElement(root, 'order')
            
            # Add fields in consistent order
            field_order = ['order_id', 'mobile_number', 'order_date_time', 'sku_id', 'sku_count', 'total_amount']
            for key in field_order:
                if key in order:
                    elem = ET.SubElement(order_elem, key)
                    elem.text = str(order[key])
        
        # Create tree and save
        tree = ET.ElementTree(root)
        ET.indent(tree, space="  ")
        tree.write(filepath, encoding='utf-8', xml_declaration=True)
        
        print(f"Saved {len(orders)} orders to {filepath}")
    
    def generate_analysis_summary(self, customers: List[Dict], orders: List[Dict]) -> Dict:
        """Generate summary statistics for the generated data."""
        # Customer analysis
        region_counts = {}
        for customer in customers:
            region = customer['region']
            region_counts[region] = region_counts.get(region, 0) + 1
        
        # Order analysis
        customer_order_counts = {}
        total_revenue = 0
        
        for order in orders:
            mobile = order['mobile_number']
            customer_order_counts[mobile] = customer_order_counts.get(mobile, 0) + 1
            total_revenue += order['total_amount']
        
        repeat_customers = sum(1 for count in customer_order_counts.values() if count > 1)
        
        return {
            'total_customers': len(customers),
            'total_orders': len(orders),
            'region_distribution': region_counts,
            'repeat_customers': repeat_customers,
            'single_order_customers': len(customers) - repeat_customers,
            'total_revenue': total_revenue,
            'avg_revenue_per_customer': total_revenue / len(customers),
            'avg_orders_per_customer': len(orders) / len(customers)
        }


def main():
    """Main function to generate test data."""
    print("Starting data generation process...")
    
    # Initialize generator
    generator = DataGenerator()
    
    # Set random seed for reproducible results
    random.seed(42)
    
    # Generate data
    customers = generator.generate_customers(count=20)
    orders = generator.generate_orders_for_customers(
        customers, 
        orders_per_customer_range=(1, 6),  # Some customers with more orders
        date_range_days=120  # Last 4 months
    )
    
    # Create data directory if it doesn't exist
    data_dir = Path('data/raw')
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Save generated data
    customer_file = data_dir / 'generated_customers.csv'
    orders_file = data_dir / 'generated_orders.xml'
    
    generator.save_customers_csv(customers, str(customer_file))
    generator.save_orders_xml(orders, str(orders_file))
    
    # Generate and display summary
    summary = generator.generate_analysis_summary(customers, orders)
    
    print("Data Generation Summary:")
    print(f"  Total Customers: {summary['total_customers']}")
    print(f"  Total Orders: {summary['total_orders']}")
    print(f"  Repeat Customers: {summary['repeat_customers']}")
    print(f"  Region Distribution: {summary['region_distribution']}")
    print(f"  Total Revenue: ₹{summary['total_revenue']:,}")
    print(f"  Avg Revenue/Customer: ₹{summary['avg_revenue_per_customer']:,.2f}")
    print(f"  Avg Orders/Customer: {summary['avg_orders_per_customer']:.1f}")
    
    print("Data generation completed successfully!")


if __name__ == "__main__":
    main()
