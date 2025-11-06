"""
Test script for data processing pipeline.
Tests CSV parsing, XML parsing, and data cleaning functionality.
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from src.data_processing.data_cleaner import DataCleaner


def test_data_processing():
    """Test the complete data processing pipeline."""
    print("=" * 60)
    print("TESTING DATA PROCESSING PIPELINE")
    print("=" * 60)
    
    # Initialize data cleaner
    cleaner = DataCleaner()
    
    # Define file paths
    customer_file = "data/raw/generated_customers.csv"
    orders_file = "data/raw/generated_orders.xml"
    
    print(f"\n1. Processing customer data from: {customer_file}")
    print("-" * 50)
    
    # Process customer data
    success, errors = cleaner.process_customer_data(customer_file)
    
    if success:
        print("✅ Customer data processing: SUCCESS")
        if errors:
            print(f"⚠️  Warnings: {len(errors)} issues found")
            for error in errors[:3]:  # Show first 3 errors
                print(f"   - {error}")
    else:
        print("❌ Customer data processing: FAILED")
        for error in errors:
            print(f"   - {error}")
        return
    
    print(f"\n2. Processing order data from: {orders_file}")
    print("-" * 50)
    
    # Process order data
    success, errors = cleaner.process_order_data(orders_file)
    
    if success:
        print("✅ Order data processing: SUCCESS")
        if errors:
            print(f"⚠️  Warnings: {len(errors)} issues found")
            for error in errors[:3]:  # Show first 3 errors
                print(f"   - {error}")
    else:
        print("❌ Order data processing: FAILED")
        for error in errors:
            print(f"   - {error}")
        return
    
    print(f"\n3. Validating data consistency")
    print("-" * 50)
    
    # Validate data consistency
    is_consistent, issues = cleaner.validate_data_consistency()
    
    if is_consistent:
        print("✅ Data consistency validation: PASSED")
    else:
        print("⚠️  Data consistency validation: ISSUES FOUND")
        for issue in issues:
            print(f"   - {issue}")
    
    print(f"\n4. Generating processing summary")
    print("-" * 50)
    
    # Generate summary
    summary = cleaner.generate_processing_summary()
    
    # Display key metrics
    print("📊 PROCESSING SUMMARY:")
    if 'customers' in summary:
        cust_data = summary['customers']
        print(f"   Customers: {cust_data.get('total_records', 0)} records")
        if 'data_quality' in cust_data:
            quality = cust_data['data_quality']
            print(f"   Customer regions: {quality.get('region_distribution', {})}")
    
    if 'orders' in summary:
        order_data = summary['orders']
        print(f"   Orders: {order_data.get('total_records', 0)} records")
        if 'data_quality' in order_data:
            quality = order_data['data_quality']
            if 'amount_stats' in quality:
                amt_stats = quality['amount_stats']
                print(f"   Total revenue: ₹{amt_stats.get('total_revenue', 0):,.2f}")
                print(f"   Avg order value: ₹{amt_stats.get('avg_amount', 0):,.2f}")
    
    if 'data_quality' in summary:
        dq = summary['data_quality']
        coverage = dq.get('customer_order_coverage', {})
        print(f"   Customer coverage: {coverage.get('coverage_percentage', 0):.1f}%")
    
    print(f"\n5. Creating enriched dataset")
    print("-" * 50)
    
    # Create enriched dataset
    enriched_df = cleaner.create_enriched_dataset()
    
    if enriched_df is not None:
        print(f"✅ Enriched dataset created: {len(enriched_df)} records")
        print(f"   Columns: {list(enriched_df.columns)}")
        
        # Show sample data
        print(f"\n📋 Sample enriched data (first 3 rows):")
        sample_cols = ['customer_id', 'customer_name', 'region', 'order_id', 'total_amount', 'sku_category']
        available_cols = [col for col in sample_cols if col in enriched_df.columns]
        print(enriched_df[available_cols].head(3).to_string(index=False))
    else:
        print("❌ Failed to create enriched dataset")
    
    print(f"\n6. Exporting cleaned data")
    print("-" * 50)
    
    # Export cleaned data
    exported_files = cleaner.export_cleaned_data()
    
    if exported_files:
        print("✅ Data export: SUCCESS")
        for data_type, file_path in exported_files.items():
            print(f"   {data_type.capitalize()}: {file_path}")
    else:
        print("❌ Data export: FAILED")
    
    print("\n" + "=" * 60)
    print("DATA PROCESSING PIPELINE TEST COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    test_data_processing()
