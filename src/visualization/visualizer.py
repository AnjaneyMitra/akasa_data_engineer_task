"""
Data Visualization Module for Akasa Data Engineering Pipeline.
Creates charts and exports data in CSV format for business analysis.
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
import numpy as np
from datetime import datetime
import os
import sys

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from common.logger import setup_logger

logger = setup_logger(__name__)

# Set style for professional visualizations
try:
    plt.style.use('seaborn-v0_8-whitegrid')
except:
    plt.style.use('default')
sns.set_palette("husl")


class DataVisualizer:
    """
    Creates visualizations and CSV exports for KPI data analysis.
    Generates business-ready charts and tabular data.
    """
    
    def __init__(self, output_dir: str = "data/outputs"):
        """
        Initialize the visualizer.
        
        Args:
            output_dir: Base directory for outputs
        """
        self.output_dir = Path(output_dir)
        self.charts_dir = self.output_dir / "charts"
        self.csv_dir = self.output_dir / "csv_exports"
        
        # Create directories
        self.charts_dir.mkdir(parents=True, exist_ok=True)
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Data visualizer initialized with output dir: {output_dir}")
    
    def create_kpi_visualizations(self, kpi_data: Dict[str, Any], pipeline_type: str = "memory") -> Dict[str, str]:
        """
        Create all KPI visualizations and CSV exports.
        
        Args:
            kpi_data: KPI results dictionary
            pipeline_type: Type of pipeline ('memory' or 'table')
            
        Returns:
            Dictionary with paths to created files
        """
        created_files = {}
        
        try:
            # Create visualizations for each KPI
            if 'repeat_customers' in kpi_data:
                files = self._visualize_repeat_customers(kpi_data['repeat_customers'], pipeline_type)
                created_files.update(files)
            
            if 'monthly_trends' in kpi_data:
                files = self._visualize_monthly_trends(kpi_data['monthly_trends'], pipeline_type)
                created_files.update(files)
            
            if 'regional_revenue' in kpi_data:
                files = self._visualize_regional_revenue(kpi_data['regional_revenue'], pipeline_type)
                created_files.update(files)
            
            if 'top_customers' in kpi_data:
                files = self._visualize_top_customers(kpi_data['top_customers'], pipeline_type)
                created_files.update(files)
            
            # Create summary dashboard
            dashboard_file = self._create_summary_dashboard(kpi_data, pipeline_type)
            if dashboard_file:
                created_files['dashboard'] = dashboard_file
            
            logger.info(f"Created {len(created_files)} visualization files for {pipeline_type} pipeline")
            
        except Exception as e:
            logger.error(f"Error creating visualizations: {str(e)}")
            
        return created_files
    
    def _visualize_repeat_customers(self, data: Dict[str, Any], pipeline_type: str) -> Dict[str, str]:
        """Create repeat customers visualizations and CSV export."""
        files = {}
        
        try:
            # Create CSV export
            repeat_customers_df = pd.DataFrame(data['repeat_customers'])
            csv_file = self.csv_dir / f"{pipeline_type}_repeat_customers.csv"
            repeat_customers_df.to_csv(csv_file, index=False)
            files['repeat_customers_csv'] = str(csv_file)
            
            # Create visualizations
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
            fig.suptitle(f'Repeat Customers Analysis - {pipeline_type.title()} Pipeline', fontsize=16, fontweight='bold')
            
            # 1. Order count distribution
            order_counts = repeat_customers_df['order_count']
            ax1.hist(order_counts, bins=max(3, len(order_counts.unique())), alpha=0.7, color='skyblue', edgecolor='black')
            ax1.set_title('Distribution of Order Counts')
            ax1.set_xlabel('Number of Orders')
            ax1.set_ylabel('Number of Customers')
            ax1.grid(True, alpha=0.3)
            
            # 2. Top spenders bar chart
            top_spenders = repeat_customers_df.nlargest(10, 'total_spent')
            bars = ax2.bar(range(len(top_spenders)), top_spenders['total_spent'], color='lightcoral')
            ax2.set_title('Top 10 Repeat Customers by Spending')
            ax2.set_xlabel('Customer Rank')
            ax2.set_ylabel('Total Spent (₹)')
            ax2.set_xticks(range(len(top_spenders)))
            ax2.set_xticklabels([f"{i+1}" for i in range(len(top_spenders))])
            ax2.grid(True, alpha=0.3)
            
            # Add value labels on bars
            for i, bar in enumerate(bars):
                height = bar.get_height()
                ax2.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                        f'₹{height:,.0f}', ha='center', va='bottom', fontsize=8)
            
            # 3. Regional distribution
            regional_counts = repeat_customers_df['region'].value_counts()
            wedges, texts, autotexts = ax3.pie(regional_counts.values, labels=regional_counts.index, 
                                             autopct='%1.1f%%', startangle=90, colors=sns.color_palette("Set3"))
            ax3.set_title('Repeat Customers by Region')
            
            # 4. Spending vs Order Count scatter
            scatter = ax4.scatter(repeat_customers_df['order_count'], repeat_customers_df['total_spent'], 
                                alpha=0.6, s=60, c=repeat_customers_df.index, cmap='viridis')
            ax4.set_title('Spending vs Order Count Relationship')
            ax4.set_xlabel('Number of Orders')
            ax4.set_ylabel('Total Spent (₹)')
            ax4.grid(True, alpha=0.3)
            
            # Add trendline
            z = np.polyfit(repeat_customers_df['order_count'], repeat_customers_df['total_spent'], 1)
            p = np.poly1d(z)
            ax4.plot(repeat_customers_df['order_count'], p(repeat_customers_df['order_count']), "r--", alpha=0.8)
            
            plt.tight_layout()
            chart_file = self.charts_dir / f"{pipeline_type}_repeat_customers_analysis.png"
            plt.savefig(chart_file, dpi=300, bbox_inches='tight')
            plt.close()
            files['repeat_customers_chart'] = str(chart_file)
            
            logger.info(f"Created repeat customers visualization: {chart_file}")
            
        except Exception as e:
            logger.error(f"Error creating repeat customers visualization: {str(e)}")
            
        return files
    
    def _visualize_monthly_trends(self, data: Dict[str, Any], pipeline_type: str) -> Dict[str, str]:
        """Create monthly trends visualizations and CSV export."""
        files = {}
        
        try:
            # Create CSV export
            trends_df = pd.DataFrame(data['monthly_trends'])
            csv_file = self.csv_dir / f"{pipeline_type}_monthly_trends.csv"
            trends_df.to_csv(csv_file, index=False)
            files['monthly_trends_csv'] = str(csv_file)
            
            # Create visualization
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 12))
            fig.suptitle(f'Monthly Trends Analysis - {pipeline_type.title()} Pipeline', fontsize=16, fontweight='bold')
            
            # Convert month to datetime for proper sorting
            trends_df['month_date'] = pd.to_datetime(trends_df['month'] + '-01')
            trends_df = trends_df.sort_values('month_date')
            
            months = trends_df['month'].tolist()
            orders = trends_df['order_count'].tolist()
            revenue = trends_df['total_revenue'].tolist()
            
            # 1. Order count trend
            ax1.plot(months, orders, marker='o', linewidth=2, markersize=8, color='blue')
            ax1.set_title('Monthly Order Count Trend')
            ax1.set_xlabel('Month')
            ax1.set_ylabel('Number of Orders')
            ax1.grid(True, alpha=0.3)
            ax1.tick_params(axis='x', rotation=45)
            
            # Add value labels
            for i, v in enumerate(orders):
                ax1.annotate(f'{v}', (i, v), textcoords="offset points", xytext=(0,10), ha='center')
            
            # 2. Revenue trend
            ax2.plot(months, revenue, marker='s', linewidth=2, markersize=8, color='green')
            ax2.set_title('Monthly Revenue Trend')
            ax2.set_xlabel('Month')
            ax2.set_ylabel('Total Revenue (₹)')
            ax2.grid(True, alpha=0.3)
            ax2.tick_params(axis='x', rotation=45)
            
            # Format revenue values
            for i, v in enumerate(revenue):
                ax2.annotate(f'₹{v:,.0f}', (i, v), textcoords="offset points", xytext=(0,10), ha='center', fontsize=9)
            
            # 3. Combined view with dual y-axis
            ax3_twin = ax3.twinx()
            
            line1 = ax3.plot(months, orders, marker='o', linewidth=2, markersize=6, color='blue', label='Orders')
            line2 = ax3_twin.plot(months, revenue, marker='s', linewidth=2, markersize=6, color='green', label='Revenue')
            
            ax3.set_title('Orders vs Revenue Trend')
            ax3.set_xlabel('Month')
            ax3.set_ylabel('Number of Orders', color='blue')
            ax3_twin.set_ylabel('Total Revenue (₹)', color='green')
            ax3.tick_params(axis='x', rotation=45)
            ax3.grid(True, alpha=0.3)
            
            # Combined legend
            lines = line1 + line2
            labels = [l.get_label() for l in lines]
            ax3.legend(lines, labels, loc='upper left')
            
            plt.tight_layout()
            chart_file = self.charts_dir / f"{pipeline_type}_monthly_trends_analysis.png"
            plt.savefig(chart_file, dpi=300, bbox_inches='tight')
            plt.close()
            files['monthly_trends_chart'] = str(chart_file)
            
            logger.info(f"Created monthly trends visualization: {chart_file}")
            
        except Exception as e:
            logger.error(f"Error creating monthly trends visualization: {str(e)}")
            
        return files
    
    def _visualize_regional_revenue(self, data: Dict[str, Any], pipeline_type: str) -> Dict[str, str]:
        """Create regional revenue visualizations and CSV export."""
        files = {}
        
        try:
            # Create CSV export
            regional_df = pd.DataFrame(data['regional_revenue'])
            csv_file = self.csv_dir / f"{pipeline_type}_regional_revenue.csv"
            regional_df.to_csv(csv_file, index=False)
            files['regional_revenue_csv'] = str(csv_file)
            
            # Create visualization
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
            fig.suptitle(f'Regional Revenue Analysis - {pipeline_type.title()} Pipeline', fontsize=16, fontweight='bold')
            
            regions = regional_df['region'].tolist()
            revenues = regional_df['total_revenue'].tolist()
            percentages = regional_df['revenue_share_pct'].tolist()
            # Handle different data structures - some may not have order_count
            if 'order_count' in regional_df.columns:
                order_counts = regional_df['order_count'].tolist()
            else:
                order_counts = [1] * len(regions)  # Default fallback
            
            # 1. Revenue by region (bar chart)
            bars = ax1.bar(regions, revenues, color=sns.color_palette("Set2", len(regions)))
            ax1.set_title('Total Revenue by Region')
            ax1.set_xlabel('Region')
            ax1.set_ylabel('Total Revenue (₹)')
            ax1.tick_params(axis='x', rotation=45)
            ax1.grid(True, alpha=0.3)
            
            # Add value labels on bars
            for i, bar in enumerate(bars):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                        f'₹{height:,.0f}', ha='center', va='bottom', fontsize=9)
            
            # 2. Revenue share (pie chart)
            wedges, texts, autotexts = ax2.pie(percentages, labels=regions, autopct='%1.1f%%', 
                                             startangle=90, colors=sns.color_palette("Set2"))
            ax2.set_title('Revenue Share by Region')
            
            # 3. Order count by region
            bars2 = ax3.bar(regions, order_counts, color=sns.color_palette("Set1", len(regions)))
            ax3.set_title('Order Count by Region')
            ax3.set_xlabel('Region')
            ax3.set_ylabel('Number of Orders')
            ax3.tick_params(axis='x', rotation=45)
            ax3.grid(True, alpha=0.3)
            
            # Add value labels
            for i, bar in enumerate(bars2):
                height = bar.get_height()
                ax3.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                        f'{int(height)}', ha='center', va='bottom')
            
            # 4. Revenue per order by region
            revenue_per_order = [r/o for r, o in zip(revenues, order_counts)]
            bars3 = ax4.bar(regions, revenue_per_order, color=sns.color_palette("viridis", len(regions)))
            ax4.set_title('Average Revenue per Order by Region')
            ax4.set_xlabel('Region')
            ax4.set_ylabel('Avg Revenue per Order (₹)')
            ax4.tick_params(axis='x', rotation=45)
            ax4.grid(True, alpha=0.3)
            
            # Add value labels
            for i, bar in enumerate(bars3):
                height = bar.get_height()
                ax4.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                        f'₹{height:,.0f}', ha='center', va='bottom', fontsize=9)
            
            plt.tight_layout()
            chart_file = self.charts_dir / f"{pipeline_type}_regional_revenue_analysis.png"
            plt.savefig(chart_file, dpi=300, bbox_inches='tight')
            plt.close()
            files['regional_revenue_chart'] = str(chart_file)
            
            logger.info(f"Created regional revenue visualization: {chart_file}")
            
        except Exception as e:
            logger.error(f"Error creating regional revenue visualization: {str(e)}")
            
        return files
    
    def _visualize_top_customers(self, data: Dict[str, Any], pipeline_type: str) -> Dict[str, str]:
        """Create top customers visualizations and CSV export."""
        files = {}
        
        try:
            # Create CSV export
            top_customers_df = pd.DataFrame(data['top_customers'])
            csv_file = self.csv_dir / f"{pipeline_type}_top_customers.csv"
            top_customers_df.to_csv(csv_file, index=False)
            files['top_customers_csv'] = str(csv_file)
            
            # Create visualization
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
            fig.suptitle(f'Top Customers Analysis - {pipeline_type.title()} Pipeline', fontsize=16, fontweight='bold')
            
            # Limit to top 10 for better visibility
            top_10 = top_customers_df.head(10)
            
            customer_names = [name[:15] + '...' if len(name) > 15 else name for name in top_10['customer_name']]
            # Handle different field names between pipelines
            if 'total_spent_in_period' in top_10.columns:
                spent_amounts = top_10['total_spent_in_period'].tolist()
                order_counts = top_10['orders_in_period'].tolist()
            else:
                spent_amounts = top_10['total_spent'].tolist()
                order_counts = top_10['total_orders'].tolist()
            regions = top_10['region'].tolist()
            
            # 1. Top customers by spending (horizontal bar chart)
            bars = ax1.barh(range(len(customer_names)), spent_amounts, color=sns.color_palette("viridis", len(customer_names)))
            ax1.set_title('Top 10 Customers by Spending (Last 30 Days)')
            ax1.set_xlabel('Total Spent (₹)')
            ax1.set_ylabel('Customer')
            ax1.set_yticks(range(len(customer_names)))
            ax1.set_yticklabels(customer_names)
            ax1.grid(True, alpha=0.3, axis='x')
            
            # Add value labels
            for i, bar in enumerate(bars):
                width = bar.get_width()
                ax1.text(width + width*0.01, bar.get_y() + bar.get_height()/2.,
                        f'₹{width:,.0f}', ha='left', va='center', fontsize=8)
            
            # 2. Top customers by order count
            bars2 = ax2.bar(range(len(customer_names)), order_counts, color='lightcoral')
            ax2.set_title('Top 10 Customers by Order Count')
            ax2.set_xlabel('Customer')
            ax2.set_ylabel('Number of Orders')
            ax2.set_xticks(range(len(customer_names)))
            ax2.set_xticklabels([f"C{i+1}" for i in range(len(customer_names))], rotation=45)
            ax2.grid(True, alpha=0.3)
            
            # Add value labels
            for i, bar in enumerate(bars2):
                height = bar.get_height()
                ax2.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                        f'{int(height)}', ha='center', va='bottom')
            
            # 3. Regional distribution of top customers
            region_counts = pd.Series(regions).value_counts()
            wedges, texts, autotexts = ax3.pie(region_counts.values, labels=region_counts.index, 
                                             autopct='%1.1f%%', startangle=90, colors=sns.color_palette("Set3"))
            ax3.set_title('Top Customers Distribution by Region')
            
            # 4. Spending efficiency (spent per order)
            efficiency = [s/o for s, o in zip(spent_amounts, order_counts)]
            bars4 = ax4.bar(range(len(customer_names)), efficiency, color='gold')
            ax4.set_title('Average Spending per Order (Top 10)')
            ax4.set_xlabel('Customer')
            ax4.set_ylabel('Avg Spending per Order (₹)')
            ax4.set_xticks(range(len(customer_names)))
            ax4.set_xticklabels([f"C{i+1}" for i in range(len(customer_names))], rotation=45)
            ax4.grid(True, alpha=0.3)
            
            # Add value labels
            for i, bar in enumerate(bars4):
                height = bar.get_height()
                ax4.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                        f'₹{height:,.0f}', ha='center', va='bottom', fontsize=8)
            
            plt.tight_layout()
            chart_file = self.charts_dir / f"{pipeline_type}_top_customers_analysis.png"
            plt.savefig(chart_file, dpi=300, bbox_inches='tight')
            plt.close()
            files['top_customers_chart'] = str(chart_file)
            
            logger.info(f"Created top customers visualization: {chart_file}")
            
        except Exception as e:
            logger.error(f"Error creating top customers visualization: {str(e)}")
            
        return files
    
    def _create_summary_dashboard(self, kpi_data: Dict[str, Any], pipeline_type: str) -> Optional[str]:
        """Create a summary dashboard with key metrics."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
            fig.suptitle(f'KPI Summary Dashboard - {pipeline_type.title()} Pipeline', fontsize=20, fontweight='bold')
            
            # 1. Key metrics summary (text display)
            ax1.axis('off')
            
            # Extract key metrics
            total_customers = len(kpi_data.get('repeat_customers', {}).get('repeat_customers', []))
            total_revenue = sum([c['total_spent'] for c in kpi_data.get('repeat_customers', {}).get('repeat_customers', [])])
            
            if 'regional_revenue' in kpi_data:
                regions_count = len(kpi_data['regional_revenue']['regional_revenue'])
                top_region = max(kpi_data['regional_revenue']['regional_revenue'], 
                               key=lambda x: x['total_revenue'])['region']
            else:
                regions_count = 0
                top_region = "N/A"
            
            if 'monthly_trends' in kpi_data:
                months_analyzed = len(kpi_data['monthly_trends']['monthly_trends'])
            else:
                months_analyzed = 0
            
            # Display key metrics as text
            metrics_text = f"""
            KEY BUSINESS METRICS
            
            📊 Total Repeat Customers: {total_customers}
            💰 Total Revenue: ₹{total_revenue:,.0f}
            🌍 Regions Analyzed: {regions_count}
            🏆 Top Performing Region: {top_region}
            📅 Months Analyzed: {months_analyzed}
            
            Pipeline Type: {pipeline_type.upper()}
            Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """
            
            ax1.text(0.1, 0.5, metrics_text, fontsize=14, verticalalignment='center',
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.7))
            ax1.set_title('Key Metrics Overview', fontsize=16, fontweight='bold')
            
            # 2. Regional revenue pie chart
            if 'regional_revenue' in kpi_data:
                regional_data = kpi_data['regional_revenue']['regional_revenue']
                regions = [r['region'] for r in regional_data]
                revenues = [r['total_revenue'] for r in regional_data]
                
                wedges, texts, autotexts = ax2.pie(revenues, labels=regions, autopct='%1.1f%%', 
                                                 startangle=90, colors=sns.color_palette("Set3"))
                ax2.set_title('Revenue Distribution by Region', fontsize=14, fontweight='bold')
            
            # 3. Monthly trends line chart
            if 'monthly_trends' in kpi_data:
                trends_data = kpi_data['monthly_trends']['monthly_trends']
                months = [t['month'] for t in trends_data]
                revenues = [t['total_revenue'] for t in trends_data]
                
                ax3.plot(months, revenues, marker='o', linewidth=3, markersize=8, color='green')
                ax3.set_title('Monthly Revenue Trend', fontsize=14, fontweight='bold')
                ax3.set_xlabel('Month')
                ax3.set_ylabel('Total Revenue (₹)')
                ax3.tick_params(axis='x', rotation=45)
                ax3.grid(True, alpha=0.3)
                
                # Format revenue values
                for i, v in enumerate(revenues):
                    ax3.annotate(f'₹{v:,.0f}', (i, v), textcoords="offset points", 
                               xytext=(0,10), ha='center', fontsize=10)
            
            # 4. Top customers bar chart
            if 'top_customers' in kpi_data:
                top_data = kpi_data['top_customers']['top_customers'][:5]  # Top 5
                names = [c['customer_name'][:10] + '...' if len(c['customer_name']) > 10 else c['customer_name'] for c in top_data]
                # Handle different field names
                if 'total_spent_in_period' in top_data[0]:
                    spent = [c['total_spent_in_period'] for c in top_data]
                else:
                    spent = [c['total_spent'] for c in top_data]
                
                bars = ax4.bar(range(len(names)), spent, color=sns.color_palette("viridis", len(names)))
                ax4.set_title('Top 5 Customers (Last 30 Days)', fontsize=14, fontweight='bold')
                ax4.set_xlabel('Customer')
                ax4.set_ylabel('Total Spent (₹)')
                ax4.set_xticks(range(len(names)))
                ax4.set_xticklabels(names, rotation=45)
                ax4.grid(True, alpha=0.3)
                
                # Add value labels
                for i, bar in enumerate(bars):
                    height = bar.get_height()
                    ax4.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                            f'₹{height:,.0f}', ha='center', va='bottom', fontsize=10)
            
            plt.tight_layout()
            dashboard_file = self.charts_dir / f"{pipeline_type}_kpi_dashboard.png"
            plt.savefig(dashboard_file, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Created KPI dashboard: {dashboard_file}")
            return str(dashboard_file)
            
        except Exception as e:
            logger.error(f"Error creating summary dashboard: {str(e)}")
            return None
    
    def export_all_to_csv(self, pipeline_type: str, kpi_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Export all KPI data to CSV files for easy analysis.
        
        Args:
            pipeline_type: Type of pipeline
            kpi_data: KPI results dictionary
            
        Returns:
            Dictionary with paths to CSV files
        """
        csv_files = {}
        
        try:
            for kpi_name, kpi_result in kpi_data.items():
                if kpi_name == 'pipeline_info':
                    continue
                    
                csv_file = self.csv_dir / f"{pipeline_type}_{kpi_name}.csv"
                
                if kpi_name == 'repeat_customers' and 'repeat_customers' in kpi_result:
                    df = pd.DataFrame(kpi_result['repeat_customers'])
                elif kpi_name == 'monthly_trends' and 'monthly_trends' in kpi_result:
                    df = pd.DataFrame(kpi_result['monthly_trends'])
                elif kpi_name == 'regional_revenue' and 'regional_revenue' in kpi_result:
                    df = pd.DataFrame(kpi_result['regional_revenue'])
                elif kpi_name == 'top_customers' and 'top_customers' in kpi_result:
                    df = pd.DataFrame(kpi_result['top_customers'])
                else:
                    # Handle other formats
                    if isinstance(kpi_result, dict):
                        df = pd.DataFrame([kpi_result])
                    elif isinstance(kpi_result, list):
                        df = pd.DataFrame(kpi_result)
                    else:
                        continue
                
                df.to_csv(csv_file, index=False)
                csv_files[f"{kpi_name}_csv"] = str(csv_file)
                logger.info(f"Exported {kpi_name} to CSV: {csv_file}")
        
        except Exception as e:
            logger.error(f"Error exporting CSV files: {str(e)}")
        
        return csv_files


def create_pipeline_visualizations(pipeline_type: str, kpi_data: Dict[str, Any], 
                                 output_dir: str = "data/outputs") -> Dict[str, str]:
    """
    Convenience function to create all visualizations for a pipeline.
    
    Args:
        pipeline_type: Type of pipeline ('memory' or 'table')
        kpi_data: KPI results dictionary
        output_dir: Base output directory
        
    Returns:
        Dictionary with paths to created files
    """
    visualizer = DataVisualizer(output_dir)
    
    # Create visualizations and CSV exports
    viz_files = visualizer.create_kpi_visualizations(kpi_data, pipeline_type)
    csv_files = visualizer.export_all_to_csv(pipeline_type, kpi_data)
    
    # Combine all files
    all_files = {**viz_files, **csv_files}
    
    logger.info(f"Created {len(all_files)} visualization and export files for {pipeline_type} pipeline")
    
    return all_files


if __name__ == "__main__":
    # Example usage
    print("Data Visualizer module - run from pipeline scripts")
