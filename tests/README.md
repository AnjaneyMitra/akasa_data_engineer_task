# Test Suite - Akasa Data Engineering Pipeline

## Overview

This directory contains the streamlined **pytest-based test suite** for the Akasa Data Engineering Pipeline. The tests are designed to validate all core functionality while being interview-presentation ready, demonstrating best practices in data engineering testing.

## 🧪 Test Structure

### `test_pipeline.py` - Essential Test Suite
The main test file contains **9 comprehensive tests** organized into 5 test classes:

#### 1. **TestDataQuality** (1 test)
- ✅ DataFrame operations and data validation
- Tests basic pandas operations used throughout the pipeline

#### 2. **TestKPICalculators** (4 tests)
- ✅ **Repeat Customers** - Business logic validation for customer retention KPI
- ✅ **Monthly Trends** - Date aggregation and trend calculation testing  
- ✅ **Regional Revenue** - Geographic revenue analysis validation
- ✅ **Top Customers** - Customer ranking and segmentation testing

#### 3. **TestErrorHandling** (2 tests)
- ✅ Empty dataset graceful handling
- ✅ Data type consistency validation

#### 4. **TestPerformance** (1 test)
- ✅ Large dataset operations (1K customers, 5K orders)
- Performance benchmarking with time thresholds

#### 5. **TestIntegration** (1 test)
- ✅ End-to-end data flow validation
- Complete KPI calculation workflow testing

### `run_tests.py` - Test Runner
Professional test runner with:
- 🎯 Formatted output for interview presentations
- ⚙️ Environment setup and path configuration
- 📊 Comprehensive test result reporting

## 🚀 Quick Start Guide

### 1. Run All Tests (Recommended)
```bash
# Using the professional test runner
python tests/run_tests.py

# Direct pytest execution
python -m pytest tests/test_pipeline.py -v
```

### 2. Run Specific Test Categories
```bash
# Run only KPI calculator tests
python -m pytest tests/test_pipeline.py::TestKPICalculators -v

# Run only performance tests  
python -m pytest tests/test_pipeline.py::TestPerformance -v

# Run only error handling tests
python -m pytest tests/test_pipeline.py::TestErrorHandling -v
```

### 3. Run Individual Tests
```bash
# Test specific KPI calculator
python -m pytest tests/test_pipeline.py::TestKPICalculators::test_repeat_customers_business_logic -v

# Test data quality operations
python -m pytest tests/test_pipeline.py::TestDataQuality::test_data_frame_operations -v
```

### 4. Advanced Test Options
```bash
# Run with detailed output and stop on first failure
python -m pytest tests/test_pipeline.py -v -x --tb=long

# Run with coverage reporting (if coverage installed)
python -m pytest tests/test_pipeline.py --cov=src --cov-report=html

# Run tests matching a pattern
python -m pytest tests/test_pipeline.py -k "kpi" -v
```

## 📋 Test Configuration

### Pytest Configuration (`pytest.ini`)
- **Test Discovery**: Automatically finds `test_*.py` files
- **Markers**: Custom markers for test categorization
- **Output**: Verbose formatting with clean tracebacks
- **Warnings**: Filtered for cleaner output

### Environment Setup
The tests automatically configure:
- ✅ Python path for `src` imports
- ✅ Pandas/NumPy data structures
- ✅ Decimal precision for financial calculations
- ✅ DateTime handling for time-series data

## 🎯 Expected Results

### ✅ All Tests Passing (9/9)
```
tests/test_pipeline.py::TestDataQuality::test_data_frame_operations PASSED
tests/test_pipeline.py::TestKPICalculators::test_repeat_customers_business_logic PASSED  
tests/test_pipeline.py::TestKPICalculators::test_monthly_trends_calculation PASSED
tests/test_pipeline.py::TestKPICalculators::test_regional_revenue_accuracy PASSED
tests/test_pipeline.py::TestKPICalculators::test_top_customers_ranking PASSED
tests/test_pipeline.py::TestErrorHandling::test_empty_dataset_handling PASSED
tests/test_pipeline.py::TestErrorHandling::test_data_consistency PASSED
tests/test_pipeline.py::TestPerformance::test_large_dataset_operations PASSED
tests/test_pipeline.py::TestIntegration::test_pipeline_data_flow PASSED

=============================================== 9 passed in 0.41s ===============================================
```

### 📊 Test Validation Coverage
- **✅ Data Quality**: DataFrame operations, joins, aggregations
- **✅ Business Logic**: All 4 KPIs with realistic test data
- **✅ Error Handling**: Empty datasets, type validation, edge cases
- **✅ Performance**: 1K+ customer dataset processing under 5 seconds
- **✅ Integration**: Complete pipeline data flow validation

## 🔧 Troubleshooting

### Common Issues & Solutions

#### Import Errors
```bash
# If you see import errors, ensure you're running from project root:
cd /path/to/akasa_data_engineer
python tests/run_tests.py
```

#### Missing Dependencies
```bash
# Install required packages
pip install pytest pandas numpy
```

#### Path Issues
```bash
# The tests automatically add src/ to Python path
# If issues persist, verify project structure matches expected layout
```

#### KPI Calculator Tests Skipped
- This is expected behavior if KPI calculators aren't available
- Tests use `pytest.skip()` for graceful handling
- Check that all required data columns are present

## 🏗️ Test Architecture Design

### Key Design Principles
1. **Interview Ready**: Clean, professional output suitable for presentations
2. **Modular Design**: Each test class focuses on specific functionality
3. **Realistic Data**: Tests use business-relevant sample datasets
4. **Error Resilience**: Graceful handling of missing components
5. **Performance Aware**: Benchmarks ensure acceptable processing speeds

### Data Schema Compatibility
Tests are designed to work with the actual pipeline data schema:
- **Customers**: `customer_id`, `customer_name`, `mobile_number`, `region`, `registration_date`
- **Orders**: `order_id`, `customer_id`, `order_date_time`, `order_amount`

## 📈 Integration with CI/CD

The test suite is designed for easy integration with continuous integration:

```yaml
# Example GitHub Actions workflow
- name: Run Tests
  run: python tests/run_tests.py
  
- name: Test Coverage
  run: python -m pytest tests/ --cov=src --cov-report=xml
```

## 🎓 Learning & Interview Notes

This test suite demonstrates:
- **Pytest Best Practices**: Fixtures, markers, parametrization
- **Data Engineering Testing**: Realistic datasets, performance benchmarks
- **Production Readiness**: Error handling, type validation, edge cases
- **Clean Code**: Well-documented, maintainable test structure
- **Business Logic Validation**: KPI calculations with expected outcomes

---

**Note**: This test suite is streamlined for interview presentations while maintaining comprehensive coverage of all critical pipeline functionality.
