# Akasa Data Engineering Pipeline

Data engineering pipeline for customer analytics with dual architecture approaches (Table-based + In-Memory).

## Design

### Architecture
```
RAW DATA → DATA CLEANING → DUAL PIPELINE → KPI CALCULATION → RESULTS
(CSV/XML)   (Validation)    (SQL/Pandas)     (4 KPIs)      (JSON/CSV)
```

### Dual Pipeline Approach
- **Table-Based**: MySQL + SQLAlchemy ORM for production scalability
- **In-Memory**: Pandas operations for fast analytics and prototyping

### KPIs Implemented
1. **Repeat Customers**: Customer retention analysis
2. **Monthly Trends**: Revenue growth patterns  
3. **Regional Revenue**: Geographic performance
4. **Top Customers**: High-value customer segmentation

## Implementation

### Technology Stack
- **Python 3.13+** with pandas, SQLAlchemy
- **MySQL** for persistence, **CSV/XML** parsing
- **Environment-based** configuration (.env)
- **Security**: Parameterized queries, no hardcoded credentials

### Key Components
```
src/
├── data_processing/     # CSV/XML parsing with validation
├── kpi_calculators/     # Business logic (4 KPIs)
├── pipeline/           # Memory & Table orchestrators
├── database/          # SQLAlchemy models & operations
└── visualization/     # Charts & CSV exports
```

### Security Features
- Environment-based credentials (.env file)
- SQLAlchemy ORM (prevents SQL injection)
- Data masking in logs
- Input validation and error handling

## How to Run

### Setup
```bash
git clone https://github.com/AnjaneyMitra/akasa_data_engineer_task
cd akasa_data_engineer
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python scripts/run_memory_pipeline.py
```

### Execution Options

**1. In-Memory Pipeline**
```bash
python scripts/run_memory_pipeline.py
```
- Uses pandas for rapid analytics
- Best for development and prototyping

**2. Table-Based Pipeline**
```bash
python scripts/setup_database.py    # First time only
python scripts/run_table_pipeline.py
```
- Uses MySQL with SQLAlchemy ORM
- Best for production and large datasets

**3. Run Tests**
```bash
python tests/run_tests.py
# 14 tests
```

### Configuration
Create `.env` file:
```bash
DB_HOST=localhost
DB_PORT=3306  
DB_USERNAME=your_username
DB_PASSWORD=your_password
DB_DATABASE=akasa_pipeline
```

## Results & Outputs

### Generated Files
```
data/
├── processed/           # Input datasets
├── outputs/
│   ├── memory_pipeline/ # JSON results
│   └── table_pipeline/  # JSON results  
├── charts/             # Universal PNG visualizations
└── csv_exports/        # Pipeline-specific CSV files
```

### Key Metrics
- **Repeat Customers**: 16/20 (80% retention rate)
- **Monthly Trends**: Peak revenue ₹7.48L (Aug 2025), avg growth 33.8%/month
- **Regional Revenue**: East leads ₹7.35L (29.3%), South ₹5.59L (22.3%)
- **Top Customers**: Neha Sharma ₹1.04L, avg customer value ₹75.5K

**Automated visualizations and CSV exports included for stakeholder analysis.**