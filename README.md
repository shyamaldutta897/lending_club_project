# Lending Club Automated Loan Grading System

A comprehensive data engineering pipeline that automatically evaluates and grades loan applications based on member creditworthiness, repayment history, and financial health indicators. This system transforms raw loan data into risk-scored records with grades ranging from A (excellent) to F (unacceptable), supporting data-driven lending decisions.

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture & Components](#architecture--components)
- [System Flow](#system-flow)
- [Directory Structure](#directory-structure)
- [Setup & Installation](#setup--installation)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Data Formats & Schemas](#data-formats--schemas)
- [Scoring Methodology](#scoring-methodology)

## Project Overview

### What This System Does

The Lending Club Grading System automates the process of evaluating loan risk by:

1. **Ingesting** data from multiple sources (member profiles, loan details, repayment history, credit indicators)
2. **Validating** data quality to identify and quarantine problematic records
3. **Transforming** raw data into clean, standardized formats
4. **Scoring** each loan across three critical dimensions:
   - Payment History (how reliably has the borrower paid?)
   - Default Risk History (what's their credit risk profile?)
   - Financial Health (what's their current financial position?)
5. **Grading** loans from A-F based on a composite risk score
6. **Outputting** both clean datasets and graded results for downstream use

### Who Needs This

- **Loan Officers**: Quick risk assessment for new applications
- **Data Analysts**: Comprehensive loan dataset with risk metrics
- **Risk Management**: Portfolio analysis and exposure management
- **Compliance**: Data quality and audit trails for regulatory requirements

## Architecture & Components

### Module Overview

The system is organized into logical modules, each handling specific responsibilities:

```
lending_club_project/
├── framework/              # Core data processing framework
├── pipelines/              # Business logic and transformation pipelines
├── schemas/                # Data structure definitions
├── configs/                # Configuration management
└── tests/                  # Test suites
```

#### 1. **Framework Module** (`framework/`)

The foundational layer providing reusable utilities:

##### `framework/session/` - Spark Session Management
- **spark_session.py**: Creates and configures Spark sessions
  - Supports LOCAL (2-core local) and PROD (Hive-enabled) environments
  - Injects custom scoring configuration into Spark runtime
  - Handles Hadoop setup for Windows compatibility

##### `framework/config/` - Configuration Management
- **config_reader.py**: Dynamic configuration loader
  - Reads environment-specific settings from `configs/application.conf`
  - Loads PySpark configurations from `configs/pyspark.conf`
  - Validates environment names and required parameters
  - Returns configuration as Python dictionaries for easy access

##### `framework/readers/` - Data Input Handling
- **data_reader.py**: Specialized readers for each dataset type
  - `read_members_data()`: Loads member profile CSV files
  - `read_loans_data()`: Loads loan detail CSV files
  - `read_loan_repayments_data()`: Loads payment history CSV files
  - `read_loan_defaulters_data()`: Loads credit risk indicator CSV files
  - Each uses appropriate schema validation on read

- **data_reader_generic.py**: Generic reader for flexible format/path combinations
  - Supports CSV, Parquet, and other Spark-compatible formats
  - Accepts custom read options and schemas
  - Used primarily in curated pipeline and testing

- **read_options.py**: Format-specific read configurations
  - CSV options (headers, delimiters)
  - Parquet optimization settings
  - Format-agnostic reusable patterns

##### `framework/writers/` - Data Output Handling
- **data_writer.py**: Unified data writer for all output scenarios
  - Supports CSV and Parquet formats
  - Implements various write modes (overwrite, append, fail)
  - Optional partitioning by column for large datasets
  - Logs output locations for tracking

##### `framework/dq_checks/` - Data Quality Validation
- **dq_logics.py**: 10+ individual validation functions
  - Type checks (float, int, string date formats)
  - Field length validation (zip code = 5 chars, grade = 1 char, etc.)
  - Referential integrity (no duplicate member IDs)
  - Allowed values checking against predefined lists
  - Complex calculation checks for derived fields
  - Null/missing value detection

- **dq_orchestrator.py**: Orchestrates quality checks
  - Loads rules from `configs/dq_rules.json`
  - Applies all relevant rules to a dataset
  - Separates valid records from invalid ones
  - Generates detailed failure reports with rule IDs and descriptions
  - Returns triple (clean_df, bad_df, detailed_bad_df) for processing

#### 2. **Pipelines Module** (`pipelines/`)

Contains the actual data transformation and scoring logic:

##### `pipelines/members/` - Member Data Processing
- **members_pipeline.py**: End-to-end member processing script
  - Reads member CSV files using member schema
  - Applies data quality checks
  - Transforms clean data using `clean_members_data()`
  - Separates bad records for investigation
  - Outputs processed members to `data/processed/members/`

- **transformations.py**: Member-specific transformation logic
  - Standardization of member profiles
  - Enrichment with derived fields
  - Consolidation of employment information

##### `pipelines/loans/` - Loan Detail Processing
- **loans_pipeline.py**: End-to-end loan processing (follows same pattern as members)

- **transformations.py**: Loan-specific transformations
  - Converts loan term strings to numeric years
  - Standardizes loan purposes (debt consolidation, credit card, etc.)
  - Adds ingestion timestamps
  - Handles missing purpose values

##### `pipelines/loan_repayment/` - Payment History Processing
- **loan_repayment_pipeline.py**: Processes repayment data

- **transformations.py**: Payment history transformations
  - Aggregates payment information
  - Calculates payment ratios and coverage metrics

##### `pipelines/loan_defaulters/` - Credit Risk Processing
- **loan_defaulters_pipeline.py**: Processes credit risk indicators
  - Splits one dataset into two: delinquency data and defaulter details
  - Enables cleaner joins in downstream logic

- **transformations.py**: Credit risk transformations
  - Separates delinquency metrics
  - Extracts and enriches defaulter indicators

##### `pipelines/curated/` - Final Scoring and Grading
- **run_pipeline.py**: THE MAIN ORCHESTRATION SCRIPT
  - Loads all processed datasets
  - Registers them as temporary views
  - Chains business logic transformations sequentially
  - Writes final graded dataset to `data/curated/loan_grade_calculation/`

- **read_processed_data.py**: Utility module for curated pipeline
  - Functions to read all processed datasets
  - `register_views()` to make them available for SQL queries

- **business_logics_implement.py**: The scoring engine
  - `create_ph_pts_view()`: Payment history scoring
  - `create_ldh_ph_df_view()`: Defaulter history scoring
  - `create_fh_ldh_ph_df_view()`: Financial health scoring
  - `create_loan_score_view()`: Composite score calculation
  - `loan_score_final_view()`: Final grade assignment

#### 3. **Schemas Module** (`schemas/`)

Data structure definitions for each dataset:

- **members_schema.py**: Member profile structure (15 fields)
- **loans_schema.py**: Loan detail structure
- **loan_repayment_schema.py**: Payment history structure
- **loan_defaulters_schema.py**: Credit risk indicator structure

#### 4. **Configs Module** (`configs/`)

Configuration and scoring parameters:

- **application.conf**: File paths and environment settings
  - Input paths for raw data files
  - Output paths for processed and curated data
  - Separate configurations for LOCAL, TEST, PROD environments

- **calculation_config.py**: Scoring parameter thresholds
  - Point values for each scoring tier
  - Weights for composite score calculation
  - Grade boundaries for final assignment

- **dq_rules.json**: Data quality rule definitions
  - Type validation rules
  - Field length constraints
  - Referential integrity checks
  - Allowed value lists

- **pyspark.conf**: Spark-specific configurations
  - Memory allocation
  - Serialization settings
  - Execution optimization parameters

### Component Interaction Diagram

```
Raw Data Files
    ↓
[Data Readers] → [Schema Validators]
    ↓
[Data Quality Checks] → [Bad Records] (rejected)
    ↓
[Transformations] → [Processed Data]
    ↓
[Curated Pipeline] 
    ├─→ [Register as Views]
    ├─→ [Payment History Scoring]
    ├─→ [Defaulter History Scoring]
    ├─→ [Financial Health Scoring]
    ├─→ [Composite Score Calc]
    └─→ [Grade Assignment]
    ↓
[Graded Loan Dataset]
```

## System Flow

### High-Level Data Flow

```
1. INGESTION
   ├─ Members Data (member profiles, income, grades)
   ├─ Loans Data (loan amounts, terms, status)
   ├─ Repayment Data (payment history and totals)
   └─ Defaulters Data (delinquencies, bankruptcies, inquiries)

2. VALIDATION (per dataset)
   ├─ Schema validation on read
   ├─ 10+ data quality rules applied
   ├─ Valid records → Processing
   └─ Invalid records → Quarantine with detailed failure reports

3. TRANSFORMATION (per dataset)
   ├─ Member: Standardize profiles
   ├─ Loans: Convert terms, standardize purposes
   ├─ Repayments: Aggregate payment metrics
   └─ Defaulters: Split into delinquency and defaulter views

4. STAGING
   └─ All processed datasets written to `data/processed/`

5. SCORING (Integrated via Curated Pipeline)
   ├─ Load all processed datasets
   ├─ Register as temporary SQL views
   ├─ Calculate payment history score (20% weight)
   │  └─ Last payment relative to installment
   │  └─ Total payment relative to funded amount
   │
   ├─ Calculate defaulter history score (45% weight)
   │  ├─ Delinquency count in past 2 years
   │  ├─ Public records and bankruptcies
   │  └─ Recent hard inquiries (6 months)
   │
   ├─ Calculate financial health score (35% weight)
   │  ├─ Current loan status
   │  ├─ Home ownership type
   │  ├─ Credit utilization ratio
   │  └─ Member's existing credit grade/sub-grade
   │
   └─ Composite score = (PH×0.20) + (DH×0.45) + (FH×0.35)

6. GRADING
   └─ Map composite score to final grade A-F

7. CURATION
   └─ Write complete dataset with all scores and final grade
```

### Execution Sequence for Complete Pipeline

```
Step 1: Initialize Spark and Load Configuration
   └─ Create LOCAL or PROD Spark session
   └─ Load environment-specific file paths
   └─ Inject custom scoring parameters

Step 2: Process Members Dataset
   └─ Read members.csv with member schema
   └─ Apply DQ rules for member_details table
   └─ Clean and transform valid records
   └─ Write to data/processed/members/
   └─ Archive bad records to data/dq_reject/members/

Step 3: Process Loans Dataset
   └─ Read loans.csv with loan schema
   └─ Apply DQ rules for loans table
   └─ Transform: extract term years, standardize purposes
   └─ Write to data/processed/loans/
   └─ Archive bad records to data/dq_reject/loans/

Step 4: Process Loan Repayment Dataset
   └─ Read loan_repayments.csv
   └─ Apply DQ rules for repayment table
   └─ Aggregate payment metrics
   └─ Write to data/processed/loan_repayment/
   └─ Archive bad records

Step 5: Process Loan Defaulters Dataset
   └─ Read loan_defaulters.csv
   └─ Apply DQ rules for defaulters table
   └─ Split into two outputs:
      ├─ Delinquency details → data/processed/loan_defaulters_delinq/
      └─ Defaulter info → data/processed/loan_defaulters/
   └─ Archive bad records

Step 6: Curated Scoring Pipeline
   └─ Load all processed datasets into memory
   └─ Register each as a temporary SQL view
   └─ Chain transformations:
      1. Create payment history points view (ph_pts)
      2. Create defaulter history + payment view (ldh_ph_pts)
      3. Create full scoring view (fh_ldh_ph_pts)
      4. Calculate composite scores (loan_score_eval)
      5. Assign final grades (final result)
   └─ Write graded dataset to data/curated/loan_grade_calculation/ (Parquet)
```

### Key Processing Patterns

1. **Per-Dataset Processing**: Each dataset (members, loans, repayments, defaulters) is processed independently through the same pattern: read → validate → transform → write
2. **Bad Data Isolation**: Invalid records are separated at each stage, enabling data remediation
3. **Schema Validation**: All CSV reads include schema specification to catch structural issues early
4. **Temporary Views**: Curated pipeline uses SQL temporary views for readable, chainable transformations
5. **Parameterized Scoring**: Business logic uses configuration parameters, enabling threshold updates without code changes

## Directory Structure

```
lending_club_project/
│
├── README.md                           # This file
├── REQUIREMENTS.md                     # Detailed requirements specification
├── Pipfile                             # Python dependencies (PySpark, pytest)
├── Pipfile.lock                        # Locked dependency versions
│
├── configs/
│   ├── application.conf                # Environment-specific file paths & settings
│   ├── calculation_config.py           # Scoring parameters and thresholds
│   ├── dq_rules.json                   # Data quality validation rules
│   └── pyspark.conf                    # Spark configuration settings
│
├── framework/                          # Reusable data engineering framework
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── config_reader.py            # Load environment configs
│   ├── session/
│   │   ├── __init__.py
│   │   └── spark_session.py            # Create Spark sessions
│   ├── readers/
│   │   ├── __init__.py
│   │   ├── data_reader.py              # Dataset-specific readers
│   │   ├── data_reader_generic.py      # Generic flexible reader
│   │   └── read_options.py             # Format-specific read options
│   ├── writers/
│   │   ├── __init__.py
│   │   └── data_writer.py              # Unified data writer
│   └── dq_checks/
│       ├── __init__.py
│       ├── dq_logics.py                # Individual validation functions
│       └── dq_orchestrator.py          # Quality check orchestration
│
├── pipelines/                          # Dataset-specific transformation logic
│   ├── __init__.py
│   ├── members/
│   │   ├── __init__.py
│   │   ├── members_pipeline.py         # Member processing orchestration
│   │   └── transformations.py          # Member-specific transforms
│   ├── loans/
│   │   ├── __init__.py
│   │   ├── loans_pipeline.py           # Loan processing orchestration
│   │   └── transformations.py          # Loan-specific transforms
│   ├── loan_repayment/
│   │   ├── __init__.py
│   │   ├── loan_repayment_pipeline.py  # Repayment processing
│   │   └── transformations.py          # Repayment-specific transforms
│   ├── loan_defaulters/
│   │   ├── __init__.py
│   │   ├── loan_defaulters_pipeline.py # Defaulter processing
│   │   └── transformations.py          # Defaulter-specific transforms
│   └── curated/
│       ├── __init__.py
│       ├── run_pipeline.py             # MAIN: Orchestrates scoring
│       ├── read_processed_data.py      # Helpers to read processed data
│       └── business_logics_implement.py # Scoring engine
│
├── schemas/                            # Data structure definitions
│   ├── __init__.py
│   ├── members_schema.py               # Member profile structure
│   ├── loans_schema.py                 # Loan detail structure
│   ├── loan_repayment_schema.py        # Payment history structure
│   └── loan_defaulters_schema.py       # Credit risk indicator structure
│
├── tests/                              # Test suites
│   ├── __init__.py
│   ├── conftest.py                     # Pytest configuration
│   └── test_member_count.py            # Example member validation test
│
├── test_reader.py                      # Quick test script for data readers
├── test_transformations.py             # Quick test script for transformations
└── test_writer.py                      # Quick test script for data writers
```

## Setup & Installation

### Prerequisites

- Python 3.12
- Java 11+ (required for Spark/PySpark)
- Hadoop binaries (Windows users need this at `C:\hadoop`)
- pip or pipenv for dependency management

### Installation Steps

1. **Clone the repository**
   ```bash
   git clone <repo-url>
   cd lending_club_project
   ```

2. **Install dependencies using Pipenv**
   ```bash
   pipenv install
   ```
   
   Or with pip:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up data directories**
   ```bash
   mkdir -p data/raw
   mkdir -p data/processed
   mkdir -p data/dq_reject
   mkdir -p data/curated
   mkdir -p data/test
   ```

4. **Place input data files**
   Copy your raw CSV files to the appropriate locations:
   ```
   data/raw/
   ├── member_details.csv
   ├── loan_details.csv
   ├── loan_repayments.csv
   └── loan_defaulters.csv
   ```

5. **Verify Hadoop setup (Windows only)**
   ```bash
   # Set environment variables
   set HADOOP_HOME=C:\hadoop
   set PATH=%PATH%;C:\hadoop\bin
   ```

## Configuration

### Environment Configuration (`configs/application.conf`)

The system supports multiple environments. Edit `application.conf` to specify file paths:

```ini
[LOCAL]
members.file.path = data/raw/member_details.csv
loans.file.path = data/raw/loan_details.csv
loan_repayment.file.path = data/raw/loan_repayments.csv
loan_defaulters.file.path = data/raw/loan_defaulters.csv

members.output.clean.path = data/processed/members
loans.output.clean.path = data/processed/loans
loan_repayment.output.clean.path = data/processed/loan_repayment
loan_defaulters_delinq.output.clean.path = data/processed/loan_defaulters_delinq
loan_defaulters.output.clean.path = data/processed/loan_defaulters

members.output.bad.path = data/dq_reject/members
loans.output.bad.path = data/dq_reject/loans
loan_repayment.output.bad.path = data/dq_reject/loan_repayment
loan_defaulters.output.bad.path = data/dq_reject/loan_defaulters

final_data.output.path = data/curated/loan_grade_calculation
```

### Scoring Configuration (`configs/calculation_config.py`)

Adjust scoring thresholds and weights:

```python
custom_spark_confs = {
    "lending_project.unacceptable_rated_pts": 0,
    "lending_project.very_bad_rated_pts": 100,
    "lending_project.bad_rated_pts": 250,
    "lending_project.good_rated_pts": 500,
    "lending_project.very_good_rated_pts": 650,
    "lending_project.excellent_rated_pts": 800,
    # ... grade point thresholds
}
```

### Spark Configuration (`configs/pyspark.conf`)

Adjust Spark settings per environment:

```ini
[LOCAL]
spark.executor.memory = 2g
spark.driver.memory = 2g
```

## Running the Pipeline

### Option 1: Run Complete Pipeline (Recommended)

Execute the curated pipeline, which triggers all prior processing:

```bash
python pipelines/curated/run_pipeline.py
```

This orchestrates:
1. Loading all processed datasets
2. Applying scoring logic
3. Writing graded results

### Option 2: Run Individual Dataset Pipelines

Process datasets independently:

```bash
# Process members data
python pipelines/members/members_pipeline.py

# Process loans data
python pipelines/loans/loans_pipeline.py

# Process repayments
python pipelines/loan_repayment/loan_repayment_pipeline.py

# Process defaulters
python pipelines/loan_defaulters/loan_defaulters_pipeline.py
```

### Option 3: Quick Testing

Use the test scripts for quick validation:

```bash
# Test data readers
python test_reader.py

# Test transformations
python test_transformations.py

# Test data writers
python test_writer.py
```

### Option 4: Run Test Suite

Execute comprehensive tests:

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_member_count.py -v
```

## Data Formats & Schemas

### Input Data Formats

All input data is CSV with headers. Here's what each dataset contains:

#### Members Data
- member_id, emp_title, emp_length, home_ownership
- annual_income, address_state, zip_code, address_country
- grade, sub_grade, verification_status
- total_high_credit_limit, application_type
- join_annual_income, verification_status_joint

#### Loans Data
- id, member_id, loan_amount, funded_amount
- term, interest_rate, installment
- issue_month_year, loan_status, purpose, title
- dti, zip_code, ... (additional credit metrics)

#### Loan Repayments Data
- id, member_id, total_payment
- last_payment_amount, last_payment_date
- ... (additional payment metrics)

#### Loan Defaulters Data
- member_id, delinq_2_years, delinq_amount
- months_since_last_delinq
- public_record, public_record_bankruptcies
- inquiry_last_6_months, months_since_last_record

### Output Data Formats

#### Processed Datasets (CSV)
Located in `data/processed/[dataset_type]/` - cleaned, validated versions of input data with added derived fields

#### Bad Records (CSV)
Located in `data/dq_reject/[dataset_type]/` - records that failed validation, with columns indicating which rule failed and why

#### Final Graded Dataset (Parquet)
Located in `data/curated/loan_grade_calculation/` - includes:
- All member and loan fields
- payment_history_pts: Points from payment behavior
- defaulters_history_pts: Points from credit risk indicators
- financial_health_pts: Points from financial position
- loan_score: Composite score (sum of weighted dimensions)
- loan_final_grade: A-F grade

## Scoring Methodology

### The Three-Dimensional Scoring Model

The system evaluates each loan across three independent dimensions, weighted by importance:

```
Final Score = (Payment History Score × 0.20) 
            + (Defaulter History Score × 0.45) 
            + (Financial Health Score × 0.35)
```

### Dimension 1: Payment History (20% weight)

**Question**: How consistently has this borrower paid their loans?

Scoring criteria:
- Last payment amount vs. scheduled installment
- Total payments received vs. original funded amount

**Interpretation**: Recent behavior is a strong indicator of future payment reliability.

### Dimension 2: Defaulter History (45% weight) - HIGHEST WEIGHT

**Question**: What's their track record of credit problems?

Scoring criteria:
- Count of delinquencies in past 2 years
- Number of public records and bankruptcies
- Recent hard inquiries (credit seeking in last 6 months)

**Interpretation**: Past credit problems are the strongest predictor of loan default. This dimension gets the most weight.

### Dimension 3: Financial Health (35% weight)

**Question**: What's their current financial position?

Scoring criteria:
- Current loan status (fully paid, current, late, charged off)
- Home ownership type (own > mortgage > rent > other)
- Credit utilization ratio (requested loan vs. available credit)
- Existing credit grade from lender's assessment

**Interpretation**: Current financial position determines ability to pay and motivation to avoid default.

### Grade Mapping

```
Composite Score Range    →    Final Grade    →    Risk Assessment
────────────────────────────────────────────────────────────────
2500+                         A              Excellent (safest)
2000 - 2500                   B              Very Good
1500 - 2000                   C              Good
1000 - 1500                   D              Bad
750 - 1000                    E              Very Bad
≤ 750                         F              Unacceptable (highest risk)
```

### Scoring Points Scale

All scoring uses a consistent point scale:
- **0 points**: Unacceptable (highest risk)
- **100 points**: Very Bad
- **250 points**: Bad
- **500 points**: Good
- **650 points**: Very Good
- **800 points**: Excellent (lowest risk)

### Example: How a Loan Gets Scored

```
Member Profile:
- Grade: A (sub-grade A2)
- Home: Owns home
- Income: $80,000/year
- Credit limit: $30,000

Loan Details:
- Requested amount: $5,000 (16.7% of credit limit)
- Term: 36 months
- Current status: Current (paying on time)

Payment History:
- Last payment: $150 (exactly matches installment) = 500 pts
- Total paid: 50% of funded amount = 500 pts
- → Payment History Score = (500 + 500) × 0.20 = 200 pts

Defaulter History:
- Delinquencies (2 years): 0 = 800 pts
- Public records: 0 = 800 pts
- Bankruptcies: 0 = 800 pts
- Recent inquiries: 1 = 650 pts (1-2 inquiries)
- → Defaulter History Score = (800 + 800 + 800 + 650) × 0.45 = 1575 pts

Financial Health:
- Loan status: Current = 500 pts
- Home ownership: Own = 800 pts
- Credit utilization: 16.7% = 650 pts (excellent)
- Grade (A2): 760 pts (800 × 0.95)
- → Financial Health Score = (500 + 800 + 650 + 760) × 0.35 = 1029 pts

─────────────────────────────────
FINAL SCORE = 200 + 1575 + 1029 = 2804 pts → GRADE A
```

This borrower gets an A-grade because they have excellent payment history, no credit problems, and strong current financial health.

## Monitoring & Support

### Data Quality Reports

After each pipeline run, check:

1. **Raw vs. Processed Record Counts**
   - Should be minimal difference (high data quality)
   - Located in `data/dq_reject/` directories

2. **Detailed Failure Reports**
   - Lists which validation rule failed
   - Which field caused the failure
   - Original record data for investigation

3. **Scoring Distribution**
   - Check grade distribution in final output
   - A-B grades should represent good quality loans
   - F-grade loans warrant further review

### Common Issues & Solutions

**Issue**: "Unable to read application configuration"
- **Solution**: Ensure `configs/application.conf` exists and is accessible

**Issue**: Data quality rejects too many records
- **Solution**: Review `configs/dq_rules.json` - rules may be too strict

**Issue**: Spark session creation fails
- **Solution**: Verify Java is installed and `JAVA_HOME` environment variable is set

**Issue**: File not found on Windows
- **Solution**: Check Hadoop setup - may need `C:\hadoop` directory

## Contributing

To extend or modify the system:

1. **Add new scoring logic**: Modify `business_logics_implement.py`
2. **Add new data quality rules**: Update `configs/dq_rules.json` and add logic to `dq_logics.py`
3. **Add new data source**: Create new pipeline under `pipelines/` following the established pattern
4. **Change scoring parameters**: Update `configs/calculation_config.py`

---

**Last Updated**: 2026  
**Version**: 1.0  
**Status**: Production Ready
