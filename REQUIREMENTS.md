# Lending Club Loan Grading System - Requirements

## Project Overview

The Lending Club Loan Grading System is a data engineering pipeline designed to automatically calculate risk grades for loan applications based on comprehensive member profiles, loan details, repayment history, and credit risk indicators. The system ingests raw data, validates data quality, applies business logic transformations, and produces a final grade (A through F) that reflects the overall creditworthiness of each loan applicant.

## Functional Requirements

### 1. Data Ingestion and Source Systems

The system must read and process data from four primary CSV data sources:

#### 1.1 Member Details
- **Source**: `data/raw/member_details.csv`
- **Purpose**: Contains applicant information including employment, income, address, credit grades, and joint application details
- **Key Fields**: 
  - Unique member identifier
  - Employment title, tenure, and income
  - Home ownership status and address information
  - Existing credit grades and verification status
  - Credit limits and application types

#### 1.2 Loan Details
- **Source**: `data/raw/loan_details.csv`
- **Purpose**: Stores structured loan application data including amounts, terms, and loan status
- **Key Fields**:
  - Loan amount and funded amount
  - Loan term (in months), interest rate, and monthly installment
  - Loan purpose and status
  - Issuance date and other origination details

#### 1.3 Loan Repayments
- **Source**: `data/raw/loan_repayments.csv`
- **Purpose**: Tracks payment history including total payments received and most recent payment amounts
- **Key Fields**:
  - Total payments received to date
  - Last payment amount and date
  - Payment status indicators

#### 1.4 Loan Defaulters and Delinquency Information
- **Source**: `data/raw/loan_defaulters.csv`
- **Purpose**: Captures credit risk indicators and delinquency patterns
- **Key Fields**:
  - Delinquency count (past 2 years)
  - Public records and bankruptcy count
  - Recent credit inquiries (6-month window)
  - Months since last delinquency or public record

### 2. Data Quality Management

The system must validate all incoming data to ensure reliability and consistency:

#### 2.1 Data Type Validation
- Numeric fields (floats, integers) must be properly typed
- String fields containing numeric data must be parseable to their target types
- Dates must follow expected formats (MMM-yyyy for month-year fields)

#### 2.2 Field Value Validation
- Zip codes must be exactly 5 characters
- Grade fields must be single characters (A-G)
- State codes must be 2 characters
- All fields must adhere to their domain-specific constraints

#### 2.3 Referential Integrity
- Member IDs must be unique within the member dataset
- Loan IDs must have corresponding entries across loan, repayment, and defaulter datasets
- Cross-dataset joins must maintain referential integrity

#### 2.4 Data Quality Outputs
- Valid records are processed forward to downstream pipelines
- Invalid records are separated into a "bad data" dataset for manual review
- Detailed failure reports capture which rule failed and which field caused the failure

### 3. Data Transformation and Cleaning

The system must transform raw data into clean, standardized formats:

#### 3.1 Member Data Transformations
- Standardize and enrich member profiles with derived fields
- Consolidate employment and income information
- Normalize verification statuses

#### 3.2 Loan Data Transformations
- Convert loan term strings to numeric years (e.g., "36 months" → 3 years)
- Standardize loan purposes to a controlled vocabulary (debt consolidation, credit card, home improvement, etc.)
- Add ingestion timestamps for audit trailing
- Handle null/missing loan purposes by defaulting to "other"

#### 3.3 Defaulter and Delinquency Transformations
- Split loan defaulter data into separate delinquency and defaulter tables for clarity
- Ensure consistent key columns for joining with other datasets
- Preserve all credit risk indicators for downstream scoring

### 4. Score Calculation and Grading

The system must calculate a comprehensive loan risk score based on three dimensions:

#### 4.1 Payment History Scoring (20% weight)
- **Last Payment Score**: Evaluates the most recent payment relative to scheduled installment
  - Below 50% of installment: 100 points (very bad)
  - 50-100% of installment: 100 points (very bad)
  - Exactly 100% of installment: 500 points (good)
  - 100-150% of installment: 650 points (very good)
  - Above 150% of installment: 800 points (excellent)

- **Total Payment Score**: Evaluates cumulative repayment progress
  - 0% funded: 0 points (unacceptable)
  - 0-50% of funded amount: 500 points (good)
  - 50%+ of funded amount: 650 points (very good)

#### 4.2 Defaulter History Scoring (45% weight)
- **Delinquency Score**: Based on 2-year delinquency count
  - No delinquencies: 800 points (excellent)
  - 1-2 delinquencies: 250 points (bad)
  - 3-5 delinquencies: 100 points (very bad)
  - 5+ delinquencies: 750 points (unacceptable)

- **Public Records Score**: Based on public record count
- **Bankruptcy Score**: Based on bankruptcy count
- **Inquiry Score**: Based on recent hard inquiries (6 months)
  - Similar scaled scoring based on inquiry frequency

#### 4.3 Financial Health Scoring (35% weight)
- **Loan Status Score**: 
  - Fully paid: 800 points (excellent)
  - Current: 500 points (good)
  - Grace period: 250 points (bad)
  - Late (16-120 days): 100 points (very bad)
  - Charged off: 0 points (unacceptable)

- **Home Ownership Score**:
  - Own: 800 points (excellent)
  - Rent: 500 points (good)
  - Mortgage: 250 points (bad)
  - Other: 100 points (very bad)

- **Credit Utilization Score**: Based on requested loan as percentage of total credit limit
  - 0-10% utilized: 800 points (excellent)
  - 10-20% utilized: 650 points (very good)
  - 20-30% utilized: 500 points (good)
  - 30-50% utilized: 250 points (bad)
  - 50-70% utilized: 100 points (very bad)
  - 70%+ utilized: 0 points (unacceptable)

- **Grade and Sub-grade Score**: Lenders' own credit grade assessment
  - Grade A (A1-A5): 750-800 points with sub-grade adjustments (5% decrements per sub-grade)
  - Grade B (B1-B5): 650-700 points with adjustments
  - Grade C (C1-C5): 500-550 points with adjustments
  - Grade D (D1-D5): 250-300 points with adjustments
  - Grade E (E1-E5): 100-150 points with adjustments
  - Grades F-G: 0 points (unacceptable)

#### 4.4 Final Grade Assignment
Based on composite score:
- **Grade A**: Score > 2500 points (excellent risk profile)
- **Grade B**: 2000 < Score ≤ 2500 (very good risk profile)
- **Grade C**: 1500 < Score ≤ 2000 (good risk profile)
- **Grade D**: 1000 < Score ≤ 1500 (bad risk profile)
- **Grade E**: 750 < Score ≤ 1000 (very bad risk profile)
- **Grade F**: Score ≤ 750 (unacceptable risk profile)

### 5. Output Requirements

#### 5.1 Processed Data Output
- **Location**: `data/processed/[dataset_type]/`
- Clean, validated data in CSV format for each input dataset
- Each dataset maintains referential integrity with other datasets
- Processed data serves as input for final score calculation

#### 5.2 Data Quality Reports
- **Location**: `data/dq_reject/[dataset_type]/`
- Records that fail data quality checks are segregated
- Detailed reports include which rule failed, which field caused the failure, and the rejection reason
- Enables root cause analysis and data remediation efforts

#### 5.3 Final Graded Loan Dataset
- **Location**: `data/curated/loan_grade_calculation/`
- Complete dataset with all intermediate scores and final grade
- One record per loan with all scoring dimensions visible
- Parquet format for efficient querying and archival

### 6. Configuration Management

The system must support multiple environments (LOCAL, TEST, PROD) with:
- Environment-specific file paths for input and output
- Environment-specific Spark configurations (e.g., master mode)
- Configurable business logic parameters (scoring thresholds, weights)
- Dynamic configuration loading to support runtime environment selection

### 7. Framework and Infrastructure Requirements

#### 7.1 Spark Framework
- PySpark 3.5.1 for distributed data processing
- Local mode execution support for development (2 executor cores)
- Hive support for production environments
- Hadoop configuration for native library compatibility

#### 7.2 Data Formats
- **Input**: CSV files with headers
- **Output**: CSV (processed datasets), Parquet (final curated dataset)
- Schema validation on read to catch structural issues early

#### 7.3 Session Management
- Configurable Spark session creation with environment-aware settings
- Custom scoring configuration parameters injected into Spark session
- Reusable session management to support multiple data source reads

### 8. Error Handling and Logging

The system must provide:
- Clear error messages when configuration files are missing or misconfigured
- Data quality check summaries showing raw, clean, and rejected record counts
- Detailed logging of transformation steps and business logic application
- Environment validation to ensure required configurations exist

### 9. Testing Requirements

The system must support:
- Unit tests for individual transformation functions
- Data reader/writer testing to validate I/O operations
- Integration tests that validate the complete pipeline
- Configuration and environment testing

## Non-Functional Requirements

### Performance
- Process large datasets efficiently using distributed computing
- Minimize memory footprint through streaming/partitioning where applicable
- Support incremental processing for high-volume scenarios

### Reliability
- Fail-safe data quality mechanisms that separate good from bad data
- Idempotent operations to support reruns without side effects
- Comprehensive logging for debugging and auditing

### Maintainability
- Modular code structure separating concerns (readers, writers, transformations)
- Configuration-driven business logic to support updates without code changes
- Clear separation between framework components and business logic

### Scalability
- Support for larger datasets through Spark's distributed computing
- Extensible architecture to add new data sources or transformations
- Schema definitions for easy addition of new fields or datasets

## Success Criteria

1. All loan records are processed through the complete pipeline without data loss
2. Data quality rules catch invalid records and separate them appropriately
3. Final loan grades accurately reflect the three-dimensional risk scoring model
4. System processes all data within acceptable time and resource constraints
5. Configuration supports multiple environments without code changes
6. Generated outputs are validated and ready for downstream analytics and reporting
