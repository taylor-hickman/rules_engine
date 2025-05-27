# NPI Provider Suppression Rule Engine

A Python-based engine for processing NPI (National Provider Identifier) suppression rules against healthcare provider data. The system validates provider universes, applies configurable suppression rules, and generates comprehensive reports.

## Features

- **Provider Type Categorization**: Automatically categorizes NPIs into practitioners, facilities, ancillary, and uncategorized
- **Configurable Rules**: Define suppression rules in YAML format with SQL queries
- **Persistent Connection**: Single database connection maintains volatile tables throughout processing
- **Comprehensive Reporting**: Generates multiple analysis reports including master results, rule impacts, and database impacts
- **Batch Processing**: Efficiently handles large datasets with configurable batch sizes

## Project Structure

```
src/
├── core/               # Core functionality
│   ├── config.py      # Configuration management with dataclasses
│   ├── connections.py # Persistent database connection management
│   ├── constants.py   # Centralized constants
│   └── exceptions.py  # Custom exception hierarchy
├── validation/        # Data validation
│   ├── npi.py        # NPI validation utilities
│   └── universe.py   # Universe validation and categorization
├── processing/        # Rule processing
│   ├── engine.py     # Main rule execution engine
│   ├── rules.py      # Rule definition and loading
│   └── tables.py     # Volatile table management
├── reporting/         # Report generation
│   ├── base.py       # Base report classes
│   ├── metrics.py    # Metrics calculation
│   └── generators.py # Report generators
├── orchestration/     # High-level orchestration
│   ├── pipeline.py   # Main processing pipeline
│   ├── universe.py   # Universe processing orchestrator
│   ├── rules.py      # Rule processing orchestrator
│   └── reports.py    # Report generation orchestrator
└── utils/            # Utilities
    ├── logging_config.py # Centralized logging
    └── csv_analyzer.py   # CSV analysis utilities
```

## Installation

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Create a `.env` file based on `.env.example` with your database credentials

## Configuration

### Database Configuration (.env)
```env
DB_HOST=your_teradata_host.com
DB_USERNAME=your_username
DB_PASSWORD=your_password
DB_PORT=1025
DB_LOGMECH=TD2
DB_ENCRYPTDATA=true
```

### Rules Configuration (rules.yml)
```yaml
rules:
  rule_1:
    name: "Rule 1 - Specialty Suppression"
    description: "Suppress specific specialties"
    level: "specialty"
    sql_query: |
      SELECT DISTINCT 
        'rule_1' as start_1,
        ...
      FROM providerdataservice_core_v.prov_spayer_practitioners
      WHERE ...
```

## Usage

### Process CSV Universe
```bash
python run.py --rules rules.yml --csv-universe universe.csv --output ./reports
```

### Process Teradata Table
```bash
python run.py --rules rules.yml --teradata-universe schema.table --output ./reports
```

### Options
- `--rules`: Path to YAML rules configuration file (required)
- `--csv-universe`: Path to CSV file containing universe NPIs
- `--teradata-universe`: Teradata table name containing universe NPIs
- `--csv-npi-column`: Column name containing NPIs in CSV (default: 'npi')
- `--batch-size`: Batch size for processing (default: 10000)
- `--output`: Output directory for reports (default: './reports')
- `--verbose`: Enable verbose debug logging
- `--dry-run`: Validate configuration without processing

## Processing Pipeline

1. **Universe Loading**: Load NPIs from CSV or Teradata table
2. **Provider Type Categorization**: Categorize NPIs using Spayer database relationships
3. **Practitioner Filtering**: Create universe containing only practitioners
4. **Rule Execution**: Apply all configured suppression rules
5. **Results Aggregation**: Combine all rule results into master table
6. **Report Generation**: Generate comprehensive analysis reports

## Generated Reports

- `universe_validation_report.csv`: Provider type categorization results
- `master_suppression_results.csv`: Complete results with all rule flags
- `rule_impact_analysis.csv`: Impact analysis for each rule
- `rule_combination_analysis.csv`: Analysis of rule combinations
- `database_impact_report.csv`: Impact on Spayer database entities
- `suppression_summary.txt`: Executive summary report

## Architecture Notes

- **Persistent Connection**: Uses a singleton pattern to maintain one database connection throughout processing, ensuring volatile tables remain accessible
- **Volatile Tables**: All intermediate results stored in volatile tables with unique session IDs
- **Batch Processing**: Large datasets processed in configurable batches to manage memory
- **Error Handling**: Comprehensive error handling with custom exception hierarchy

## Development

The codebase follows Python best practices:
- Type hints for better IDE support
- Dataclasses for configuration management
- Context managers for resource management
- Comprehensive logging throughout
- Modular design with clear separation of concerns