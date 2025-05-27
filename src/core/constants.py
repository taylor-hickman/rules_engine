"""Constants used throughout the NPI Suppression Rule Engine."""

# Database settings
DEFAULT_BATCH_SIZE = 10000
MAX_BATCH_SIZE = 200000
CONNECTION_RETRY_ATTEMPTS = 3
CONNECTION_TIMEOUT = 30

# NPI validation
NPI_LENGTH = 10

# Provider types
class ProviderType:
    PRACTITIONER = 'practitioner'
    FACILITY = 'facility'
    ANCILLARY = 'ancillary'
    UNCATEGORIZED = 'uncategorized'
    UNKNOWN = 'unknown'

# Rule columns
RULE_COLUMNS = [
    'start_1', 'start_2', 'specialty', 'end_date', 'term_date',
    'non_md_end_dt', 'provider_type', 'degree_cd'
]

# Report file names
REPORT_FILES = {
    'summary': 'suppression_summary.txt',
    'universe': 'universe_validation_report.csv',
    'master': 'master_suppression_results.csv',
    'database': 'database_impact_report.csv',
    'rule': 'rule_impact_analysis.csv',
    'rule_column': 'rule_column_impact_report.csv',
    'combination': 'rule_combination_analysis.csv',
    'detailed': 'detailed_npi_suppression_report.csv'
}

# Logging format
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'