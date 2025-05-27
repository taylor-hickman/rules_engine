"""Custom exceptions for the NPI Suppression Rule Engine."""


class SuppressionEngineError(Exception):
    """Base exception for all suppression engine errors."""
    pass


class ConfigurationError(SuppressionEngineError):
    """Raised when configuration is invalid or missing."""
    pass


class DatabaseConnectionError(SuppressionEngineError):
    """Raised when database connection fails."""
    pass


class ValidationError(SuppressionEngineError):
    """Raised when data validation fails."""
    pass


class UniverseValidationError(ValidationError):
    """Raised when universe validation fails."""
    pass


class RuleProcessingError(SuppressionEngineError):
    """Raised when rule processing fails."""
    pass


class ReportGenerationError(SuppressionEngineError):
    """Raised when report generation fails."""
    pass