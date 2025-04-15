class AWRError(Exception):
    """Base exception for Warehouse Replenishment System errors."""
    
    def __init__(self, message=None, code=None, details=None):
        """Initialize the exception.
        
        Args:
            message: Error message
            code: Error code
            details: Additional error details
        """
        self.message = message or "An error occurred in the Warehouse Replenishment System"
        self.code = code
        self.details = details
        super().__init__(self.message)
    
    def __str__(self):
        """String representation of the error."""
        if self.code:
            return f"[{self.code}] {self.message}"
        return self.message
    
    def to_dict(self):
        """Convert the exception to a dictionary."""
        error_dict = {
            'error': self.__class__.__name__,
            'message': self.message,
        }
        
        if self.code:
            error_dict['code'] = self.code
            
        if self.details:
            error_dict['details'] = self.details
            
        return error_dict


class ConfigError(AWRError):
    """Exception raised for configuration errors."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Configuration error"
        super().__init__(message, code, details)


class DatabaseError(AWRError):
    """Exception raised for database-related errors."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Database error"
        super().__init__(message, code, details)


class ValidationError(AWRError):
    """Exception raised for data validation errors."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Validation error"
        super().__init__(message, code, details)


class ForecastError(AWRError):
    """Exception raised for forecasting-related errors."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Forecasting error"
        super().__init__(message, code, details)


class OrderError(AWRError):
    """Exception raised for order-related errors."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Order error"
        super().__init__(message, code, details)


class ItemError(AWRError):
    """Exception raised for item-related errors."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Item error"
        super().__init__(message, code, details)


class VendorError(AWRError):
    """Exception raised for vendor-related errors."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Vendor error"
        super().__init__(message, code, details)


class NotFoundError(AWRError):
    """Exception raised when a requested resource is not found."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Resource not found"
        super().__init__(message, code, details)


class BatchProcessError(AWRError):
    """Exception raised for batch process errors."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Batch process error"
        super().__init__(message, code, details)


class TimeoutError(AWRError):
    """Exception raised when an operation times out."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Operation timed out"
        super().__init__(message, code, details)


class CalculationError(AWRError):
    """Exception raised for calculation errors."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Calculation error"
        super().__init__(message, code, details)


class SafetyStockError(AWRError):
    """Exception raised for safety stock calculation errors."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Safety stock calculation error"
        super().__init__(message, code, details)


class LeadTimeError(AWRError):
    """Exception raised for lead time calculation errors."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Lead time calculation error"
        super().__init__(message, code, details)


class OPAError(AWRError):
    """Exception raised for Order Policy Analysis errors."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Order Policy Analysis error"
        super().__init__(message, code, details)


class TimeBasedParameterError(AWRError):
    """Exception raised for time-based parameter errors."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Time-based parameter error"
        super().__init__(message, code, details)


class ReportingError(AWRError):
    """Exception raised for reporting errors."""
    
    def __init__(self, message=None, code=None, details=None):
        message = message or "Reporting error"
        super().__init__(message, code, details)