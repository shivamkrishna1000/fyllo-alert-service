class FylloAPIError(Exception):
    """Raised when Fyllo API request fails."""


class FylloAuthError(Exception):
    """Raised when authentication with Fyllo fails."""


class DatabaseError(Exception):
    """Raised when database operation fails."""


class NotificationError(Exception):
    """Raised when notification sending fails."""
