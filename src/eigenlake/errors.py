from __future__ import annotations


class EigenlakeError(Exception):
    """Base error for the Eigenlake SDK."""


class AuthenticationError(EigenlakeError):
    pass


class NotFoundError(EigenlakeError):
    pass


class ConflictError(EigenlakeError):
    pass


class ValidationError(EigenlakeError):
    pass


class APIError(EigenlakeError):
    pass


class NetworkError(EigenlakeError):
    pass
