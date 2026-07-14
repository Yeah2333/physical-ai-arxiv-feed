"""Public contract primitives for the Physical AI arXiv Fact Feed."""

from .contract import (
    CONTRACT_VERSION,
    RECORD_SCHEMA_VERSION,
    ContractValidationError,
    build_observation,
    validate_observation,
    validate_scope,
)

__all__ = [
    "CONTRACT_VERSION",
    "RECORD_SCHEMA_VERSION",
    "ContractValidationError",
    "build_observation",
    "validate_observation",
    "validate_scope",
]
