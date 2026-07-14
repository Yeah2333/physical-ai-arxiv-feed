"""Public contract primitives for the robotics arXiv fact feed."""

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
