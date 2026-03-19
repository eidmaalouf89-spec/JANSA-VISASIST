"""Enumerations for the JANSA VISASIST pipeline."""

from enum import Enum


class Severity(str, Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


class RowQuality(str, Enum):
    OK = "OK"
    WARNING = "WARNING"
    ERROR = "ERROR"


class MappingConfidence(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    FAILED = "FAILED"
