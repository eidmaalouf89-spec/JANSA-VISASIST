"""Pipeline exceptions for GED pipeline."""


class NM1InputError(Exception):
    """Raised when required input column is missing or sheet not found."""
    pass


class NM1OutputError(Exception):
    """Raised when NM1 produces zero output rows."""
    pass


class ContractError(Exception):
    """Raised when a module's input contract is violated (missing required columns)."""
    pass
