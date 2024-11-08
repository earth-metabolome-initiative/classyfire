"""Exceptions used in the ClassyFire package."""

from typing import List


class ClassyFireError(Exception):
    """Base exception for ClassyFire errors."""


class ClassyFireAPIRequestError(ClassyFireError):
    """ClassyFire API request error."""

    def __init__(self, message: str):
        """ClassyFire API request error."""
        super().__init__(message)


class MultipleRadicalsOrAttachmentPointsNotSupported(ClassyFireError):
    """Multiple radicals or attachment points not supported exception."""

    def __init__(self, smiles_or_inchikey: str):
        """Multiple radicals or attachment points not supported exception."""
        super().__init__(
            f"Multiple radicals or attachment points not supported by ClassyFire: {smiles_or_inchikey}"
        )


class EmptySMILESClassification(ClassyFireError):
    """Empty classification exception."""

    def __init__(self, smiles: str):
        """Empty classification exception."""
        super().__init__(f"Empty classification for SMILES: {smiles}")


class InvalidInchiKey(ClassyFireError):
    """Invalid InChIKey exception."""

    def __init__(self, inchikey: str):
        """Invalid InChIKey exception."""
        super().__init__(f"Invalid InChIKey: {inchikey}")


class InvalidSMILES(ClassyFireError):
    """Invalid SMILES exception."""

    def __init__(self, smiles: str):
        """Invalid SMILES exception."""
        super().__init__(f"Invalid SMILES: {smiles}")
