"""Utility to determine whether a provided SMILES string is valid."""

from typeguard import typechecked
from typeguard import TypeCheckError
from classyfire.utils.convert_smiles_to_inchikey import convert_smiles_to_inchikey


@typechecked
def is_valid_smiles(smiles: str) -> bool:
    """Determine whether a provided SMILES string is valid.

    Parameters
    ----------
    smiles : str
        The SMILES string to validate.

    Returns
    -------
    bool
        Whether the SMILES string is valid.
    """
    try:
        convert_smiles_to_inchikey(smiles)
        return True
    except (TypeError, TypeCheckError):
        return False
