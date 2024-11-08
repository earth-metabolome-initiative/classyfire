"""Utility to convert a provided SMILES string to an InChIKey string."""

from typing import List
from rdkit.Chem import MolFromSmiles  # pylint: disable=no-name-in-module
from rdkit.Chem import MolToInchiKey
from rdkit.Chem.rdchem import Mol
from rdkit import RDLogger
from typeguard import typechecked


@typechecked
def convert_smiles_to_inchikey(smiles: str) -> str:
    """Convert a provided SMILES string to an InChIKey string.

    Parameters
    ----------
    smiles : str
        The SMILES string to convert.

    Returns
    -------
    str
        The InChIKey string.
    """
    # Suppress RDKit warnings
    RDLogger.DisableLog("rdApp.error")  # type: ignore
    mol: Mol = MolFromSmiles(smiles)
    inchikey: str = MolToInchiKey(mol)
    # Re-enable RDKit warnings
    RDLogger.EnableLog("rdApp.error")  # type: ignore
    return inchikey


@typechecked
def convert_smiles_to_inchikeys(smiles: List[str]) -> List[str]:
    """Convert a list of provided SMILES strings to a list of InChIKey strings.

    Parameters
    ----------
    smiles : List[str]
        The list of SMILES strings to convert.

    Returns
    -------
    List[str]
        The list of InChIKey strings.
    """
    return [convert_smiles_to_inchikey(smiles) for smiles in smiles]
