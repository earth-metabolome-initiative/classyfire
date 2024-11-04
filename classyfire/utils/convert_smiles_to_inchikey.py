"""Utility to convert a provided SMILES string to an InChIKey string."""

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
