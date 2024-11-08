"""Test whether the package is working as expected."""

from classyfire import ClassyFire, Compound


def test_classyfire():
    """Test the ClassyFire client."""
    classyfire = ClassyFire()
    compound: Compound = list(classyfire.classify_smiles("CC(=O)OC1=CC=CC=C1C(O)=O"))[0]

    assert compound.smiles == "CC(=O)OC1=CC=CC=C1C(O)=O"
    assert compound.inchikey == "InChIKey=BSYNRYMUTXBXSQ-UHFFFAOYSA-N"


def test_classify_multiple():
    """Test the ClassyFire client."""
    classyfire = ClassyFire()
    compounds = list(
        classyfire.classify_smiles(
            [
                "CC(=O)OC1=CC=CC=C1C(O)=O",
                "CC(=O)OC1=CC=CC=C1C(O)=O",
                "CNC1(CCCCC1=O)C1=CC=CC=C1Cl",
            ]
        )
    )

    assert len(compounds) == 3
    assert compounds[0].smiles == "CC(=O)OC1=CC=CC=C1C(O)=O"
    assert compounds[0].inchikey == "InChIKey=BSYNRYMUTXBXSQ-UHFFFAOYSA-N"
    assert compounds[1].smiles == "CC(=O)OC1=CC=CC=C1C(O)=O"
    assert compounds[1].inchikey == "InChIKey=BSYNRYMUTXBXSQ-UHFFFAOYSA-N"
    assert compounds[2].smiles == "CNC1(CCCCC1=O)C1=CC=CC=C1Cl"
    assert compounds[2].inchikey == "InChIKey=YQEZLKZALYSWHR-UHFFFAOYSA-N"
