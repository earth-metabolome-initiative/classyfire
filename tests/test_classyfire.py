"""Test whether the package is working as expected."""

import pandas as pd
from classyfire import ClassyFire, Compound


def test_classyfire():
    """Test the ClassyFire client."""
    classyfire = ClassyFire()
    compound: Compound = classyfire.classify_inchikey("BSYNRYMUTXBXSQ-UHFFFAOYSA-N")

    assert compound.smiles == "CC(=O)OC1=CC=CC=C1C(O)=O"
    assert compound.inchikey == "InChIKey=BSYNRYMUTXBXSQ-UHFFFAOYSA-N"

    compound: Compound = classyfire.classify_smiles("CC(=O)OC1=CC=CC=C1C(O)=O")

    assert compound.smiles == "CC(=O)OC1=CC=CC=C1C(O)=O"
    assert compound.inchikey == "InChIKey=BSYNRYMUTXBXSQ-UHFFFAOYSA-N"


def test_classify_multiple():
    """Test the ClassyFire client."""
    classyfire = ClassyFire()
    compounds = list(
        classyfire.classify_inchikeys(
            [
                "BSYNRYMUTXBXSQ-UHFFFAOYSA-N",
                "BSYNRYMUTXBXSQ-UHFFFAOYSA-N",
                "YQEZLKZALYSWHR-UHFFFAOYSA-N",
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

    compounds = list(
        classyfire.classify_smiles_list(
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


def test_classify_csv():
    """Test the ClassyFire client."""
    classyfire = ClassyFire()
    compounds = list(classyfire.classify_csv("tests/example.csv"))
    df_compounds = list(classyfire.classify_df(pd.read_csv("tests/example.csv")))

    assert df_compounds == compounds
    assert len(compounds) == 2

    first_compound = compounds[0]
    second_compound = compounds[1]

    assert len(first_compound) == 3
    assert len(second_compound) == 3

    assert "inchikey1" in first_compound
    assert "inchikey2" in first_compound
    assert "smiles4" in first_compound
    assert "inchikey1" in second_compound
    assert "inchikey2" in second_compound
    assert "smiles4" in second_compound

    assert (
        first_compound["inchikey1"].smiles
        == "[H][C@@]12OC3=C(OC)C=CC4=C3[C@@]11CCN(C)[C@]([H])(C4)[C@]1([H])C=C[C@@H]2O"
    )
    assert (
        first_compound["inchikey1"].inchikey == "InChIKey=OROGSEYTTFOCAN-DNJOTXNNSA-N"
    )
    assert (
        first_compound["inchikey2"].smiles
        == "[H][C@@]12OC3=C(OC)C=CC4=C3[C@@]11CCN(C)[C@]([H])(C4)[C@]1([H])C=C[C@@H]2O"
    )
    assert (
        first_compound["inchikey2"].inchikey == "InChIKey=OROGSEYTTFOCAN-DNJOTXNNSA-N"
    )

    assert first_compound["smiles4"].smiles == "CC(=O)OC1=CC=CC=C1C(O)=O"

    assert first_compound["smiles4"].inchikey == "InChIKey=BSYNRYMUTXBXSQ-UHFFFAOYSA-N"

    assert second_compound["inchikey1"].smiles == "CNC1(CCCCC1=O)C1=CC=CC=C1Cl"
    assert (
        second_compound["inchikey1"].inchikey == "InChIKey=YQEZLKZALYSWHR-UHFFFAOYSA-N"
    )
    assert (
        second_compound["inchikey2"].smiles
        == "[H][C@@]12OC3=C(OC)C=CC4=C3[C@@]11CCN(C)[C@]([H])(C4)[C@]1([H])C=C[C@@H]2O"
    )
    assert (
        second_compound["inchikey2"].inchikey == "InChIKey=OROGSEYTTFOCAN-DNJOTXNNSA-N"
    )

    assert second_compound["smiles4"].smiles == "CNC1(CCCCC1=O)C1=CC=CC=C1Cl"

    assert second_compound["smiles4"].inchikey == "InChIKey=YQEZLKZALYSWHR-UHFFFAOYSA-N"
