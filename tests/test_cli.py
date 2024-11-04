"""Test commands of the CLI to check if they are working as expected."""

import os
from typing import Dict, List
import subprocess
import compress_json
from classyfire import ClassyFire, Compound


def test_classify_inchikey():
    """Test classify_inchikey command."""
    inchikey = "BSYNRYMUTXBXSQ-UHFFFAOYSA-N"
    expected: Compound = ClassyFire().classify_inchikey(inchikey)
    output = "output.json"
    subprocess.run(
        [
            "classyfire",
            inchikey,
            "--output",
            output,
        ],
        check=True,
    )
    assert os.path.exists(output)
    assert expected == Compound.from_dict(compress_json.load(output))


def test_classify_csv() -> None:
    """Test classify_csv command."""
    expected: List[Dict[str, Compound]] = list(
        ClassyFire().classify_csv("tests/example.csv")
    )
    expected_dict = [
        compound.to_dict() for compounds in expected for compound in compounds.values()
    ]
    output = "output.json"
    subprocess.run(
        [
            "classyfire",
            "tests/example.csv",
            "--output",
            output,
        ],
        check=True,
    )
    assert os.path.exists(output)

    loaded = compress_json.load(output)

    assert expected_dict == loaded
