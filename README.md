# ClassyFire API Client

<!-- [![pip](https://badge.fury.io/py/classyfire.svg)](https://pypi.org/project/classyfire/)
[![python](https://img.shields.io/pypi/pyversions/classyfire)](https://pypi.org/project/classyfire/)
[![license](https://img.shields.io/pypi/l/classyfire)](https://pypi.org/project/classyfire/)
[![downloads](https://pepy.tech/badge/classyfire)](https://pepy.tech/project/classyfire) -->
[![Github Actions](https://github.com/LucaCappelletti94/classyfire/actions/workflows/python.yml/badge.svg)](https://github.com/LucaCappelletti94/classyfire/actions/)

A Python package to classify chemical entities using the [ClassyFire API](http://classyfire.wishartlab.com). This package provides a simple interface to retrieve the chemical classification of compounds by their InChIKey or SMILES, which are automatically cached to avoid redundant requests to the ClassyFire API. Caching ensures that repeated queries for the same InChIKey or SMILES are faster and more efficient, not requiring additional network calls or waiting time.

In some relatively rare cases, the ClassyFire API may not be able to classify a compound, and in such cases it will return an empty classification. When this happens, depending on the behaviour selected for the `behavior_on_empty_classification` parameter, the package will either raise an `EmptyInchikeyClassification` or `EmptySMILESClassification` exception or, when you are classifying multiple compounds, it will try again to classify the failed compounds after all other compounds have been classified. This is done by default, as sometimes the APIs may fail to classify a compound but succeed in doing so after a while (which may be several minutes at least).

*While it is unclear why this happens, it is my belief that they run the classifier in such cases, while in all other instances they use a lookup table.*

Furthermore, the package offers a CLI interface to classify InChIKeys or SMILES from the command line, which can be useful for batch processing of chemical entities.

## Installation

To install the package, while we would really like to publish this package on PyPi, we are currently unable to do so due to the fact that the [PEP 541 Request being in limbo](https://github.com/pypi/support/issues/4935). As such, you can install the package from the GitHub repository using the following command:

```bash
pip install git+https://github.com/LucaCappelletti94/classyfire
```

## Usage

To use the `ClassyFire` client, first instantiate it with optional parameters like `timeout`, `sleep`. **Note that the API documentation specifies to not execute more than 12 requests per minute, so you should not set a `sleep` parameter smaller than `60/12=5`.** Furthermore, in practice, the API will still blacklist you when you exceed 6 requests per minute, so you should set the `sleep` parameter to at least `10` to be safe.

```python
from classyfire import ClassyFire, Compound

# Initialize the ClassyFire API client
client: ClassyFire = ClassyFire(
    # The email adress used to identify the user agent,
    # so to allow the API maintainers to contact you
    # in case of issues. This is just to be polite.
    email="your.email.for@user.agent",
    # Maximum time to wait for a response
    timeout = 10,
    # Time to wait between requests (in seconds)
    sleep = 10,
    # What to do when the API returns an empty classification
    behavior_on_empty_classification="retry-last",
    # Whether to show a loading bar when fetching
    # multiple classifications
    verbose = True
)

# You can classify a single InChIKey as follows
inchikey: str = "BSYNRYMUTXBXSQ-UHFFFAOYSA-N"
compound: Compound = client.classify_inchikey(inchikey)

# Access compound details
assert compound.smiles == "CC(=O)OC1=CC=CC=C1C(O)=O"
assert compound.kingdom.name == "Organic compounds"

smiles: str = "CC(=O)OC1=CC=CC=C1C(O)=O"
compound: Compound = client.classify_smiles(smiles)

# Access compound details
assert compound.inchikey == "InChIKey=BSYNRYMUTXBXSQ-UHFFFAOYSA-N"
assert compound.kingdom.name == "Organic compounds"

# And you can execute multiple classifications in sequence
inchikeys = [
    "BSYNRYMUTXBXSQ-UHFFFAOYSA-N",
    "YQEZLKZALYSWHR-UHFFFAOYSA-N",
]

# The method returns an iterable of Compound instances
for compound in client.classify_inchikeys(inchikeys):
    assert isinstance(compound, Compound)
    assert compound.smiles is not None

# Analogously, you can classify multiple SMILES

smiles_list = [
    "CC(=O)OC1=CC=CC=C1C(O)=O",
    "[H][C@@]12OC3=C(OC)C=CC4=C3[C@@]11CCN(C)[C@]([H])(C4)[C@]1([H])C=C[C@@H]2O",
]

# The method returns an iterable of Compound instances
for compound in client.classify_smiles_list(smiles_list):
    assert isinstance(compound, Compound)
    assert compound.inchikey is not None

```

### Classify CSV or TSV files

Finally, it is possible to classify a CSV or TSV file containing InChIKeys and/or SMILES using the method `classify_csv`, which will yeald a generator of dictionaries with as keys the column names of the InChIKeys and/or SMILES and as values the corresponding classifications.

```python
import pandas as pd
from classyfire import ClassyFire

# Initialize the ClassyFire API client
client: ClassyFire = ClassyFire(
    email="your.email.for@user.agent",
)

# Classify a CSV file, which in this example
# is equal to the following DataFrame
csv: pd.DataFrame = pd.DataFrame({
    "InChIKey": [
        "BSYNRYMUTXBXSQ-UHFFFAOYSA-N",
        "YQEZLKZALYSWHR-UHFFFAOYSA-N",
    ],
    "OtherColumn": [
        "Value1",
        "Value2",
    ],
    "AnotherInChIKey": [
        "BSYNRYMUTXBXSQ-UHFFFAOYSA-N",
        "YQEZLKZALYSWHR-UHFFFAOYSA-N",
    ],
})

# We save the DataFrame to a CSV file so that we can
# show the method that loads a CSV file from disk
csv.to_csv("readme_example.csv", index=False)

# Classify the DataFrame
df_classifications = client.classify_df(csv)
classifications = client.classify_csv("readme_example.csv", sep=",", header=True)

assert list(df_classifications) == list(classifications)
```

## Command Line Interface

The package also provides a command line interface to classify InChIKeys from the command line. The CLI interface is available through the `classyfire` command, which can be used to classify InChIKeys from the command line.

To classify a single InChIKey, use the following command:

```bash
classyfire BSYNRYMUTXBXSQ-UHFFFAOYSA-N
```

which will output to stdout the classification of the InChIKey:

```json
{
  "smiles": "CC(=O)OC1=CC=CC=C1C(O)=O",
  "inchikey": "InChIKey=BSYNRYMUTXBXSQ-UHFFFAOYSA-N",
  "kingdom": {
    "name": "Organic compounds",
    "description": "Compounds that contain at least one carbon atom, excluding isocyanide/cyanide and their non-hydrocarbyl derivatives, thiophosgene, carbon diselenide, carbon monosulfide, carbon disulfide, carbon subsulfide, carbon monoxide, carbon dioxide, Carbon suboxide, and dicarbon monoxide.",
    "chemont_id": "CHEMONTID:0000000",
    "url": "http://classyfire.wishartlab.com/tax_nodes/C0000000"
  },
  "...": "...",
  "predicted_lipidmaps_terms": [
    "Dicarboxylic acids (FA0117)"
  ],
  "classification_version": "2.1"
}
```

To classify a single SMILES, use the following command:

```bash
classyfire "CC(=O)OC1=CC=CC=C1C(O)=O"
```

which will output to stdout the classification of the SMILES:

```json
{
  "smiles": "CC(=O)OC1=CC=CC=C1C(O)=O",
  "inchikey": "InChIKey=BSYNRYMUTXBXSQ-UHFFFAOYSA-N",
  "kingdom": {
    "name": "Organic compounds",
    "description": "Compounds that contain at least one carbon atom, excluding isocyanide/cyanide and their non-hydrocarbyl derivatives, thiophosgene, carbon diselenide, carbon monosulfide, carbon disulfide, carbon subsulfide, carbon monoxide, carbon dioxide, Carbon suboxide, and dicarbon monoxide.",
    "chemont_id": "CHEMONTID:0000000",
    "url": "http://classyfire.wishartlab.com/tax_nodes/C0000000"
  },
  "...": "...",
  "predicted_lipidmaps_terms": [
    "Dicarboxylic acids (FA0117)"
  ],
  "classification_version": "2.1"
}
```

If you are only interested in getting the gist of the classification, you can use the `--short` flag, which will output a selection of the most relevant fields:

```bash
classyfire "CC(=O)OC1=CC=CC=C1C(O)=O" --short
```

which will output:

```bash
Description: This compound belongs to the class of organic compounds known as acylsalicylic
             acids. These are o-acylated derivatives of salicylic acid.
Direct Parent: Acylsalicylic acids
Kingdom: Organic compounds
└── Superclass: Benzenoids
    └── Class: Benzene and substituted derivatives
        └── Subclass: Benzoic acids and derivatives
```

Given a CSV file containing InChIKeys and/or SMILES, the CLI interface can be used to classify all InChIKeys and/or SMILES in the file.

| InChIKey1                           | InChIKey2                           | Kebab | Pizza | SMILES1                  |
|-------------------------------------|-------------------------------------|-------|-------|--------------------------|
| OROGSEYTTFOCAN-DNJOTXNNSA-N         | OROGSEYTTFOCAN-DNJOTXNNSA-N         | 1     | 2     | CC(=O)OC1=CC=CC=C1C(O)=O |
| YQEZLKZALYSWHR-UHFFFAOYSA-N         | OROGSEYTTFOCAN-DNJOTXNNSA-N         | 3     | 4     | CC(=O)OC1=CC=CC=C1C(O)=O |

To classify a CSV file containing InChIKeys, use the following command:

```bash
classyfire tests/example.csv --verbose --separator "," --output "output.json.gz" --email "your.email.for@user.agent"
```

Which will classify the InChIKeys in the file `tests/example.csv` and output the classifications to the file `output.json.gz`. The `--verbose` flag will show a progress bar, while the `--separator` flag specifies the separator used in the CSV file.

It is possible also to process mass spectrometry spectra documents such as MGF, MSP, and mzML files. The CLI interface can be used to classify all InChIKeys and/or SMILES in the file. As described earlier, you can use the following command:

```bash
classyfire tests/example.mgf --verbose --output "output.json.gz" --email "your.email.for@user.agent"
```

## License

This project is licensed under the MIT License.
