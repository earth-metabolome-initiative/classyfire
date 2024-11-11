"""Submodule providing the ClassyFire class."""

from typing import Iterable, Optional, Union, Set
import time
import os
import requests
from requests.exceptions import HTTPError
from typeguard import typechecked
import compress_json
from matchms.importing import (
    load_from_mgf,
    load_from_msp,
    load_from_mzml,
    load_from_mzxml,
)
from matchms import Spectrum
from tqdm.auto import trange
import pandas as pd
from rich.console import Console
from rich.table import Table
from humanize import naturaldelta
from classyfire.exceptions import (
    ClassyFireAPIRequestError,
    EmptySMILESClassification,
    MultipleRadicalsOrAttachmentPointsNotSupported,
)
from classyfire.utils import (
    is_valid_smiles,
    convert_smiles_to_inchikey,
)
from classyfire.__version__ import __version__
from classyfire.classification import Compound


def _sleeping_loading_bar(sleep_time: int, reason: str, verbose: bool):
    """Sleeping loading bar."""
    for _ in trange(
        0,
        int(sleep_time * 1_000),
        100,
        desc=reason,
        unit="hms",
        leave=False,
        dynamic_ncols=True,
        disable=not verbose,
    ):
        time.sleep(0.1)


class ClassyFire:
    """ClassyFire API client."""

    BASE_URL = "http://classyfire.wishartlab.com"
    QUERY_URL = f"{BASE_URL}/queries"
    QUERY_STATUS_URL = f"{BASE_URL}/queries/{{query_id}}/status.json"
    RESPONSE_URL_PATTERN = f"{BASE_URL}/queries/{{query_id}}.json"
    INCHIKEY_URL_PATTERN = f"{BASE_URL}/entities/{{inchikey}}.json"

    @typechecked
    def __init__(
        self,
        timeout: int = 10,
        sleep: int = 5,
        directory: str = "classyfire_cache",
        verbose: bool = True,
    ):
        """ClassyFire API client."""
        self._timeout = timeout
        self._sleep = sleep
        self._classyfire_cache = directory
        self._verbose = verbose
        self._last_request_time = 0

    @typechecked
    def _is_classified(self, inchikey: str) -> bool:
        """Check if an InChIKey is already classified."""
        return os.path.exists(os.path.join(self._classyfire_cache, f"{inchikey}.json"))

    @typechecked
    def _classify_inchikey(self, inchikey: str) -> Compound:
        """Get the classification of a chemical entity."""

        if not os.path.exists(os.path.join(self._classyfire_cache, f"{inchikey}.json")):
            time_to_sleep = max(
                0, self._sleep - (time.time() - self._last_request_time)
            )
            self._last_request_time = time.time()
            _sleeping_loading_bar(
                int(time_to_sleep), "Sleeping before request", self._verbose
            )
            classification_response = requests.get(
                self.INCHIKEY_URL_PATTERN.format(inchikey=inchikey),
                timeout=self._timeout,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
            )
            self._last_request_time = time.time()
            classification_response.raise_for_status()
            classification_response_json = classification_response.json()

            if "report" in classification_response_json and any(
                "multiple radicals" in entry.lower()
                for entry in classification_response_json["report"]
            ):
                raise MultipleRadicalsOrAttachmentPointsNotSupported(
                    f"Multiple radicals or attachment points are not supported for {inchikey}"
                )

            if "smiles" not in classification_response_json:
                raise EmptySMILESClassification(
                    f"Empty SMILES classification for {inchikey}"
                )
            if "inchikey" not in classification_response_json:
                raise EmptySMILESClassification(
                    f"Empty InChIKey classification for {inchikey}"
                )

            compress_json.dump(
                classification_response_json,
                os.path.join(self._classyfire_cache, f"{inchikey}.json"),
            )

        return Compound.from_dict(
            compress_json.load(os.path.join(self._classyfire_cache, f"{inchikey}.json"))
        )

    @typechecked
    def classify_smiles(
        self,
        smiles: Union[Iterable[str], str],
        total: Optional[int] = None,
    ) -> Iterable[Compound]:
        """Get the classification of a list of chemical entities.

        Parameters
        ----------
        smiles : Iterable[str]
            smiles of the chemical entities
        total : Optional[int], optional
            Total number of rows in the MGF file, by default None
        """

        if isinstance(smiles, str):
            smiles = [smiles]
            total = 1

        failed_smiles: Set[str] = set()
        invalid_smiles: Set[str] = set()
        multiple_radicals: Set[str] = set()
        classified_smiles: Set[str] = set()

        started = time.time()

        console = Console()
        last_print = 0

        for smiles_candidate in smiles:
            while True:
                if time.time() - last_print > 1:
                    table = Table(title="ClassyFire Progress")
                    table.add_column("Time Elapsed", justify="right")
                    table.add_column("Remaining Time", justify="right")
                    table.add_column("Invalid SMILES", justify="right")
                    table.add_column("Failed SMILES", justify="right")
                    table.add_column("Multiple Radicals", justify="right")
                    table.add_column("Classified SMILES", justify="right")
                    processed_smiles = (
                        len(invalid_smiles)
                        + len(failed_smiles)
                        + len(multiple_radicals)
                        + len(classified_smiles)
                    )
                    table.add_row(
                        naturaldelta(time.time() - started),
                        naturaldelta(
                            (time.time() - started)
                            * (total - processed_smiles)
                            / (processed_smiles + 1)
                        ),
                        f"{len(invalid_smiles)}",
                        f"{len(failed_smiles)}",
                        f"{len(multiple_radicals)}",
                        f"{len(classified_smiles)}",
                    )
                    console.clear()
                    console.print(table)
                    last_print = time.time()

                if not is_valid_smiles(smiles_candidate):
                    invalid_smiles.add(smiles_candidate)
                    break

                if smiles_candidate in classified_smiles:
                    break

                if smiles_candidate in failed_smiles:
                    break

                if smiles_candidate in multiple_radicals:
                    break

                try:
                    classification = self._classify_inchikey(
                        inchikey=convert_smiles_to_inchikey(smiles_candidate),
                    )
                    classified_smiles.add(smiles_candidate)
                    yield classification
                    break
                except requests.exceptions.ReadTimeout:
                    _sleeping_loading_bar(
                        60,
                        "Request timeout, sleeping for 1 minute",
                        self._verbose,
                    )
                    continue
                except requests.exceptions.JSONDecodeError:
                    failed_smiles.add(smiles_candidate)
                    break
                except HTTPError as http_error:
                    if http_error.response.status_code == 429:
                        _sleeping_loading_bar(
                            60,
                            "Too many requests, sleeping for 1 minute",
                            self._verbose,
                        )
                        continue
                    failed_smiles.add(smiles_candidate)
                    break
                except (
                    EmptySMILESClassification,
                    ClassyFireAPIRequestError,
                ):
                    failed_smiles.add(smiles_candidate)
                    break
                except MultipleRadicalsOrAttachmentPointsNotSupported:
                    multiple_radicals.add(smiles_candidate)
                    break

    @typechecked
    def classify_spectra(
        self, spectra: Iterable[Spectrum], total: Optional[int] = None
    ) -> Iterable[Compound]:
        """Get the classification of a list of chemical entities from a MGF file.

        Parameters
        ----------
        mgf_path : str
            Path to the MGF/mzML/mzXML file containing the InChIKeys of the chemical entities
        total : Optional[int], optional
            Total number of rows in the MGF/mzML/mzXML file, by default None
        """
        return self.classify_smiles(
            (
                spectrum.get("smiles")
                for spectrum in spectra
                if "smiles" in spectrum.metadata
            ),
            total=total,
        )

    @typechecked
    def classify_mgf(
        self, mgf_path: str, total: Optional[int] = None
    ) -> Iterable[Compound]:
        """Get the classification of a list of chemical entities from a MGF file.

        Parameters
        ----------
        mgf_path : str
            Path to the MGF file containing the InChIKeys of the chemical entities
        total : Optional[int], optional
            Total number of rows in the MGF file, by default None
        """
        return self.classify_spectra(load_from_mgf(mgf_path), total=total)

    @typechecked
    def classify_mzml(
        self, mzml_path: str, total: Optional[int] = None
    ) -> Iterable[Compound]:
        """Get the classification of a list of chemical entities from a MZML file.

        Parameters
        ----------
        mzml_path : str
            Path to the MZML file containing the InChIKeys of the chemical entities
        total : Optional[int], optional
            Total number of rows in the MZML file, by default
        """
        return self.classify_spectra(load_from_mzml(mzml_path), total=total)

    @typechecked
    def classify_mzxml(
        self, mzxml_path: str, total: Optional[int] = None
    ) -> Iterable[Compound]:
        """Get the classification of a list of chemical entities from a MZXML file.

        Parameters
        ----------
        mzxml_path : str
            Path to the MZXML file containing the InChIKeys of the chemical entities
        total : Optional[int], optional
            Total number of rows in the MZXML file, by default None
        """
        return self.classify_spectra(load_from_mzxml(mzxml_path), total=total)

    @typechecked
    def classify_msp(
        self, msp_path: str, total: Optional[int] = None
    ) -> Iterable[Compound]:
        """Get the classification of a list of chemical entities from a MSP file.

        Parameters
        ----------
        msp_path : str
            Path to the MSP file containing the InChIKeys of the chemical entities
        total : Optional[int], optional
            Total number of rows in the MSP file, by default None
        """
        return self.classify_spectra(load_from_msp(msp_path), total=total)

    @typechecked
    def classify_series_list(
        self, series_list: Iterable[pd.Series], total: Optional[int] = None
    ) -> Iterable[Compound]:
        """Classify a list of pandas Series containing InChIKeys and/or SMILES.

        Parameters
        ----------
        series_list : Iterable[pd.Series]
            List of Series containing the InChIKeys of the chemical entities
        total : Optional[int], optional
            Total number of rows in the Series, by default None
        """
        return self.classify_smiles(
            (
                smiles_candidate
                for series in series_list
                for smiles_candidate in series.values
                if isinstance(smiles_candidate, str)
                and is_valid_smiles(smiles_candidate)
            ),
            total=total,
        )

    @typechecked
    def classify_df(self, df: pd.DataFrame) -> Iterable[Compound]:
        """Classify a pandas DataFrame containing InChIKeys and/or SMILES."""
        return self.classify_series_list((row for _, row in df.iterrows()))

    @typechecked
    def classify_csv(
        self,
        csv_path: str,
        sep: str = ",",
        header: bool = True,
        total: Optional[int] = None,
    ) -> Iterable[Compound]:
        """Get the classification of a list of chemical entities from a CSV file.

        Parameters
        ----------
        csv_path : str
            Path to the CSV file containing the InChIKeys of the chemical entities
        sep : str, optional
            Separator used in the CSV file, by default ","
        header : bool, optional
            Whether the CSV file contains a header, by default True
        total : Optional[int], optional
            Total number of rows in the CSV file, by default None
        """

        csv_reader = pd.read_csv(
            csv_path, sep=sep, header=0 if header else None, iterator=True, chunksize=1
        )

        return self.classify_series_list(
            (row.iloc[0] for row in csv_reader), total=total
        )
