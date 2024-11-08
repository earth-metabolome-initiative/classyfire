"""Submodule providing the ClassyFire class."""

from typing import Dict, Iterable, List, Tuple, cast, Optional, Union
import warnings
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
from tqdm.auto import tqdm, trange
import pandas as pd
from dict_hash import sha256
from classyfire.exceptions import (
    ClassyFireAPIRequestError,
    InvalidSMILES,
    EmptySMILESClassification,
    MultipleRadicalsOrAttachmentPointsNotSupported,
)
from classyfire.utils import (
    is_valid_inchikey,
    is_valid_smiles,
    convert_smiles_to_inchikey,
    convert_smiles_to_inchikeys,
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
    RESPONSE_URL_PATTERN = f"{BASE_URL}/queries/{{query_id}}.json"

    @typechecked
    def __init__(
        self,
        email: Optional[str] = None,
        timeout: int = 10,
        sleep: int = 10,
        classification_attempts: int = 10,
        sleep_between_attempts: int = 10,
        chunk_size: int = 500,
        directory: str = "classyfire_cache",
        verbose: bool = False,
    ):
        """ClassyFire API client.

        Parameters
        ----------
        email : str
            Email address to be used as part of the user agent so
            that the server administrators can contact you in case
            of issues.
        timeout : int, optional
            Timeout for the HTTP requests, by default 10
        sleep : int, optional
            Sleep time between requests, by default 5
        user_agent : Optional[UserAgent], optional
            User agent for the HTTP requests, by default None
        classification_attempts : int, optional
            Number of attempts to classify an InChIKey, by default 3.
            This only applies when the behavior_on_empty_classification is set to "retry-last"
        sleep_between_attempts : int, optional
            Sleep time between classification attempts, by default 10
        chunk_size : int,
            Number of InChIKeys to classify in a single request, by default 100
        classyfire_cache : str, optional
            Directory to store the cache files, by default "classyfire_cache"
        verbose : bool, optional
            Whether to print verbose output, by default False
        """
        self._timeout = timeout
        self._sleep = sleep
        self._user_agent: str = (
            f"ClassyFire/{__version__}"
            if email is None
            else f"ClassyFire/{__version__} ({email})"
        )
        self._classification_attempts = classification_attempts
        self._sleep_between_attempts = sleep_between_attempts
        self._chunk_size = chunk_size
        self._classyfire_cache = directory
        self._verbose = verbose
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": self._user_agent,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        self._last_request_time = 0

    @typechecked
    def _is_classified(self, inchikey: str) -> bool:
        """Check if an InChIKey is already classified."""
        return os.path.exists(os.path.join(self._classyfire_cache, f"{inchikey}.json"))

    @typechecked
    def _classify_smiles(self, smiles: List[str]) -> List[Compound]:
        """Get the classification of a chemical entity."""

        inchikeys: List[str] = convert_smiles_to_inchikeys(smiles)

        unclassified_smiles: List[str] = [
            s
            for (inchikey, s) in zip(inchikeys, smiles)
            if not self._is_classified(inchikey)
        ]

        if len(unclassified_smiles) > 0:
            expected_label: str = sha256({"smiles": unclassified_smiles})

            query_data: Dict[str, str] = {
                "label": expected_label,
                "query_input": "\n".join(unclassified_smiles),
                "query_type": "STRUCTURE",
            }

            time_to_sleep = max(
                0, self._sleep - (time.time() - self._last_request_time)
            )
            _sleeping_loading_bar(
                int(time_to_sleep), "Sleeping before request", self._verbose
            )
            self._last_request_time = int(time.time())
            query_response = self._session.post(
                self.QUERY_URL,
                json=query_data,
                timeout=self._timeout,
            )
            self._last_request_time = int(time.time())
            query_response.raise_for_status()
            query_response_json: Dict = query_response.json()

            query_id: int = query_response_json["id"]

            _sleeping_loading_bar(5, "Sleeping before classification", self._verbose)

            self._last_request_time = int(time.time())
            classification_response = self._session.get(
                self.RESPONSE_URL_PATTERN.format(query_id=query_id),
                timeout=self._timeout,
            )
            self._last_request_time = int(time.time())

            classification_response.raise_for_status()

            classification_response_json: Dict = classification_response.json()

            for entities in classification_response_json["entities"]:
                if "report" in entities:
                    if entities["report"] is None:
                        raise EmptySMILESClassification(
                            f"Classification of {entities['smiles']} failed"
                        )
                    if "multiple radicals" in entities["report"].lower():
                        raise MultipleRadicalsOrAttachmentPointsNotSupported(
                            f"Multiple radicals or attachment points are not supported for {entities['smiles']}"
                        )
                inchikey = convert_smiles_to_inchikey(entities["smiles"])
                inchikey = inchikey.replace("InChIKey=", "")
                compress_json.dump(
                    entities,
                    os.path.join(self._classyfire_cache, f"{inchikey}.json"),
                )
            
            if len(classification_response_json["invalid_entities"]) > 0:
                raise EmptySMILESClassification(
                    f"Classification of {classification_response_json['invalid_entities']} failed"
                )

        return [
            Compound.from_dict(
                compress_json.load(
                    os.path.join(self._classyfire_cache, f"{inchikey}.json")
                )
            )
            for inchikey in inchikeys
        ]

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

        batch: List[str] = []

        failed_smiles: List[str] = []

        for smiles in tqdm(
            smiles,
            desc="Classifying SMILES",
            unit="SMILES",
            leave=False,
            dynamic_ncols=True,
            total=total,
            disable=not self._verbose,
        ):
            batch.append(smiles)
            if len(batch) == self._chunk_size:
                try:
                    yield from self._classify_smiles(batch)
                except HTTPError as http_error:
                    if http_error.response.status_code == 429:
                        _sleeping_loading_bar(
                            60,
                            "Too many requests, sleeping for 1 minute",
                            self._verbose,
                        )
                    failed_smiles.extend(batch)
                except EmptySMILESClassification as empty_smiles_error:
                    warnings.warn(str(empty_smiles_error))
                    failed_smiles.extend(batch)
                except (
                    MultipleRadicalsOrAttachmentPointsNotSupported
                ) as multiple_radicals_error:
                    warnings.warn(str(multiple_radicals_error))
                    failed_smiles.extend(batch)
                batch = []

        if len(batch) > 0:
            yield from self._classify_smiles(batch)

        for smile in tqdm(
            failed_smiles,
            desc="Retrying failed SMILES",
            unit="SMILES",
            leave=False,
            dynamic_ncols=True,
            disable=not self._verbose,
        ):
            try:
                yield from self._classify_smiles([smile])
            except HTTPError as http_error:
                if http_error.response.status_code == 429:
                    _sleeping_loading_bar(
                        60,
                        "Too many requests, sleeping for 1 minute",
                        self._verbose,
                    )
            except (
                EmptySMILESClassification,
                MultipleRadicalsOrAttachmentPointsNotSupported,
            ):
                pass

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
