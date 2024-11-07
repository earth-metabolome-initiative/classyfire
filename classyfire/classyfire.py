"""Submodule providing the ClassyFire class."""

from typing import Dict, Iterable, List, Tuple, cast, Optional
import warnings
import time
import requests
from typeguard import typechecked
from cache_decorator import Cache
from matchms.importing import (
    load_from_mgf,
    load_from_msp,
    load_from_mzml,
    load_from_mzxml,
)
from matchms import Spectrum
from tqdm.auto import tqdm, trange
import pandas as pd
from classyfire.exceptions import (
    InvalidInchiKey,
    ClassyFireAPIRequestError,
    InvalidSMILES,
    EmptyInchikeyClassification,
    EmptySMILESClassification,
    MultipleRadicalsOrAttachmentPointsNotSupported,
)
from classyfire.utils import (
    is_valid_inchikey,
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

    URL = "http://classyfire.wishartlab.com"

    @typechecked
    def __init__(
        self,
        email: Optional[str] = None,
        timeout: int = 10,
        sleep: int = 10,
        classification_attempts: int = 10,
        sleep_between_attempts: int = 10,
        behavior_on_empty_classification: str = "retry-last",
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
        behavior_on_empty_classification : str, optional
            Behavior when an empty classification is returned, by default "raise"
            Other allowed values are "warn", "ignore" and "retry-last"
            When "retry-last" is used, the failed classifications requests will
            be retried once at the end of the classification process, allowing for
            the classification of InChIKeys that were not classified in the first
            attempt due to server-side issues.
        verbose : bool, optional
            Whether to print verbose output, by default False
        """
        if behavior_on_empty_classification not in [
            "raise",
            "warn",
            "ignore",
            "retry-last",
        ]:
            raise ValueError(
                f"Invalid value for 'behavior_on_empty_classification': "
                f"{behavior_on_empty_classification}"
                "Allowed values are 'raise', 'warn', 'ignore' and 'retry-last'"
            )

        self._timeout = timeout
        self._sleep = sleep
        self._user_agent: str = (
            f"ClassyFire/{__version__}"
            if email is None
            else f"ClassyFire/{__version__} ({email})"
        )
        self._classification_attempts = classification_attempts
        self._sleep_between_attempts = sleep_between_attempts
        self._behavior_on_empty_classification = behavior_on_empty_classification
        self._verbose = verbose
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": self._user_agent,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "DNT": "1",  # Do Not Track
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
            }
        )
        self._last_request_time = 0

    def build_url(self, inchikey: str) -> str:
        """Build the URL for the classification request.

        Parameters
        ----------
        inchikey : str
            InChIKey of the chemical entity
        """
        inchikey = inchikey.replace("InChIKey=", "")
        if not is_valid_inchikey(inchikey):
            raise InvalidInchiKey(inchikey)
        return f"{ClassyFire.URL}/entities/{inchikey}.json"

    @Cache(
        cache_path="{cache_dir}/{inchikey}.json",
        cache_dir="classyfire_cache",
    )
    @typechecked
    def _classify_inchikey(self, inchikey: str) -> Dict:
        """Get the classification of a chemical entity."""

        try:
            time_to_sleep = max(
                0, self._sleep - (time.time() - self._last_request_time)
            )
            _sleeping_loading_bar(
                int(time_to_sleep), "Sleeping before request", self._verbose
            )
            response = self._session.get(
                self.build_url(inchikey),
                timeout=self._timeout,
            )
            self._last_request_time = int(time.time())
            response.raise_for_status()
            response_json: Dict = response.json()

            if "report" in response_json and any(
                "multiple radicals" in report.lower()
                for report in response_json["report"]
            ):
                raise MultipleRadicalsOrAttachmentPointsNotSupported(inchikey)

            if not response_json:
                if self._behavior_on_empty_classification == "warn":
                    warnings.warn(f"Empty classification for InChIKey: {inchikey}")
                elif self._behavior_on_empty_classification == "ignore":
                    pass
                elif self._behavior_on_empty_classification in ("retry-last", "raise"):
                    if self._behavior_on_empty_classification == "retry-last":
                        warnings.warn(
                            f"Empty classification for InChIKey: {inchikey}. "
                            f"Will retry classification at the end of the classification process."
                        )
                    raise EmptyInchikeyClassification(inchikey)

            return response_json

        except requests.exceptions.HTTPError as http_error:
            # Sometimes, when the classification fails, instead of returning an empty
            # response, the server returns a 404 error. In this case, we should treat
            # it as an empty classification and raise an EmptyInchikeyClassification
            # exception.
            if http_error.response.status_code in (404, 429, 500):
                if http_error.response.status_code == 429:
                    _sleeping_loading_bar(
                        60,
                        "We got scolded, let's wait a minute.",
                        self._verbose,
                    )

                if self._behavior_on_empty_classification == "warn":
                    warnings.warn(f"Empty classification for InChIKey: {inchikey}")
                elif self._behavior_on_empty_classification == "ignore":
                    pass
                elif self._behavior_on_empty_classification in ("retry-last", "raise"):
                    if self._behavior_on_empty_classification == "retry-last":
                        warnings.warn(
                            f"Empty classification for InChIKey: {inchikey}. "
                            f"Will retry classification at the end of the classification process."
                        )
                    raise EmptyInchikeyClassification(inchikey) from http_error

            raise ClassyFireAPIRequestError(
                f"Classification request for InChIKey '{inchikey}' failed "
                f"with status code {http_error.response.status_code}"
            ) from http_error
        except requests.exceptions.RequestException as e:
            raise ClassyFireAPIRequestError(
                f"Classification request for InChIKey '{inchikey}' failed"
            ) from e

    @typechecked
    def classify_inchikey(self, inchikey: str) -> Compound:
        """Get the classification of a chemical entity.

        Parameters
        ----------
        inchikey : str
            InChIKey of the chemical entity
        """
        return Compound.from_dict(
            self._classify_inchikey(inchikey),
        )

    @typechecked
    def classify_smiles(self, smiles: str) -> Compound:
        """Get the classification of a chemical entity.

        Parameters
        ----------
        smiles : str
            smiles of the chemical entity
        """
        if not is_valid_smiles(smiles):
            raise InvalidSMILES(smiles)

        try:
            return Compound.from_dict(
                self._classify_inchikey(convert_smiles_to_inchikey(smiles)),
            )
        except EmptyInchikeyClassification as empty_inchikey_classification:
            raise EmptySMILESClassification(smiles) from empty_inchikey_classification
        except (
            MultipleRadicalsOrAttachmentPointsNotSupported
        ) as multiple_radicals_or_attachment_points_not_supported:
            raise MultipleRadicalsOrAttachmentPointsNotSupported(
                smiles
            ) from multiple_radicals_or_attachment_points_not_supported
        except ClassyFireAPIRequestError as classyfire_api_request_error:
            raise ClassyFireAPIRequestError(
                f"Classification request for SMILES '{smiles}' failed"
            ) from classyfire_api_request_error

    @typechecked
    def classify_inchikeys(self, inchikeys: Iterable[str]) -> Iterable[Compound]:
        """Get the classification of a list of chemical entities.

        Parameters
        ----------
        inchikeys : Iterable[str]
            InChIKeys of the chemical entities
        """

        to_retry: List[Tuple[str, int]] = []

        for inchikey in tqdm(
            inchikeys,
            desc="Classifying InChIKeys",
            unit="InChIKey",
            leave=False,
            dynamic_ncols=True,
            disable=not self._verbose,
        ):
            try:
                yield self.classify_inchikey(inchikey)
            except EmptyInchikeyClassification as empty_inchikey_classification:
                if self._behavior_on_empty_classification == "retry-last":
                    to_retry.append((inchikey, 1))
                else:
                    raise empty_inchikey_classification

        if self._behavior_on_empty_classification == "retry-last":
            while to_retry:
                _sleeping_loading_bar(
                    self._sleep_between_attempts, "Sleeping before retry", self._verbose
                )
                new_to_retry: List[Tuple[str, int]] = []
                for inchikey, attempt in to_retry:
                    try:
                        yield self.classify_inchikey(inchikey)
                    except EmptyInchikeyClassification as empty_inchikey_classification:
                        if attempt < self._classification_attempts:
                            new_to_retry.append((inchikey, attempt + 1))
                        else:
                            raise empty_inchikey_classification
                to_retry = new_to_retry

    @typechecked
    def classify_smiles_list(self, smiles_list: Iterable[str]) -> Iterable[Compound]:
        """Get the classification of a list of chemical entities.

        Parameters
        ----------
        smiles_list : Iterable[str]
            smiles of the chemical entities
        """

        to_retry: List[Tuple[str, int]] = []

        for smiles in tqdm(
            smiles_list,
            desc="Classifying SMILES",
            unit="SMILES",
            leave=False,
            dynamic_ncols=True,
            disable=not self._verbose,
        ):
            try:
                yield self.classify_smiles(smiles)
            except EmptySMILESClassification as empty_smiles_classification:
                if self._behavior_on_empty_classification == "retry-last":
                    to_retry.append((smiles, 1))
                else:
                    raise empty_smiles_classification

        if self._behavior_on_empty_classification == "retry-last":
            while to_retry:
                _sleeping_loading_bar(
                    self._sleep_between_attempts, "Sleeping before retry", self._verbose
                )
                new_to_retry: List[Tuple[str, int]] = []
                for smiles, attempt in to_retry:
                    try:
                        yield self.classify_smiles(smiles)
                    except EmptySMILESClassification as empty_smiles_classification:
                        if attempt < self._classification_attempts:
                            new_to_retry.append((smiles, attempt + 1))
                        else:
                            raise empty_smiles_classification
                to_retry = new_to_retry

    @typechecked
    def classify_series(self, series: pd.Series) -> Dict[str, Compound]:
        """Classify a pandas Series containing InChIKeys and/or SMILES.

        Parameters
        ----------
        series : pd.Series
            Series containing the InChIKeys of the chemical entities
        """
        return {
            cast(str, column): (
                self.classify_inchikey(candidate_inchikey_or_smiles)
                if is_valid_inchikey(candidate_inchikey_or_smiles)
                else self.classify_smiles(candidate_inchikey_or_smiles)
            )
            for column, candidate_inchikey_or_smiles in series.items()
            if isinstance(candidate_inchikey_or_smiles, str)
            and (
                is_valid_inchikey(candidate_inchikey_or_smiles)
                or is_valid_smiles(candidate_inchikey_or_smiles)
            )
        }

    @typechecked
    def classify_series_list(
        self, series_list: Iterable[pd.Series], total: Optional[int] = None
    ) -> Iterable[Dict[str, Compound]]:
        """Classify a list of pandas Series containing InChIKeys and/or SMILES.

        Parameters
        ----------
        series_list : Iterable[pd.Series]
            List of Series containing the InChIKeys of the chemical entities
        total : Optional[int], optional
            Total number of rows in the Series, by default None
        """

        to_retry: List[Tuple[pd.Series, int]] = []

        for row in tqdm(
            series_list,
            desc="Classifying InChIKeys and/or SMILES",
            unit="row",
            leave=False,
            dynamic_ncols=True,
            total=total,
            disable=not self._verbose,
        ):
            try:
                yield {
                    cast(str, column): (
                        self.classify_inchikey(candidate_inchikey_or_smiles)
                        if is_valid_inchikey(candidate_inchikey_or_smiles)
                        else self.classify_smiles(candidate_inchikey_or_smiles)
                    )
                    for column, candidate_inchikey_or_smiles in row.items()
                    if isinstance(candidate_inchikey_or_smiles, str)
                    and (
                        is_valid_inchikey(candidate_inchikey_or_smiles)
                        or is_valid_smiles(candidate_inchikey_or_smiles)
                    )
                }
            except (
                EmptyInchikeyClassification,
                EmptySMILESClassification,
            ) as empty_classification:
                if self._behavior_on_empty_classification == "retry-last":
                    to_retry.append((row, 1))
                else:
                    raise empty_classification

        if self._behavior_on_empty_classification == "retry-last":
            while to_retry:
                _sleeping_loading_bar(
                    self._sleep_between_attempts, "Sleeping before retry", self._verbose
                )
                new_to_retry: List[Tuple[pd.Series, int]] = []
                for row, attempt in to_retry:
                    try:
                        yield {
                            cast(str, column): (
                                self.classify_inchikey(candidate_inchikey_or_smiles)
                                if is_valid_inchikey(candidate_inchikey_or_smiles)
                                else self.classify_smiles(candidate_inchikey_or_smiles)
                            )
                            for column, candidate_inchikey_or_smiles in row.items()
                            if isinstance(candidate_inchikey_or_smiles, str)
                            and (
                                is_valid_inchikey(candidate_inchikey_or_smiles)
                                or is_valid_smiles(candidate_inchikey_or_smiles)
                            )
                        }
                    except (
                        EmptyInchikeyClassification,
                        EmptySMILESClassification,
                    ) as empty_classification:
                        if attempt < self._classification_attempts:
                            new_to_retry.append((row, attempt + 1))
                        else:
                            raise empty_classification
                to_retry = new_to_retry

    @typechecked
    def classify_csv(
        self,
        csv_path: str,
        sep: str = ",",
        header: bool = True,
        total: Optional[int] = None,
    ) -> Iterable[Dict[str, Compound]]:
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

        inchikeys_to_retry: List[Tuple[str, int]] = []
        smiles_to_retry: List[Tuple[str, int]] = []

        for spectrum in tqdm(
            spectra,
            desc="Classifying Spectra",
            unit="spectrum",
            leave=False,
            dynamic_ncols=True,
            total=total,
            disable=not self._verbose,
        ):
            if "inchikey" in spectrum.metadata:
                try:
                    yield self.classify_inchikey(spectrum.metadata["inchikey"])
                except EmptyInchikeyClassification as empty_inchikey_classification:
                    if self._behavior_on_empty_classification == "retry-last":
                        inchikeys_to_retry.append((spectrum.metadata["inchikey"], 1))
                    else:
                        raise empty_inchikey_classification
            elif "smiles" in spectrum.metadata:
                try:
                    yield self.classify_smiles(spectrum.metadata["smiles"])
                except EmptySMILESClassification as empty_smiles_classification:
                    if self._behavior_on_empty_classification == "retry-last":
                        smiles_to_retry.append((spectrum.metadata["smiles"], 1))
                    else:
                        raise empty_smiles_classification

        if self._behavior_on_empty_classification == "retry-last":
            while inchikeys_to_retry or smiles_to_retry:
                _sleeping_loading_bar(
                    self._sleep_between_attempts, "Sleeping before retry", self._verbose
                )
                new_inchikey_to_retry: List[Tuple[str, int]] = []
                for inchikey, attempt in inchikeys_to_retry:
                    try:
                        yield self.classify_inchikey(inchikey)
                    except EmptyInchikeyClassification as empty_inchikey_classification:
                        if attempt < self._classification_attempts:
                            new_inchikey_to_retry.append((inchikey, attempt + 1))
                        else:
                            raise empty_inchikey_classification
                inchikeys_to_retry = new_inchikey_to_retry

                new_smiles_to_retry: List[Tuple[str, int]] = []
                for smiles, attempt in smiles_to_retry:
                    try:
                        yield self.classify_smiles(smiles)
                    except EmptySMILESClassification as empty_smiles_classification:
                        if attempt < self._classification_attempts:
                            new_smiles_to_retry.append((smiles, attempt + 1))
                        else:
                            raise empty_smiles_classification
                smiles_to_retry = new_smiles_to_retry

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
    def classify_df(self, df: pd.DataFrame) -> Iterable[Dict[str, Compound]]:
        """Classify a pandas DataFrame containing InChIKeys and/or SMILES."""
        return self.classify_series_list((row for _, row in df.iterrows()))
