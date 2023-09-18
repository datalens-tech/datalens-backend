from __future__ import annotations

import asyncio
from asyncio import AbstractEventLoop
import datetime
import logging
import random
from ssl import SSLContext
import time
from typing import (
    Any,
    ClassVar,
    Optional,
    Tuple,
    Type,
    Union,
)

from aiogoogle import (
    Aiogoogle,
    GoogleAPI,
    HTTPError,
)
from aiogoogle.auth import (
    ApiKey,
    UserCreds,
)
from aiogoogle.auth.creds import ClientCreds
import aiogoogle.excs
import aiogoogle.models
from aiogoogle.models import Response
from aiogoogle.sessions.aiohttp_session import AiohttpSession
from aiohttp import (
    ClientResponse,
    ClientTimeout,
    Fingerprint,
)
from aiohttp.typedefs import StrOrURL
import attr
from yarl import URL

from dl_constants.enums import BIType
from dl_core.aio.web_app_services.gsheets import (
    Cell,
    GSheetsSettings,
    NumberFormatType,
    Range,
    Sheet,
    Spreadsheet,
)
from dl_file_uploader_lib import exc as file_upl_exc
from dl_utils.aio import ContextVarExecutor

LOGGER = logging.getLogger(__name__)


# this is the dt from which all DATE and DATE_TIME values are calculated in gsheets
# https://developers.google.com/sheets/api/guides/formats#about_date_time_values
GSHEETS_EPOCH = datetime.datetime(
    year=1899,
    month=12,
    day=30,
)
URL_SAFE_CHARS = ""  # we want to encode all symbols with no exceptions


def google_api_error_to_file_uploader_exception(err: HTTPError) -> file_upl_exc.DLFileUploaderBaseError:
    if not isinstance(err.res, Response):
        LOGGER.warning(f"Unknown aiogoogle http error: {err}")
        return file_upl_exc.DLFileUploaderBaseError(orig=err)

    orig_error_obj: dict[str, Any] = err.res.json.get("error", {})
    if isinstance(orig_error_obj, dict):
        orig_status_code = orig_error_obj.get("code", -1)
        orig_reason = orig_error_obj.get("status")
    else:
        orig_status_code = err.res.status_code
        orig_reason = None

    err_cls: Type[file_upl_exc.DLFileUploaderBaseError]
    if orig_status_code == 403:
        err_cls = file_upl_exc.PermissionDenied
    elif orig_status_code == 404:
        err_cls = file_upl_exc.DocumentNotFound
    elif orig_status_code == 400 and orig_reason == "FAILED_PRECONDITION":
        err_cls = file_upl_exc.UnsupportedDocument
    elif orig_status_code >= 500:
        err_cls = file_upl_exc.RemoteServerError
    else:
        LOGGER.warning(f"Unknown aiogoogle http error: {err}")
        err_cls = file_upl_exc.DLFileUploaderBaseError

    return err_cls(
        orig=err,
        debug_info=err.res.json,
        details=dict(
            orig_error=orig_error_obj,
        ),
    )


@attr.s
class GSheetsOAuth2:
    access_token: Optional[str] = attr.ib(repr=False)
    refresh_token: str = attr.ib(repr=False)


class AiohttpGSheetsSession(AiohttpSession):
    def __init__(
        self,
        *args: Any,
        proxy: Optional[StrOrURL] = None,
        proxy_headers: Optional[dict[str, str]] = None,
        ssl: Optional[Union[SSLContext, bool, Fingerprint]] = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.proxy = proxy
        self.proxy_headers = proxy_headers
        self.ssl = ssl
        self.last_response_size_bytes = -1

    async def _request(
        self,
        method: str,
        str_or_url: StrOrURL,
        **kwargs: Any,
    ) -> ClientResponse:
        url = URL(str_or_url)
        headers = kwargs.pop("headers", {})
        if "key" in url.query:
            query_params = {**url.query}
            key = query_params.pop("key")
            url = url.with_query(query_params)
            headers["X-goog-api-key"] = key

        LOGGER.info(f"Sending request: {method} {url}")
        start_t = time.monotonic()
        resp = await super()._request(
            method,
            url,
            allow_redirects=False,
            proxy=self.proxy,
            proxy_headers=self.proxy_headers,
            ssl=self.ssl,
            headers=headers,
            **kwargs,
        )
        elapsed = time.monotonic() - start_t
        resp_text = await resp.read()
        self.last_response_size_bytes = len(resp_text)
        LOGGER.info(
            f"Received response: {method} {url},"
            f" status_code: {resp.status},"
            f" elapsed: {elapsed:.6f} s,"
            f" payload_size: {self.last_response_size_bytes} bytes"
        )
        return resp


def make_type(value: Any, user_type: BIType | str) -> Any:
    if value is None or value == "":
        return None
    if user_type == BIType.integer:
        if isinstance(value, str):  # overflow
            return None
        return int(value)
    if user_type == BIType.float:
        if isinstance(value, str):  # overflow
            return None
        return float(value)
    if user_type == BIType.boolean:
        return bool(value)
    if user_type in (BIType.date, BIType.genericdatetime, BIType.datetime, BIType.datetimetz):
        actual_dt = GSHEETS_EPOCH + datetime.timedelta(days=value)
        if user_type == BIType.date:
            dt_str = actual_dt.strftime("%Y-%m-%d")
        else:
            dt_str = actual_dt.strftime("%Y-%m-%d %H:%M:%S")
        year, *rest = dt_str.split("-")
        return f"{year:>04}-" + "-".join(rest)  # years <1000 are not padded properly in strftime
    if user_type == "time":
        time_value = datetime.timedelta(days=value)
        hours, remainder = divmod(time_value.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
    if user_type == BIType.string:
        return str(value)

    raise ValueError(f"Type {user_type} is not supported here")


@attr.s
class GSheetsClient:
    settings: GSheetsSettings = attr.ib()
    _tpe: ContextVarExecutor = attr.ib()
    _loop: AbstractEventLoop = attr.ib(init=False, factory=asyncio.get_event_loop)
    auth: Optional[GSheetsOAuth2] = attr.ib(default=None)

    session_timeout: ClassVar[ClientTimeout] = ClientTimeout(total=180.0)

    last_request_size_bytes: int = attr.ib(init=False, default=-1)
    _aiogoogle: Optional[Aiogoogle] = attr.ib(init=False, default=None)
    _sheets_api: Optional[GoogleAPI] = attr.ib(init=False, default=None)

    async def __aenter__(self: GSheetsClient) -> GSheetsClient:
        self._aiogoogle = Aiogoogle(
            session_factory=lambda: AiohttpGSheetsSession(
                timeout=self.session_timeout,
                headers={
                    "user-agent": "DataLens",
                    "Accept": "application/json",
                },
            ),
            api_key=ApiKey(self.settings.api_key),
            client_creds=ClientCreds(
                client_id=self.settings.client_id,
                client_secret=self.settings.client_secret,
            ),
        )
        await self._aiogoogle.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        assert self._aiogoogle, "Aiogoogle is not initialized for the client"
        await self._aiogoogle.__aexit__(*args)

    def _process_values(
        self, raw_values: list[list[Any]], user_types: list[BIType | str]
    ) -> Tuple[list[list[Any]], list[BIType | str]]:
        """
        Tries to convert values to passed BITypes and falls back to string when fails to do so
        But the fallback happens only on return, i.e. it tries to convert all values to the original passed type
        Values are processed inplace, types are copied
        """

        new_user_types = user_types.copy()
        for row_idx, row in enumerate(raw_values):
            width_diff = len(user_types) - len(row)
            if width_diff > 0:
                row.extend([None] * width_diff)
            else:
                row = row[: len(user_types)]
            for col_idx, (value, user_type) in enumerate(zip(row, user_types)):
                try:
                    raw_values[row_idx][col_idx] = make_type(value, user_type)
                except (ValueError, TypeError):
                    new_user_types[col_idx] = BIType.string
                    raw_values[row_idx][col_idx] = str(value)
                except OverflowError:
                    raw_values[row_idx][col_idx] = None
        return raw_values, new_user_types

    def _get_cell_data(self, value: dict[str, Any]) -> Cell:
        top_level_value_keys = ("userEnteredValue", "effectiveValue", "formattedValue")
        if not value or not any(value_key in value for value_key in top_level_value_keys):
            return Cell(
                value=None,
                number_format_type=NumberFormatType.NUMBER_FORMAT_TYPE_UNSPECIFIED,
                empty=True,
            )
        number_format = NumberFormatType(
            value.get("effectiveFormat", {}).get(
                "numberFormat", {"type": NumberFormatType.NUMBER_FORMAT_TYPE_UNSPECIFIED}
            )["type"]
        )

        extended_value_key = (
            "formattedValue"
            if number_format
            in (
                NumberFormatType.TIME,
                NumberFormatType.NUMBER_FORMAT_TYPE_UNSPECIFIED,
                NumberFormatType.TEXT,
            )
            else "effectiveValue"
        )
        if "effectiveValue" in value and "boolValue" in value["effectiveValue"]:
            # bool values may be marked with any number format
            extended_value = value["effectiveValue"]
        else:
            extended_value = value.get(extended_value_key, None)

        if isinstance(extended_value, dict):  # formattedValue is a string
            value_key = list(extended_value.keys())[0]
            actual_value = extended_value[value_key]
            if value_key == "boolValue":
                number_format = NumberFormatType.BOOLEAN
        else:
            actual_value = extended_value

        NUMBER_FORMATS = (
            NumberFormatType.NUMBER,
            NumberFormatType.PERCENT,
            NumberFormatType.CURRENCY,
            NumberFormatType.SCIENTIFIC,
        )

        if number_format in NUMBER_FORMATS:
            if isinstance(actual_value, float) or actual_value is None:
                number_format = NumberFormatType.FLOAT
            elif isinstance(actual_value, int):
                number_format = NumberFormatType.INTEGER
            elif isinstance(actual_value, str):
                try:
                    _ = float(actual_value)
                except ValueError:
                    # not a number
                    number_format = NumberFormatType.TEXT
                else:
                    # if the conversion was successful, this means that the number is too big to be encoded in json
                    actual_value = None
                    number_format = NumberFormatType.FLOAT
            else:
                actual_value = str(actual_value)
                number_format = NumberFormatType.TEXT

        if number_format in (NumberFormatType.DATE, NumberFormatType.DATE_TIME):
            if isinstance(actual_value, (int, float)):
                gsheets_internal_value = actual_value

                try:
                    actual_dt = GSHEETS_EPOCH + datetime.timedelta(days=gsheets_internal_value)
                    if number_format == NumberFormatType.DATE:
                        actual_value = actual_dt.strftime("%Y-%m-%d")
                    elif number_format == NumberFormatType.DATE_TIME:
                        actual_value = actual_dt.strftime("%Y-%m-%d %H:%M:%S")
                except OverflowError:
                    actual_value = None
            else:
                number_format = NumberFormatType.TEXT

        return Cell(
            value=actual_value,
            number_format_type=number_format,
        )

    def _get_sheet_data(self, sheet: dict[str, Any], num_rows: Optional[int] = None) -> list[list[Cell]]:
        data: list[list[Cell]] = []
        rows_read = 0
        for rowdata in sheet["data"][0].get("rowData", []):
            if num_rows is not None and rows_read >= num_rows:
                break
            processed_rowdata = [self._get_cell_data(value) for value in rowdata.get("values", [])]
            actual_data_ends_at = len(processed_rowdata) - 1
            while actual_data_ends_at > 0 and processed_rowdata[actual_data_ends_at].empty:
                actual_data_ends_at -= 1
            processed_rowdata = processed_rowdata[: actual_data_ends_at + 1]
            data.append(processed_rowdata)
            rows_read += 1
        if data:
            n_cols = max(len(row) for row in data)
            for idx, row in enumerate(data):  # filling incomplete rows with NULLs and trimming too wide rows
                diff = n_cols - len(row)
                if diff > 0:
                    data[idx].extend([Cell(value=None, empty=True)] * diff)
                elif diff < 0:
                    data[idx] = row[:n_cols]
        return data

    async def _require_init(self) -> None:
        if self._aiogoogle is None:
            raise ValueError("Aiogoogle is not set up for the client. It must be used as an async context manager.")
        if self._sheets_api is None:
            self._sheets_api = await self._aiogoogle.discover("sheets", "v4")

    def _is_retryable_status(self, status_code: int) -> bool:
        return status_code in (408, 429) or status_code >= 500

    def _raise_retryable(self, err: aiogoogle.excs.HTTPError) -> None:
        """Makes an exception based on the passed error response and raises"""

        err_resp_json: dict[str, Any] = err.res.json or {}
        details: list[dict] = err_resp_json["error"].get("details", [])

        if err.res.status_code == 429:
            err_info: dict[str, Any] = next((item for item in details if item["reason"] == "RATE_LIMIT_EXCEEDED"), {})
            quota_err_cls = {
                "ReadRequestsPerMinutePerProject": file_upl_exc.QuotaExceededReadRequestsPerMinutePerProject,
                "ReadRequestsPerMinutePerUser": file_upl_exc.QuotaExceededReadRequestsPerMinutePerUser,
            }.get(
                err_info.get("metadata", {}).get("quota_limit"),
                file_upl_exc.QuotaExceeded,
            )
            raise quota_err_cls(debug_info=err_resp_json)
        if err.res.status_code >= 500:
            raise file_upl_exc.RemoteServerError(debug_info=err_resp_json)
        raise err

    async def _make_request(self, request: aiogoogle.models.Request) -> dict:
        await self._require_init()
        assert self._aiogoogle is not None

        # exponential backoff
        maximum_backoff = 64  # maximum number of seconds to wait between retries
        deadline = 220  # maximum number of seconds to keep sending requests
        current_backoff_sum = 0
        current_try = 0
        resp_json: Optional[dict] = None
        while True:
            try:
                if self.auth is not None:
                    # Aiogoogle saves user creds on refresh and reuses them
                    # But if we pass incomplete creds smth will go wrong, so it is better not to interfere
                    if self._aiogoogle.user_creds is None:
                        user_creds = UserCreds(
                            access_token=self.auth.access_token, refresh_token=self.auth.refresh_token
                        )
                    else:
                        user_creds = None
                    resp_json = await self._aiogoogle.as_user(request, user_creds=user_creds)  # type: ignore
                else:
                    resp_json = await self._aiogoogle.as_api_key(request)  # type: ignore
                break
            except aiogoogle.excs.HTTPError as err:
                if self._is_retryable_status(err.res.status_code):
                    LOGGER.error(err)
                    if current_backoff_sum > deadline:
                        self._raise_retryable(err)
                    backoff_seconds = min(2**current_try + random.uniform(0, 1), maximum_backoff)
                    current_backoff_sum += backoff_seconds
                    LOGGER.info(
                        f"Got status {err.res.status_code} on try {current_try},"
                        f" going to back off for {backoff_seconds:.3f} seconds"
                    )
                    current_try += 1
                    await asyncio.sleep(backoff_seconds)
                    continue
                raise

        assert isinstance(resp_json, dict), resp_json
        session = self._aiogoogle.session_context.get()
        assert isinstance(session, AiohttpGSheetsSession)
        self.last_response_size_bytes = session.last_response_size_bytes
        return resp_json

    async def _request_spreadsheet(self, spreadsheet_id: str, include_data: bool, range: Optional[str] = None) -> dict:
        await self._require_init()
        assert self._sheets_api is not None

        req_params = dict(
            spreadsheetId=spreadsheet_id,
            includeGridData=include_data,
        )
        if range is not None:
            req_params["ranges"] = range
            req_params["path_params_safe_chars"] = {"ranges": URL_SAFE_CHARS}

        return await self._make_request(self._sheets_api.spreadsheets.get(**req_params))

    async def _request_values(self, spreadsheet_id: str, range: str) -> dict:
        await self._require_init()
        assert self._sheets_api is not None

        req_params = dict(
            spreadsheetId=spreadsheet_id,
            range=range,
            dateTimeRenderOption="SERIAL_NUMBER",
            majorDimension="ROWS",
            valueRenderOption="UNFORMATTED_VALUE",
            path_params_safe_chars={"range": URL_SAFE_CHARS},
        )

        return await self._make_request(self._sheets_api.spreadsheets.values.get(**req_params))

    async def get_spreadsheet(
        self,
        spreadsheet_id: str,
        include_data: bool = True,
        num_rows: Optional[int] = None,
    ) -> Spreadsheet:
        """
        :param spreadsheet_id: ID of the spreadsheet to get, e.g. 1rnUFa7AiSKD5O80IKCvMy2cSZvLU1kRw9dxbtZbDMWc
        :param include_data: whether to include actual data or just save spreadsheet and sheet properties
        :param num_rows: number of rows to get, all rows if None (default), ignored when included_data is False
        :return: Spreadsheet object with properties and data if it is requested
        """

        resp_json = await self._request_spreadsheet(spreadsheet_id=spreadsheet_id, include_data=include_data)

        sheets = []
        for sheet in resp_json.get("sheets", []):
            sheet_properties = sheet["properties"]
            data: Optional[list[list[Cell]]]
            if include_data:
                data = await self._loop.run_in_executor(self._tpe, self._get_sheet_data, sheet, num_rows)
            else:
                data = None
            sheets.append(
                Sheet(
                    id=sheet_properties["sheetId"],
                    index=sheet_properties["index"],
                    title=sheet_properties["title"],
                    row_count=sheet_properties["gridProperties"]["rowCount"],
                    column_count=sheet_properties["gridProperties"]["columnCount"],
                    data=data,
                )
            )
        return Spreadsheet(
            id=resp_json["spreadsheetId"],
            url=resp_json["spreadsheetUrl"],
            title=resp_json["properties"]["title"],
            sheets=sheets,
        )

    async def get_spreadsheet_sample(self, spreadsheet_id: str, sample_rows: Optional[int] = None) -> Spreadsheet:
        """
        :param spreadsheet_id: spreadsheetId
        :param sample_rows: rows to request from every sheet (for values), default is `2600 / column count * 50` (~4 MB in each response, Google recommends 2 MB)
        :return: Spreadsheet instance
        """

        resp_json = await self._request_spreadsheet(spreadsheet_id=spreadsheet_id, include_data=False)

        sheets = []
        for sheet in resp_json.get("sheets", []):
            sheet_properties = sheet["properties"]
            sheets.append(
                Sheet(
                    id=sheet_properties["sheetId"],
                    index=sheet_properties["index"],
                    title=sheet_properties["title"],
                    row_count=sheet_properties["gridProperties"]["rowCount"],
                    column_count=sheet_properties["gridProperties"]["columnCount"],
                    data=None,
                )
            )
        spreadsheet_meta = Spreadsheet(
            id=resp_json["spreadsheetId"],
            url=resp_json["spreadsheetUrl"],
            title=resp_json["properties"]["title"],
            sheets=sheets,
        )

        for idx, sheet in enumerate(spreadsheet_meta.sheets):
            sample_rows_to_request = 2600 // sheet.column_count if sample_rows is None else sample_rows
            sample_range = Range(
                sheet_title=sheet.title,
                row_from=1,
                col_from=1,
                row_to=sample_rows_to_request,
                col_to=sheet.column_count,
            )
            sheet_sample = await self.get_single_range(
                spreadsheet_id=spreadsheet_id,
                range=sample_range,
            )

            sheet_sample.batch_size_rows = sample_rows_to_request * 50  # for values
            sheet_sample.column_count = len(sheet_sample.data[0]) if sheet_sample.data else 0

            spreadsheet_meta.sheets[idx] = sheet_sample

        return spreadsheet_meta

    async def get_single_range(self, spreadsheet_id: str, range: Range) -> Sheet:
        resp_json = await self._request_spreadsheet(spreadsheet_id=spreadsheet_id, include_data=True, range=str(range))

        sheet_json = resp_json["sheets"][0]
        sheet_properties = sheet_json["properties"]
        sheet_data = await self._loop.run_in_executor(self._tpe, self._get_sheet_data, sheet_json)
        sheet = Sheet(
            id=sheet_properties["sheetId"],
            index=sheet_properties["index"],
            title=sheet_properties["title"],
            row_count=sheet_properties["gridProperties"]["rowCount"],
            column_count=sheet_properties["gridProperties"]["columnCount"],
            data=sheet_data,
        )
        return sheet

    async def get_single_values_range(
        self, spreadsheet_id: str, range: Range, user_types: list[BIType | str]
    ) -> Tuple[list[list[Any]], list[BIType | str]]:
        resp_json = await self._request_values(spreadsheet_id=spreadsheet_id, range=str(range))

        raw_values = resp_json.get("values", [])
        values, updated_user_types = await self._loop.run_in_executor(
            self._tpe,
            self._process_values,
            raw_values,
            user_types,
        )

        return values, updated_user_types
