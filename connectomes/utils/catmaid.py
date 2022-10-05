import logging
import typing as tp

from urllib.parse import urljoin
from requests_futures.sessions import FuturesSession
from requests import Session
import requests
import textwrap
import json

logger = logging.getLogger(__name__)

GET = "GET"
POST = "POST"
DEFAULT_MAX_WORKERS = 10

class WrappedCatmaidException(requests.HTTPError):
    spacer = "    "

    def __init__(self, response, error_data=None):
        """
        Exception wrapping a django error which results in a JSON response being returned containing information
        about that error.
        Parameters
        ----------
        response : requests.Response
            Response containing JSON-formatted error from Django
        """
        super(WrappedCatmaidException, self).__init__(
            "Received HTTP{} from {}".format(response.status_code, response.url),
            response=response,
        )
        if error_data is None:
            error_data = response.json()

        self.error = error_data["error"]
        self.detail = error_data["detail"]
        self.type = error_data["type"]

        self.meta = error_data.get("meta")
        self.info = error_data.get("info")
        self.traceback = error_data.get("traceback")

    def format_detail(self, indent=""):
        return textwrap.indent("Response contained:\n" + self.detail.rstrip(), indent)

    def __str__(self):
        return (
            super(WrappedCatmaidException, self).__str__()
            + "\n"
            + self.format_detail(self.spacer)
        )

    @classmethod
    def raise_for_status(cls, response):
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            if response.headers.get("content-type") == "application/json":
                try:
                    wrapped = cls(response)
                    raise wrapped from e
                except KeyError:
                    pass
            raise e


class CatmaidClient():
    def __init__(
        self,
        server: str,
        api_token: tp.Optional[str] = None,
        http_user: tp.Optional[str] = None,
        http_password: tp.Optional[str] = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
    ):
        self.session = Session()
        self.max_workers = max_workers
        self.f_session = FuturesSession(session=self.session, max_workers=max_workers)
        self.server = server

        if api_token:
            self.session.headers['X-Authorization'] = 'Token ' + api_token

        if http_user and http_password:
            self.session.auth = (http_user, http_password)
        elif http_user or http_password:
            logger.warning("Only one of http_user or http_password set")

    @classmethod
    def from_json(cls, fpath, max_workers=DEFAULT_MAX_WORKERS):
        with open(fpath) as f:
            creds = json.load(f)
        return cls(
            creds["server"],
            creds.get("api_token"),
            creds.get("http_user"),
            creds.get("http_password"),
            max_workers,
        )

    def request_fut(self, method, url, params_or_data=None, **kwargs):
        whole_url = urljoin(self.server, url)
        kwargs2 = dict(**kwargs)
        if params_or_data is not None:
            if method.upper() == POST:
                key = "data"
            elif method.upper() == GET:
                key = "params"
            else:
                raise ValueError(f"Unknown method '{method}'")
            kwargs2[key] = params_or_data
        return self.f_session.request(method, whole_url, **kwargs2)

    def request(self, method, url, params_or_data=None, *args, **kwargs):
        response = self.request_fut(method, url, params_or_data, *args, **kwargs).result()
        WrappedCatmaidException.raise_for_status(response)
        return response

    def request_many(self, method, url_data: tp.Iterable[tuple[str, tp.Optional[dict[str, tp.Any]]]], **kwargs):
        futs = [self.request_fut(method, url, data, **kwargs) for url, data in url_data]

        for f in futs:
            response = f.result()
            WrappedCatmaidException.raise_for_status(response)
            yield response

    def get(self, url, params=None, **kwargs):
        return self.request(GET, url, params, **kwargs)

    def get_many(self, url_params: tp.Iterable[tuple[str, tp.Optional[dict[str, tp.Any]]]], **kwargs):
        yield from self.request_many(GET, url_params, **kwargs)

    def post(self, url, data=None, **kwargs):
        return self.request(POST, url, data, **kwargs)

    def post_many(self, url_data: tp.Iterable[tuple[str, tp.Optional[dict[str, tp.Any]]]], **kwargs):
        yield from self.request_many(POST, url_data, **kwargs)
