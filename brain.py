import time
import requests

from urllib.parse import urljoin

WQB_API = "https://api.worldquantbrain.com/"
RETRY_TIMES = 3


class BrainError(Exception):
    def __str__(self):
        return "brain base error."


class NetworkError(BrainError):
    def __init__(self, err: Exception):
        self.inner = err

    def __str__(self):
        return "network error: {}".format(self.inner)


class AuthenticationError(BrainError):
    def __str__(self):
        return "API authentication fail."


class Client:
    def __init__(self, user, password, **kwargs):
        self._user = user
        self._pass = password
        self._session = None
        self._retry_times = kwargs.get("retry_times", RETRY_TIMES)

    def connect(self):
        self._session = requests.Session()

        req = requests.Request(
            method="POST",
            url=urljoin(WQB_API, "authentication"),
            auth=requests.auth.HTTPBasicAuth(self._user, self._pass),
        ).prepare()

        try:
            resp = self._session.send(req)
        except Exception as e:
            raise NetworkError(e)
        else:
            if not resp.ok:
                raise AuthenticationError

    def send(self, req: requests.Request) -> requests.Response:
        if not self._session:
            self.connect()
        return self._send(req, self._retry_times)

    def _send(self, req: requests.Request, retry_times: int = 0) -> requests.Response:
        try:
            s = self._session
            resp = s.send(s.prepare_request(req))
        except Exception as e:
            if retry_times == 0:
                raise NetworkError(e)
        else:
            if resp.status_code == 401:
                self.connect()
                return self._send(req, retry_times=retry_times - 1)
            return resp

    def data_fields(self):
        return DataFields(self)

    def simulation(self):
        return Simulation(self)


class DataField:
    def __init__(self, response_dict: dict):
        self._content = response_dict

    @property
    def id(self) -> str:
        return self._content["id"]

    @property
    def description(self) -> str:
        return self._content["description"]


class DataFieldAPIError(BrainError):
    def __init__(self, resp: requests.Response):
        self.resp = resp

    def __str__(self):
        return "data field api: response code, {}. error: {}".format(
            self.resp.status_code, self.resp.text
        )


class DataFields:
    def __init__(self, cli: Client):
        self._cli = cli
        self._filter = {
            "region": "USA",
            "delay": "1",
            "universe": "TOP3000",
            "instrumentType": "EQUITY",
        }
        self._limit = 200

    def with_filter(
        self,
        universe: str = None,
        delay: int = None,
        region: str = None,
        instrument_type: str = None,
        data_type: str = None,
        dataset_id: str = None,
        chunk_size: int = 50,
    ):
        if universe is not None:
            self._filter["universe"] = universe
        if delay is not None:
            self._filter["delay"] = delay
        if region is not None:
            self._filter["region"] = region
        if instrument_type is not None:
            self._filter["instrumentType"] = instrument_type
        if data_type is not None:
            self._filter["type"] = data_type
        if dataset_id is not None:
            self._filter["dataset.id"] = dataset_id

        self._filter["limit"] = chunk_size
        return self

    def limit(self, limit: int):
        self._limit = limit
        return self

    def search(self, query: str):
        self._filter["search"] = query
        return self

    def iter(self):
        url = urljoin(WQB_API, "data-fields")
        count = 0
        query = self._filter.copy()
        while count < self._limit:
            query["offset"] = count
            req = requests.Request("GET", url, params=query)

            resp = self._cli.send(req)
            if not resp.ok:
                raise DataFieldAPIError

            resp_json = resp.json()
            resp_count = resp_json["count"]

            for item in resp_json["results"]:
                count += 1
                if count > self._limit:
                    break
                yield DataField(item)

            if count == resp_count or count > self._limit:
                break


class SimulationAPIError(BrainError):
    def __init__(self, resp: requests.Response):
        self.resp = resp

    def __str__(self):
        return "simulation api: response code, {}. error: {}".format(
            self.resp.status_code, self.resp.text
        )


class Simulation:
    def __init__(self, cli: Client):
        self._cli = cli
        self._sim = {
            "type": "REGULAR",
            "settings": {
                "instrumentType": "EQUITY",
                "region": "USA",
                "universe": "TOP3000",
                "delay": 1,
                "decay": 6,
                "neutralization": "SUBINDUSTRY",
                "truncation": 0.08,
                "pasteurization": "ON",
                "unitHandling": "VERIFY",
                "nanHandling": "ON",
                "language": "FASTEXPR",
                "visualization": False,
            },
            "regular": "",
        }

    def with_type(self, type: str):
        self._sim["type"] = type
        return self

    def with_settings(
        self,
        instrument_type: str | None = None,
        region: str | None = None,
        universe: str | None = None,
        delay: int | None = None,
        decay: int | None = None,
        neutralization: str | None = None,
        truncation: float | None = None,
        pasteurization: str | None = None,
        unitHandling: str | None = None,
        nanHandling: str | None = None,
        language: str | None = None,
        visualization: bool | None = None,
    ):
        if instrument_type is not None:
            self._sim["settings"]["instrumentType"] = instrument_type

        if region is not None:
            self._sim["settings"]["region"] = region

        if universe is not None:
            self._sim["settings"]["universe"] = universe

        if delay is not None:
            self._sim["settings"]["delay"] = delay

        if decay is not None:
            self._sim["settings"]["decay"] = decay

        if neutralization is not None:
            self._sim["settings"]["neutralization"] = neutralization

        if truncation is not None:
            self._sim["settings"]["truncation"] = truncation

        if pasteurization is not None:
            self._sim["settings"]["pasteurization"] = pasteurization

        if unitHandling is not None:
            self._sim["settings"]["unitHandling"] = unitHandling

        if nanHandling is not None:
            self._sim["settings"]["nanHandling"] = nanHandling

        if language is not None:
            self._sim["settings"]["language"] = language

        if visualization is not None:
            self._sim["settings"]["visualization"] = visualization

        return self

    def with_expr(self, expr: str):
        self._sim["regular"] = expr
        return self

    def send(self):
        req = requests.Request(
            method="POST",
            url=urljoin(WQB_API, "simulations"),
            json=self._sim,
        )

        resp = self._cli.send(req)
        if not resp.ok:
            raise SimulationAPIError(resp)

        return SimulationResult(self._cli, resp.headers["Location"])


class SimulationResultAPIError(BrainError):
    def __init__(self, cause: str):
        self.inner = cause

    def __str__(self):
        return "simulation detail api error: {}".format(self.inner)


class SimulationResult:
    def __init__(self, cli: Client, url: str):
        self._cli = cli
        self._url = url
        self.alpha = None
        self.default_retry_after = 1.0  # default check period
        self.max_fail_times = 3  # max fail times

    def wait(self):
        fail_times = 0
        while True:
            req = requests.Request("GET", self._url)
            resp = self._cli.send(req)

            if not resp.ok:
                fail_times += 1
                if fail_times > self.max_fail_times:
                    raise SimulationResultAPIError(
                        "exceed max retry time when trying to get simulation result."
                    )

                time.sleep(self.default_retry_after * 2**fail_times)
                continue

            if "Retry-After" in resp.headers:
                time.sleep(float(resp.headers["Retry-After"]))
                continue

            # response:
            # {
            #   "id":"3gpq1X1iB4kHaHlnDFZGJMw",
            #   "type":"REGULAR",
            #   "settings":{...},
            #   "regular":"vwap/close",
            #   "status":"COMPLETE",
            #   "alpha":"w2zl935"
            # }
            self.alpha = resp.json()["alpha"]
            return self

    def detail(self):
        if self.alpha is None:
            raise SimulationResultAPIError(
                "wait method should be called before detail method"
            )

        req = requests.Request("GET", urljoin(WQB_API, "alphas", self.alpha_id))
        resp = self._cli.send(req)
        if resp.ok:
            return resp.json()

        raise SimulationResultAPIError(
            "api error response, code: {}, content: {}".format(
                resp.status_code, resp.text
            )
        )
