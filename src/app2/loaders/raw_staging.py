from sqlalchemy import Integer, JSON, Text, bindparam, text
from sqlalchemy.engine import Engine

_INSERT_QUERY = (
    text(
        """
        INSERT INTO stg.raw_football_api (endpoint, request_params, http_status, response_json)
        VALUES (:endpoint, :request_params, :http_status, :response_json)
        """
    )
    .bindparams(
        bindparam("endpoint", type_=Text),
        bindparam("request_params", type_=JSON),
        bindparam("http_status", type_=Integer),
        bindparam("response_json", type_=JSON),
    )
)


def load_raw(engine: Engine, endpoint: str, status_code: int, payload, metadata: dict | None = None):
    params = metadata or {}
    with engine.begin() as conn:
        result = conn.execute(
            _INSERT_QUERY,
            {
                "endpoint": endpoint,
                "request_params": params,
                "http_status": status_code,
                "response_json": payload,
            },
        )
    return int(result.rowcount or 0)
