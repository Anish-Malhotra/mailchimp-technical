from datetime import datetime

from dataclasses import dataclass, field
from dataclasses_json import config, dataclass_json


def _date_decoder(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%d/%b/%Y:%H:%M:%S %z")

def _date_encoder(dt: datetime) -> str:
    return dt.strftime('%Y-%m-%dT%H:%M:%S%z')


@dataclass_json
@dataclass
class NginxLog:
    time: datetime = field(
        metadata=config(
            decoder=_date_decoder,
            encoder=_date_encoder
        )
    )
    remote_ip: str
    remote_user: str
    request: str
    response: int
    bytes: int
    referrer: str
    agent: str
    
    
_NGINX_LOG_MAPPING = {
    "mappings": {
        "properties": {
            "time": {
                "type": "date",
            },
            "remote_ip": {"type": "text"},
            "remote_user": {"type": "text"},
            "request": {"type": "text"},
            "response": {"type": "long"},
            "bytes": {"type": "long"},
            "referrer": {"type": "text"},
            "agent": {"type": "text"},
        }    
    },
}
    
    
INDEX_NAME_TO_DOCUMENT_TYPE = {
    'nginx': NginxLog
}

INDEX_NAME_TO_DOCUMENT_MAPPING = {
    'nginx': _NGINX_LOG_MAPPING
}