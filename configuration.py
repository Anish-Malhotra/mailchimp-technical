from dataclasses import dataclass


@dataclass
class IndexConfiguration:
    cluster: str
    index: str
    username: str
    password: str
    filepath: str
    verbose: bool
    exit_on_error: bool