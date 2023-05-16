import argparse
import asyncio

from elasticsearch import Elasticsearch

from configuration import IndexConfiguration
from core.runner import load_and_index


def create_client(configuration: IndexConfiguration) -> Elasticsearch:
    return Elasticsearch(
        hosts=[configuration.cluster],
        http_auth=[configuration.username, configuration.password],
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog="index.py",
        usage="%(prog)s [options]",
        description="Indexes NGINX request/response data to a given Elasticsearch cluster/index name",
    )
    
    parser.add_argument('-c', '--cluster', dest="cluster", type=str, required=True, help="Elasticsearch API URL")
    parser.add_argument('-i', '--index', dest="index", type=str, required=True, default='nginx', help="The Elasticsearch index name")
    parser.add_argument('-u', '--username', dest="username", type=str, required=True, help="API username")
    parser.add_argument('-p', '--password', dest="password", type=str, required=True, help="API password")
    parser.add_argument('-f', '--file', dest="filepath", type=str, required=True, default="nginx.json", help="Path to JSON file to index")
    parser.add_argument('-v', '--verbose', dest="verbose", action='store_true', default=False, help="Verbose output")
    parser.add_argument('-e', '--error', dest="exit_on_error", action='store_true', default=False, help="Break on error")
    
    config = IndexConfiguration(**vars(parser.parse_args()))
    
    client = create_client(config)
        
    asyncio.run(load_and_index(config, client))
    