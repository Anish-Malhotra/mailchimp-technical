import asyncio
from itertools import islice

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from core.document_models import INDEX_NAME_TO_DOCUMENT_MAPPING, INDEX_NAME_TO_DOCUMENT_TYPE
from configuration import IndexConfiguration


# The queue size is the number of documents that will be indexed using
# Elasticsearch's bulk indexing interface. The input file has ~52K lines which totals
# to approximately 12MB of storage. As such, bulk-indexing 21500 documents is about ~5MB per batch,
# as recommended by Elasticsearch to begin with: https://www.elastic.co/guide/en/elasticsearch/guide/current/indexing-performance.html

# Finally, we also use this queue size as the chunk size (# lines) when reading the input JSON file.
_MAX_QUEUE_SIZE = 21500


async def load_and_index(config: IndexConfiguration, client: Elasticsearch):
    # Exit early if the index name provided is invalid per our mappings in 'document_models.py'
    if not config.index in INDEX_NAME_TO_DOCUMENT_TYPE \
        or config.index not in INDEX_NAME_TO_DOCUMENT_MAPPING:
            raise ValueError(f"Configuration not found for index {config.index}")
    
    # Create the index if it doesn't already exist and we have a mapping for it
    client.indices.create(index=config.index, ignore=400, mappings=INDEX_NAME_TO_DOCUMENT_MAPPING[config.index])
    
    # create a shared queue
    queue = asyncio.Queue(maxsize=_MAX_QUEUE_SIZE)
    
    # start the consumer which reads from the queue
    _ = asyncio.create_task(consumer(config, queue, client))
    
    # start the producer which writes to the queue
    await asyncio.create_task(producer(config, queue))
    
    # wait for all items to be processed and then close the client
    client.close()
    
    
def data_generator(index: str, documents: list):
    docs = [doc.to_json() for doc in documents]
    for doc in docs:
        yield {
            "_index": index,
            "_source": doc
        }
        

# This function is called synchronously by the consumer coroutine, which means that
# when it's triggered, we've already marked the documents as processed and allow our producer to
# continue parsing the JSON file as we bulk index the documents
def index(index: str, documents: list, client: Elasticsearch):
    return bulk(client, data_generator(index, documents), stats_only=True)
            

# Consumes parsed documents from the queue asynch and synchronously triggers a bulk index
async def consumer(config: IndexConfiguration, queue: asyncio.Queue, client: Elasticsearch):
    documents_total = 0
    successful = 0
    while True:
        # Wait for the producer to tell us we're ready to process a batch of documents
        if not queue.full():
            await asyncio.sleep(1)
        else:
            documents = []
            while not queue.empty():
                # Pop the documents from the queue one-by-one and add them to a generator
                # for bulk indexing through Elasticsearch
                document = queue.get_nowait()
                if document:
                    documents.append(document)
                    documents_total += 1
                # Mark that we've processed this document (so the producer knows to continue loading from file)
                queue.task_done()
            try:
                successful += index(config.index, documents, client)[0]
            except Exception as exc:
                if config.exit_on_error:
                    raise exc
                else:
                    if config.verbose:
                        print (f"Encountered exception: {str(exc)}")
            print (f"Processed batch: results {successful}/{documents_total} documents successfully indexed")
                

# Parses the input JSON file in chunks and adds them to a queue for indexing
# Although this is a fixed file, we're treating it as a stream so it should be easily
# extendable to a poller or webhook that processes new records hitting an HTTP endpoint
async def producer(config: IndexConfiguration, queue: asyncio.Queue):    
    with open(config.filepath, 'r') as f:
        while True:
            chunk = list(islice(f, _MAX_QUEUE_SIZE))
            if not chunk:
                # We've reached EOF, so let's exit the coroutine
                break
            for item in chunk:
                # Determines the appropriate document class for the specified index name
                # and then converts the JSON entry to that document type and adds it to the queue
                document = INDEX_NAME_TO_DOCUMENT_TYPE[config.index].from_json(item)
                queue.put_nowait(document)
            while not queue.full():
                # In case we've reached EOF before the queue is full, we add dummy values
                # so that the consumer still gets the signal to process the remaining documents
                queue.put_nowait(None)
            # Block until the consumer has bulk-processed the thus-far loaded documents
            await queue.join()