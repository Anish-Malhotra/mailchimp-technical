Welcome to my attempt at the Data Engineer II take-home project.

Here I've created a CLI script that asynchronously loads a JSON file in chunks to bulk index the relevant documents.

The approach is made generic so that it can be extended to any other document types or data sources (API stream, polling, etc).

Resolving the yellow status for the 'github-events' index was the most challenging part of this assignment for me.

Some research yielded the following to me:
* ```GET /_nodes``` -> reveals we have a total of 6 nodes in the cluster
* ```GET _cluster/settings?pretty``` -> reveals we don't have an allocation policy setup (auto-rebalance, etc)
* ```GET github-events/_settings``` -> reveals we only allow 1 shard per node
* ```GET /_cluster/allocation/explain``` -> Shows all of the unassigned shards are replicas, and the error message is that the replicas are being stored on the same nodes as the primary shards

I increased the number of shards per node to 2 and enabled rebalancing for all indices, and that seemed to solve the issue.

Resources used:
- https://www.datadoghq.com/blog/elasticsearch-unassigned-shards/#reason-3-you-need-to-re-enable-shard-allocation
- https://opster.com/guides/elasticsearch/glossary/elasticsearch-rebalance/
- https://bonsai.io/blog/the-importance-of-shard-math-in-elasticsearch

Potential improvements:
* Run the parse/transform of the input data in a separate thread
* Integrate with the Async versions of the Elasticsearch client
* Improve error messaging/verbosity
* Add unit tests