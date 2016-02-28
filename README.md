# revault

```
    {
        "request": {
            "uri": {
                "pattern":"/revault/{path:*}/feed",
                "args": {
                    "path": "about-author"
                }
            },
            "method":"post",
            "contentType":"about-author",
            "messageId":"123",
            "headers": {
                "hyperbus-revision": ["100500"],
                "hyperbus-original-method": "put"
            }
        },
        "body": {
            "_links": {
                "self": { "href": "/revault/about-author" }
            },
            "authorName": "Jack London",
            "books": {
                "1": "The Call of the Wild",
                "2": "The Sea-Wolf",
                "3": "Martin Eden"
            }
        }
    }
```

todo:
  * recovery job:
        1. recovery control actor
            1.1. track cluster data
            1.2. track list of self channels
            1.3. run hot recovery worker on channel
            1.4. run stale recovery worker on channel
            1.5. warns if hot isn't covered within hot period, extend hot period [range]
        2. hot data recovery worker
            2.1. selects data from channel for last (30) minutes
            2.1. sends tasks for recovery, waits each to answer (limit?)
        3. stale data recovery worker
            3.1. selects data from channel starting from last check, until now - 30min (hot level)
            3.2. sends tasks for recovery, waits each to anwser (indefinitely)
            3.3. updates check dates (with replication lag date)
  * /revault/content
    /revault/monitor

  * CREATED/NO_CONTENT instead of ACCEPTED
  * limit stash size
  * url validator and splitter
  * collections
    + partitioning collection events
    + query filter
  * history period support, remove older monitors
    + (content monitorList delta updates)
  * performance test
  * facade JS test
  * revault monitor query
  * cache results
  * integration test
    + kafka + recovery + switch
  * better DI and abstractions
  * split tests (port numbers) workerspec
  * define base classes for a RAML generated classes
  * StringDeserializer -> accept Message
  * EmptyBody without content-type!
    distributed akka, use protobuf: https://github.com/akka/akka/issues/18371
