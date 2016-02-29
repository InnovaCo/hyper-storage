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
            "headers": {
                "method": ["post"],
                "contentType": ["about-author"],
                "messageId": ["123"],
                "revision": ["100500"]
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
  * limit stash size
  * url validator and splitter

  * collections
    + partitioning collection events
    + query filter
  * history period support, remove older transactions
    + (content transactionsList delta updates)
  * performance test
  * facade JS test
  * revault transactions query (/revault/transactions/?)
  * cache results
  * integration test
    + kafka + recovery + switch
  * better DI and abstractions
  * split tests (port numbers) workerspec
  * define base classes for a RAML generated classes
  * StringDeserializer -> accept Message
  * EmptyBody without content-type!
    distributed akka, use protobuf: https://github.com/akka/akka/issues/18371
