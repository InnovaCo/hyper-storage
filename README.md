# hyper-storage

```
    {
        "request": {
            "uri": {
                "pattern":"/hyper-storage/{path:*}/feed",
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
                "self": { "href": "/hyper-storage/about-author" }
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
  * collections
    + partitioning collection events
    + query filter

    collection item id validation!

  * history period support, remove older transactions
    + (content transactionsList delta updates)
  * performance test
  * facade JS test
  * transactions query (/hyper-storage/transactions/?)
  * cache results
  * integration test
    + kafka + recovery + switch
  * better DI and abstractions
  * split tests (port numbers) workerspec
  * define base classes for a RAML generated classes
    distributed akka, use protobuf: https://github.com/akka/akka/issues/18371
  * paging when deleted items
  * test:
    - deleted collection elements
    - 404 on documents and collections

  * document:
    - stash-size
