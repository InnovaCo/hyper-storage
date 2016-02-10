# revault

```
    {
        "request": {
            "uri": {
                "pattern":"/revault/{path:*}:events",
                "args": {
                    "path": "about-author"
                }
            },
            "method":"put",
            "contentType":"about-author",
            "messageId":"123"
        },
        "body": {
            "_revision": "100500",
            "authorName": "Jack London",
            "books": {
                "1": "The Call of the Wild",
                "2": "The Sea-Wolf",
                "3": "Martin Eden"
            }
        }
    }
```


1. Check existing resource monitor
2. complete if previous update is not complete
  2.2. test
3. create & insert new monitor
4. update resource
5. send accepted to the client (if any)
6. publish event
  6.1. + revision
  6.2. + :events path
  6.3. + for post request add self link
7. when event is published complete monitor
8. request next task


todo:
  + kafka event generator
    + test
  + event fail test
  + get method
    + test put + get
  * return created/accepted
  * other methods
  * url validator and splitter
  * collections
    + partitioning collection events
  * null patch tests
  * performance test
  * cache results
  * define base classes for a RAML generated classes

