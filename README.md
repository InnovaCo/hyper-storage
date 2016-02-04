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
