# HyperStorage, реализация REST сервиса хранящего данные для hyperbus

HyperStorage использует cassandra для оперативного хранения и kafka для публикации событий об изменениях данных. Основные возможности HyperStorage:

- Сохранение и чтение документов в хранилище;
- Работа с коллекциями;
- Поддержка публикации событий при изменениях данных;
- поддержка GET,PUT,DELETE,PATCH;
- POST для коллекций.

## Подключение RAML спецификации к своему проекту

Интерфейс сервиса описан в файле [hyperstorage.raml](src/main/resources/hyperstorage.raml)
Для того, чтобы работать с HyperStorage используя типизированые классы, нужно подключить RAML файл содержащий спецификацию hyper-storage используя hyperbus плагин. Для этого, в `projects/plugin.sbt` добавить:

```scala
addSbtPlugin("eu.inn" % "hyperbus-sbt-plugin" % "0.1.82")

resolvers ++= Seq("Innova plugins" at "http://repproxy.srv.inn.ru/artifactory/plugins-release-local")
```

И в build.sbt проекта добавить:
```scala
ramlHyperbusSource := file("hyperstorage.raml")

ramlHyperbusPackageName := "eu.inn.hyperstorage.api"
```
В качестве RAML файла может быть общий с другими сервисами файл, но он должен включать спецификацию для HyperStorage.

## Документы

HyperStorage поддерживает работу с контентом в хранилище через URI: `/hyper-storage/content/{path:*}`

> path:* в hyperbus позволяет работать с путем, который содержит множество сегментов. Т.е. path может быть равен abc/xyz/kkk

Для документов поддерживается операции GET, PUT, PATCH и DELETE. В тело документа можно положить любой JSON объект, его схема не ограничена чем-либо.

> *Важно*, чтобы в URI/path документов, отсутсвовала тильда ~ в конце последнего/предпоследнего сегмента, это признак коллекции.

### PUT — вставка/замена документа

В этом примере посылается типизированый запрос `HyperStorageContentPut` для вставки документа.

```scala
hyperbus <~ HyperStorageContentPut("abc/123", DynamicBody(ObjV("a" → 10, "x" → "hello"))) map {
    case Ok(body) ⇒ println("abc/123 is updated")
    case Created(body) ⇒ println("abc/123 is created")
} recover {
    case hyperbusError: ErrorResponse ⇒ println(hyperbusError.body.code)
    case otherException ⇒ println("something wrong")
}
```

В теле посылается `Obj` из библиотеки binders. В примере выше он будет сериализован в JSON объект `{"a":10, "x":"hello"}`
В случае отсутствия объекта по указанному пути, он создается. Если уже существует, заменяется целиком.
Важно понимать, что при сохранении документов в HyperStorage поля объекта JSON с значением null удаляются.

### GET — чтение документа

```scala
hyperbus <~ HyperStorageContentGet("abc/123") map {
    case Ok(body, _) ⇒ println("abc/123 is fetched:", body.content)
} recover {
    case NotFound(body) ⇒ println("abc/123 is not found")
    case hyperbusError: ErrorResponse ⇒ println(hyperbusError.body.code)
    case otherException ⇒ println("something wrong")
}
```

В body.content содержится тот-же самый объект `Obj`/`Value` из binders.

### DELETE — удаление документа

```scala
hyperbus <~ HyperStorageContentDelete("abc/123") map {
    case Ok(body) ⇒ println("abc/123 is deleted.")
} recover {
    case NotFound(body) ⇒ println("abc/123 is not found")
    case hyperbusError: ErrorResponse ⇒ println(hyperbusError.body.code)
    case otherException ⇒ println("something wrong")
}
```      

### PATCH — изменение документа

Все выглядит точно также, как и для PUT. Разница в том, что нужно посылать `HyperStorageContentPatch`. При этом:

- PATCH можно применить только к уже существующему документу;
- результатом выполнение PATCH является объединение полей уже существующего с PATCH телом из запроса;
- для удаления полей в уже существующем документе, нужно указать их в теле запроса с значением null
- При сохранении документов в HyperStorage поля объекта с значением null удаляются

### Версионирование документов

У каждого документа имеется версия и HyperStorage гарантирует, что он будет последовательно (без пропусков) инкрементиться при изменении документа. Эта версия возвращается для GET запроса в заголовке `revision` из hyperbus.

### События об изменении документов

При изменении документов, для каждого из запросов PUT,PATCH,DELETE публикуется событие с тем-же самым URI в hyperbus. При этом метод меняется на FEED:PUT, FEED:PATCH и FEED:DELETE соответственно. В теле события содержится именно то тело запроса, которое привело к изменению ресурса. Также событие содержит заголовок `revision` соответствующий состоянию документа. Таким образом реализована поддержка надежных фидов для HyperFacade.

## Коллекции

В целом к коллекциям применимо все, что написано про документы. Каждая коллекция это документ со своим `revision` + свои механизмы для изменения и выборки его элементов, приспособленные к большому разммеру коллекции.
Также для коллекций поддерживаются индексы. Для работы с коллекциями используются все те-же самые объекты `HyperStorageContentGet`, `HyperStorageContentPut`, etc, что и с документами.

Коллекция идентифицируется тильдой в конце сегмента, т.е. вот это: `abc~`, `abc/xyz~` — коллекции, а вот это: `abc`, `abc/~xyz` — документы. Если нужно, чтобы URI документа содержал тильду, значит его нужно кодировать каким-либо образом.

### Идентификатор элемента коллекции

Каждый элемент коллекции содержит идентификатор, который указывается в пути к нему.
В случае, если путь коллекции равен `abc~`, то его элементы будут иметь пути: `abc~/1`, `abc~/2`, etc. При этом в теле элемента коллекции также всегда содержится поле `id` с значением этого идентификатора. Оно всегда заполняется на стороне  HyperStorage, в соответствии с путем элемента.

Для коллекции поддерживается вставка с использованием POST запроса, который генерирует новый уникальный идентификатор для элемента коллекции и вставляет его в коллекцию. При этом, будет опубликовано событие FEED:PUT (не POST) содержащее необходимое тело.

### Язык запросов и схема ответов для коллекций



# Архитектура



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
