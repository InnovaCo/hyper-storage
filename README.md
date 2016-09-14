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

> Важно понимать, что поскольку коллекция обрабатывается на одной ноде HyperStorage и хранится в одной строке (ROW) в Cassandra, которая идентифицируется первичным ключем. Т.е. она не может масштабироваться и к ней применимы [ограничения для ROW Cassandra](https://docs.datastax.com/en/cql/3.1/cql/cql_reference/refLimits.html).

### Идентификатор элемента коллекции

Каждый элемент коллекции содержит идентификатор, который указывается в пути к нему.
В случае, если путь коллекции равен `abc~`, то его элементы будут иметь пути: `abc~/1`, `abc~/2`, etc. При этом в теле элемента коллекции также всегда содержится поле `id` с значением этого идентификатора. Оно всегда заполняется на стороне  HyperStorage, в соответствии с путем элемента.

Для коллекции поддерживается вставка с использованием POST запроса, который генерирует новый уникальный идентификатор для элемента коллекции и вставляет его в коллекцию. При этом, будет опубликовано событие FEED:PUT (не POST) содержащее необходимое тело.
Уникальные идентификаторы, генерируемые в HyperStorage гарантированно, в рамках одной коллекции возрастают.

### Выполнение запросов к коллекции

При запросе содержимого коллекции через GET, по умолчанию возвращается первые N (100 — настраивается в HyperStorage) записей коллекции. Для того, чтобы запросить больше элементов или сделать постраничный вывод нужно использовать три параметра Query запроса GET:

- `size` — количество запрашиваемых элементов;
- `filter` — выражение фильтра, в виде `id > '10'`;
- `sort` — выражение сортировки, в виде `+a,-b`, что означает сортировать сначала по a, потом по b (descending). + можно опускать, т.е. `sort=+a` эквивалентен `sort=a`.

Элементы по умолчанию отсортированы в порядке возрастания `id`.

Пример запроса в hyperbus, с указанием этих полей:

```scala
hyperbus <~ HyperStorageContentGet("collection-1~",
    body = new QueryBuilder() sortBy Seq(SortBy("id")) add("size", 50) add("filter", "b > 10") result()
)
```

Если-же запрос идет из фасада, то это будет соответственно:
`/hyper-storage/content/collection-1~?size=50&sort=id&filter=b%3E10`

Элементы из коллекции возвращаются согласно спецификации hal+json, пример в JSON:
```json
"_embedded": {
  "els": [
    {
      "appId": "1",
      "name": "RF Online",
      "id": "1",
    },
    {
      "appId": "1006",
      "name": "Lineage2 EU",
      "id": "1006"
    }
  ]
}  
```

### Постраничный вывод коллекции

При отсутствии индексов на коллекции, она может быть эффективно отсортирована только по `id`. Также по `id` можно эффективно применить фильтр на равенство или диапазон. Учитывая это, можно построить постраничный вывод следующим образом:

1. Для первой страницы запрашиваем `collection~?size=50`
2. для последующих страниц запрашиваем `collection~?size=50&id>"abc"`, где `"abc"` — значение `id` последнего элемента предыдущей страницы.

Такой подход к постраничному выводу наиболее точно ложится на то, как коллекция хранится в cassandra.

### Индексы

В случае, если колллекция большая и нам нужны запросы к ней, которые фильтруют и/или сортируют не по значению `id`, то можно создать для нее индекс. 

Пример создания индекса запросом через hyperbus:

```scala
hyperbus <~ HyperStorageIndexPost("abc~", HyperStorageIndexNew(
    indexId = Some("index1"), // название
    sortBy = Seq(HyperStorageIndexSortItem("b", order = Some("asc"), fieldType = Some("decimal"))),  // сортировка
    filterBy = Some("b > 10")) // выражение фильтрации
)
```
в случае, если запрос отправляется через фасад (и это разрешено), то:

`POST /hyper-storage/indexes/abc~`
`Content-Type: application/vnd.hyper-storage-index-new+json`
```json
{
    "indexId":"index1", 
    "sortBy": [{"fieldName":"b","order":"asc","fieldType":"decimal"}], 
    "filterBy": "b > 10"
}
```

> Важно, что в случае создания индекса, путь к коллекции указан в hyperbus как {path}, а не {path:*} и это означает, что если в пути есть более одного сегмента, то его нужно кодировать. Т.е. индекс для ресурса `abc/xyz~` создается с путем `abc%2Fxyz`

Используемые поля запроса имеют следующий смысл:

- `indexId` — уникально идентифицирует индекс в рамках коллекции, если с таким идентификатором индекс уже существует, при выполнении запроса вернется ошибка. Можно опустить значение и тогда будет индентификатор будет сгенерирован системой;
- `sortBy` — сортировка индекса, необходимо указать поля, способ сортировки (текстовый или численный) и направление сортировки (по возрастанию или убыванию); Если `sortBy` не указан, то будет использоваться сортировка по `id` элемента.
- `filterBy` — выражение для фильтрации элементов, если опустить, то в индекс включаются все элементы.

> Индекс содержит полную копию данных из основной таблицы, что может оказаться неприемлимым в случае если тело элемента большого размера

После запроса на создание индекса, HyperStorage начнет индексировать таблицу в фоновом режиме. До момента, пока индексация не завершится, при выполнении запросов индекс не используется. Все изменения которые вносятся в основную таблицу автоматически дублируются в индексную таблицу.

#### Выбор индекса

При каждом запросе, HyperStorage анализирует какой индекс использовать для выполнения запроса. При анализе используется поля `sort` и `filter`, то насколько они соответствуют индексу. Наибольший приоритет имеет индекс, который точно соответствует. Если такого нет, то может быть выбран индекс, который покрывает запрос, примеры:

1. в запросе sort=`a,b`. А индекс построен по `a`.
2. в запросе выражение `a > 10`, а в индексе `a > 5`. Также работает когда `a > 10 and id > "abc"`, а индекс `a > 5`, и в других более сложных случаях, когда *множество параметров выражения запроса входит в множество параметров выражения фильтра*.

Для того, чтобы удовлетворить выражению, HyperStorage может выполнять несколько запросов в casandra и дополнительную фильтрацию и сортировку в соответствии с запросом. В худшем случае, который может быть если сортировка идет по полю, для которого нет индекса, HyperStorage попытается прочитать всю коллекцию, отсортировать и выдать согласно запросу.
В случае большой таблицы, сканирование будет прекращено при чтении более `10000` записей + размер запрошеной таблицы и HyperStorage вернет ошибку. Этим значением можно управлять указывая параметр `skipMax` в запросе.
Также, выборка будет оборвана, если запрос приведет к более чем `20` запросам в cassandra.

# Архитектура

## Шардирование по нодам

Логика шардирования реализована в [ShardProcessor.scala](src/main/scala/eu/inn/hyperstorage/sharding/ShardProcessor.scala).
При запуске ноды HyperStorage она, через Akka Cluster координирует с другими нодами HyperStorage состав и состояние нод HyperStorage. Для общения между собой ноды используют Akka Cluster напрямую (без Hyperbus). Используются следующие состояния:

- `Unknown`
- `Passive` — нода подключилась и может посылать команды другим нодам, но не берет на себя работу;
- `Activating` — нода в процессе активирования, разослала всем информацию и ждет подтверждения об этом;
- `Active` — нода получила от всех подтверждение и активна;
- `Deactivating` — нода в процессе деактивациии и завершает работу;

Задачи, которые посылают друг другу реализуют интерфейс `ShardTask` и содержат строковый параметры `key`, который используется для вычисления ноды, которая ответственна за эту задачу. Это вычисление выполняется используя consistent hashing, чтобы минимизировать перекидывания задач друг-другу при включении и выключении ноды.
Каждая нода, получив задачу, сначала проверяет является-ли она владельцем этой задачи. Владельцами задач могут быть только те ноды, которые находятся в состоянии `Activating` или `Active`. Если нода получила задачу, владельцем которой она не является, она ее форвардит владельцу.

> В Akka Cluster, все ноды HyperStorage должны иметь роль `"hyper-storage"` (параметр akka.cluster.roles)

### Активация

При активации нода ждет подтерждения от всех остальных в кластере, что ей можно начинать работу. При этом, поскольку она уже стала владельцем ноды, ей сразу начнут форвардить задачи. Эти задачи откладываются до тех пор, пока нода не получит подтверждения от всех.

### Деактивация

При деактивации нода завершает те задачи, которые уже начаты и пересылает новому владельцу задачи, которые еще не начаты. Новый владелец задач (в статусе `Active` или `Activating` не начнет над ними работу, пока деактивируемая нода не покинет кластер.

### Исполнители задач в кластере

Сам `ShardProcessor` не выполняет задачи, а пересылает их дочерним акторам. При выполнении каждой задачи создается новый дочерний актор. Задачи могут выполняться акторами разного типа, поэтому `ShardProcessor` поддерживает группы акторов используая параметр `ShardTask.group`.

`ShardProcessor` гарантирует, что если с некоторым `key` и `group` задача находится уже в работе у дочернего актора, то он будет ожидать и создаст новый актор только при завершении текущей работы.

Цель всего этого — **гарантировать, что задачи идентифицируемые ключем будут выполняться только на конкретной ноде, конкретным актором**, это разрешает работу с Cassandra без `paxos` и поддержку версионирования.

На текущий момент имеются два вида исполнителей в HyperStorage:

1. `PrimaryWorker` — основной обработчик, принимает запросы на изменение данных (PUT,PATCH,POST,DELETE).
2. `SecondaryWorker` — вторичный (фоновый) обработчик, который:
    - завершает выполнение транзакции по изменению данных, что включает в себя: индексацию, публикацию событий в Kafka (задачи `BackgroundContentTask`);
    - выполняет команды `IndexDefTask` по созданию и удалению индекса и делегирует контроль над этим в `IndexManager`;
    - индексирует контент по запросу от `IndexManager`, задачи `IndexContentTask`. Это происходит при создании индекса для уже существующих элементов коллекции;

## Схема хранения данных
## Обработка транзакций
### Eventual consistency
## Индексирование


# TODO

  * collection item id validation!
  * history period support
  * remove older transactions
    + (content transactionsList delta updates)?
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
