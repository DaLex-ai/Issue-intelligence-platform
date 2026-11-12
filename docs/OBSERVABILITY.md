# Наблюдаемость конвейера

Приложение пишет JSON в stdout. Alloy обнаруживает контейнеры по Docker label `com.docker.compose.project`, собирает только app, batch и worker и передаёт записи в Loki. Docker socket смонтирован read-only. Loki индексирует `service`, `level` и `env`. Поиск по `task_id` и `trace_id` выполняется внутри записей: индексирование этих полей породило бы новый поток почти на каждую задачу.

OpenTelemetry SDK передаёт OTLP/HTTP в Collector. `memory_limiter` стоит перед `batch`, чтобы ограничивать вход при росте памяти до накопления пачки. Collector экспортирует трейсы по OTLP в Tempo и отдаёт метрики для Prometheus. Grafana загружает источники, дашборд и правила из provisioning. Loki и Tempo используют локальные файлы в томах и подходят для стенда без отдельного кластера хранилища. Prometheus также читает штатный плагин RabbitMQ на 15692.

## Запуск

Из каталога проекта, PowerShell. Обычный `docker compose up -d --build` использует приложение и его зависимости. Batch запускается по требованию. Для пустой базы MLflow сначала зарегистрируйте модель по основному README или используйте изолированный сценарий ниже.

| Профиль | Компоненты | Переменные перед запуском |
|---|---|---|
| `logs` | Alloy, Loki | `OTEL_SDK_DISABLED=true` |
| `traces` | Collector, Tempo | `OTEL_SDK_DISABLED=false`, `OTEL_METRICS_EXPORTER=none`, `OTEL_TRACES_EXPORTER=otlp` |
| `metrics` | Collector, Prometheus | `OTEL_SDK_DISABLED=false`, `OTEL_TRACES_EXPORTER=none`, `OTEL_METRICS_EXPORTER=otlp` |
| `observability` | Все компоненты и Grafana | `OTEL_SDK_DISABLED=false`, оба exporter равны `otlp` |

Пример для полного профиля:

```powershell
$env:OTEL_SDK_DISABLED = 'false'
$env:OTEL_TRACES_EXPORTER = 'otlp'
$env:OTEL_METRICS_EXPORTER = 'otlp'
docker compose --profile observability up -d --build
```

Имена, версию, окружение и instance ID можно задать через `OTEL_SERVICE_NAME_*` и `OTEL_RESOURCE_ATTRIBUTES`. По умолчанию ID экземпляра берётся из имени хоста контейнера, поэтому две реплики различимы. URL Collector задаётся через `OTEL_EXPORTER_OTLP_ENDPOINT`, уровень логов — через `LOG_LEVEL`. Без активного Collector можно отключить SDK через `OTEL_SDK_DISABLED=true`. При аварии Collector SDK ограничивает очередь и таймаут отправки, экспорт выполняется фоновыми потоками. Часть телеметрии при длительном отказе теряется. При аварии Loki Alloy повторяет отправку в пределах своего буфера.

## Как читать сигналы

- В Loki Explore запрос `{service=~"app|batch|worker"} | json | task_id="<UUID>"` находит историю задачи. `{service=~"app|batch|worker"} | json | level=~"ERROR|CRITICAL"` на интервале 15 минут показывает ошибки. Для DLQ и shutdown фильтруйте поле `message` по `task_sent_to_dlq` и `graceful_shutdown_.*`.
- В логе откройте derived field **Trace**. В спане Tempo используйте **Logs for this span**. Поиск использует общие `service.name` и `trace_id`.
- `rabbitmq process` продолжает `rabbitmq publish` через W3C `traceparent`. `messaging.queue_wait_seconds` — оценка времени между отправкой и первой доставкой по часам одного хоста. Повторные доставки имеют отдельные process-спаны того же трейса и признак redelivery. `task.attempt` — попытка обработки, а не число доставок. `inference` — дочерний спан с alias и фактической версией модели, контекст передан в поток executor. Успешная обработка заканчивается спаном записи PostgreSQL.
- Отдельная запись batch получает отдельный корневой спан. Backoff и отказ брокера отражаются в дочерних спанах. Исчерпание попыток даёт ERROR и событие `dead_letter`. Некорректный JSON уходит в DLQ сразу. Для демонстрации повторов используется корректная задача с управляемой ошибкой inference в тестовом воркере.
- На стенде `parentbased_always_on` сохраняет все трейсы и уважает решение родителя. Для сотен задач в секунду пример — `parentbased_traceidratio` с долей 0,01 после замера допустимого объёма. Head sampling может пропустить ошибки. Tail sampling в Collector позволяет оставить ошибки и медленные задачи ценой буферизации и передачи всех спанов до решения.

## Метрики и алерты

Для процессов выбран push по OTLP. Batch отправляет показатели раз в секунду и сбрасывает остаток при завершении. Collector оставляет последнее состояние доступным для нескольких scrape. Batch использует одну метрику instance при последовательных запусках, поэтому счётчик сбрасывается, а PromQL применяет `rate`. Параллельные batch-процессы с одинаковым набором атрибутов в этом стенде не поддерживаются. Воркер отмечает `ok` после ACK, `error` при ошибке попытки и `dlq` при отправке в DLQ. Один сбой может увеличить `error` и `dlq`.

Гистограммы inference привязаны к `INFERENCE_DELAY_SECONDS`. При 0,5 с вокруг основной массы измерений есть границы 0,50, 0,525, 0,575 и 0,75 с. У API границы от 5 мс до 10 с. `iip_task_duration_seconds` измеряет успешно обработанные задачи от создания записи в PostgreSQL до ACK, включая ожидание и повторы. При необходимости анализировать SLO для всех задач добавьте к знаменателю окончательные ошибки и отдельно определите допустимую долю отказов. Пример SLI успешных задач с порогом 8 с: отношение `sum(rate(iip_task_duration_seconds_bucket{le="8.0"}[5m]))` к `sum(rate(iip_task_duration_seconds_count[5m]))`. Границу `le` сверяйте с фактическим рядом и настройкой задержки. RSS берётся из `/proc/self/statm`, это память Python-процесса в Linux-контейнере. Для бюджета компонентов используются отдельные наблюдения Docker Stats.

В режиме насыщения сравнивайте `sum(rate(iip_source_published_total[30s]))` и `sum(rate(iip_worker_tasks_total{result="ok"}[30s]))`. Успешные результаты проверяются по PostgreSQL. Для разреза используйте `sum by (worker) (rate(iip_worker_tasks_total{result="ok"}[30s]))`. Квантили считаются после объединения бакетов, через `histogram_quantile(..., sum by (le) (rate(..._bucket[1m])))`.

Оценка порядка числа рядов приложения для двух воркеров, пяти групп маршрутов и семи HTTP-кодов:

| Группа | Оценка |
|---|---:|
| Запросы API | 5 маршрутов × 7 кодов = 35 |
| Гистограмма API | 5 × (11 границ + Inf + sum + count) = 70 |
| Inference и полный цикл | 2 × (10 + 3) + 2 × (8 + 3) = 48 |
| Результаты, in flight и redelivery | 2 × (3 + 1 + 1) = 10 |
| Источник, RSS, лимит | 3 + 4 + 1 = 8 |
| **Оценка приложения** | **171** |

Это оценка живых рядов без собственных метрик Prometheus и RabbitMQ. Метка worker ограничена числом реплик в момент измерения. При замене контейнеров старые ряды остаются до retention. Добавление task_id к одному счётчику создаст до 6000 рядов на прогон 6000 уникальных задач вместо одного ряда. Для гистограммы число умножается на количество бакетов и агрегатов.

Алерт очереди срабатывает при `ready / x-max-length > 0.9` в течение 1 минуты, что отличает устойчивый затор от короткого всплеска. Для лимита 20 нужны 19 или 20 сообщений. DLQ должна быть непустой 10 секунд, чтобы отбросить кратковременное появление сообщения и при этом быстро показать устойчивый отказ. Grafana проверяет правила каждые 10 секунд.

| Компонент | Лимит, MiB |
|---|---:|
| Alloy | 128 |
| Loki | 320 |
| Tempo | 256 |
| Collector | 160 |
| Prometheus | 256 |
| Grafana | 288 |
| **Итого** | **1408** |

Retention логов, трейсов и метрик — 48 часов. Docker Stats измеряет потребление контейнеров, а не только heap. В нагрузочном скрипте бюджет проверяется суммой синхронных наблюдений шести контейнеров.

## Изолированный сценарий защиты

Все следующие команды используют отдельный проект `iip-observe-qa`. Удаление томов применяется только к этому тестовому проекту. Тестовая модель обучается на синтетическом наборе и регистрируется в его собственном MLflow. QA override включает контролируемую ошибку для строки `iip-demo-poison`.

```powershell
cd E:\Issue-intelligence-platform
$env:OTEL_SDK_DISABLED = 'false'
$env:OTEL_TRACES_EXPORTER = 'otlp'
$env:OTEL_METRICS_EXPORTER = 'otlp'
function qa {
    docker compose -p iip-observe-qa -f docker-compose.yml -f observability/qa.compose.yaml --profile observability @args
}
qa down -v
qa up -d --build --wait app grafana alloy loki tempo otel-collector prometheus
qa run --rm -T --no-deps -e IIP_QA_SEED=true -e MLFLOW_TRACKING_URI=http://mlflow:5000 batch python /demo/seed_model.py
qa up -d --no-build --wait worker
```

Откройте дашборд http://127.0.0.1:13000/d/iip-pipeline. Логин `admin`, пароль из `GRAFANA_ADMIN_PASSWORD` или локальное значение из `.env.example`. API — http://127.0.0.1:18080/docs. Запускать проверки из корня проекта:

```powershell
.\.venv\Scripts\python.exe observability/demo/check.py task
.\.venv\Scripts\python.exe observability/demo/check.py poison
.\.venv\Scripts\python.exe observability/demo/load.py
.\.venv\Scripts\python.exe observability/demo/burst.py
```

Первый сценарий печатает task_id, trace_id, очередь и длительность inference. Второй показывает три попытки и DLQ. Третий генерирует новые тестовые JSONL, сравнивает один и два воркера и сохраняет замеры в `results/observability`. Для полного файла можно указать `--tasks 6000`. Четвёртый запускайте после poison и после дренирования предыдущей нагрузки. Он приостанавливает тестовых воркеров, заполняет очередь, подтверждает 503 с ERROR в Tempo и дожидается двух алертов в Firing, затем возвращает воркеры и проверяет обработку принятых задач.

Для проверки деградации `qa stop otel-collector loki`, затем `.\.venv\Scripts\python.exe observability/demo/check.py basic` и `qa start otel-collector loki`. Для проверки SIGTERM запустите batch и остановите одну реплику через `docker stop --time 75 iip-observe-qa-worker-2`. Сопоставьте `graceful_shutdown_started`, `task_completed`, `graceful_shutdown_completed` и проверьте состояние принятой задачи в БД. После завершения `qa stop` сохраняет тестовые данные.

## Воспроизведение train и test

В MLflow Run откройте Dataset Inputs с контекстами `training` и `evaluation`, затем скачайте `data/split-manifest.json`. Загрузите указанный CSV и примените `prepare_dataset`. Сверьте SHA-256 результата `prepared[["text", "Priority"]].to_json(orient="records", force_ascii=False).encode()`. Выберите `prepared.iloc[manifest["train_indices"]]` и `prepared.iloc[manifest["test_indices"]]`. Порядок позиций в манифесте соответствует порядку, переданному модели. Для локального CSV можно задать `IIP_DATASET_PATH`, а сервер MLflow — через `MLFLOW_TRACKING_URI`.

## Замеры на тестовом стенде

По 420 задач на сценарий, очередь до 20, задержка inference 0,5 с, два слота обработки на воркер. Медианы PromQL `rate(...[30s])` по замерам после 35 с при `ready >= 18`.

| Воркеры | Источник, задач/с | Воркеры, задач/с | Макс. ready | Успешно в PostgreSQL | Пик стека, MiB |
|---:|---:|---:|---:|---:|---:|
| 1 | 3,79 | 3,79 | 20 | 420 | 362,3 |
| 2 | 7,61 | 7,57 | 20 | 420 | 379,0 |

Отношение пропускной способности — 2,00× на этой нагрузке. Исходные срезы, включая Docker Stats каждого компонента и PromQL по репликам, находятся в `results/observability/load-20261106T162335Z.json`. В обычном прогоне alert давления дошёл до Pending. Для Firing используется сценарий с удержанием заполненной очереди.

## Проверки по критериям

| Критерий | Где реализован | Результат проверки |
|---|---|---|
| 1.1–1.4 Логирование | `core/logging.py`, обработчики, Alloy и Loki | Worker stdout разбирается как JSON. Сырой запрос Loki вернул только `env`, `level`, `service` в индексных лейблах. Записи app и worker найдены по одному task_id, backoff найден в batch. |
| 2.1–2.5 Трейсинг | `core/telemetry.py`, API, publisher, worker, Collector | Один trace ID включает POST, publish, process, inference и запись PostgreSQL. Проверен parentSpanId inference. Два worker instance ID наблюдались в Tempo. Poison прошёл три попытки и DLQ. У HTTP 503 подтверждён ERROR. |
| 3.1–3.3 Метрики | `core/telemetry.py`, RabbitMQ Prometheus plugin, `observability/prometheus.yaml` | Очередь достигла 20, PostgreSQL подтвердил 420/420 в каждом запуске. Медианы 3,79 и 7,57 задач/с. Замеры памяти и per-worker PromQL сохранены. |
| 4.1–4.4 Grafana | `observability/grafana` | Два запуска Grafana с новым томом дали 11 панелей, 3 datasource и 2 правила. Запросы панелей приняты Loki и Prometheus. Настройки переходов проверены через Grafana API, корреляция данных через Loki и Tempo. Клики в UI отдельно не автоматизировались. Оба алерта перешли в Firing. |
| 5.1–5.2 Compose | `docker-compose.yml`, `observability/compose.yaml` | Проверены конфигурации всех профилей. Задача завершилась при остановленных Collector и Loki. Без профилей с выключенным SDK задача также завершилась. Лимит 1408 MiB, измеренный пик 379 MiB. |
| Согласованное train/test | `ml/data.py`, `ml/train.py` | В тестовом MLflow run `8107a349a37a4b00bf01faba592ca084` скачан манифест, проверены хеш, два Dataset Inputs, непересекающиеся 32 train и 8 test. Тестовая версия модели 1 загружена воркерами. |

Переполнение и восстановление сохранены в `results/observability/burst-20261106T162839Z.json`: 20 принятых задач, 8 ответов 503 и 20 успешных результатов после возобновления. `results/observability/shutdown-and-degradation.json` содержит две задачи, завершённые после начала SIGTERM, итог 96/96, контрольные задачи при отключении телеметрии и проверку после её возврата. Пример итогового трейса `873e351ee80e6e8f97902eba8590690d`: ожидание около 5,65 мс, inference 553,72 мс. История в живых хранилищах доступна в пределах retention.

Финальные проверки: 98 тестов `pytest -q -W error`, Ruff check и format. Выполнены на Windows с Docker Desktop в отдельном Compose-проекте. Нагрузочная модель была синтетической, показатели качества обучения на ней не интерпретируются. Нагрузка 6000 поддерживается скриптом, измерения выше относятся к 420 задачам на конфигурацию.

Затронуты `core/telemetry.py` и `core/logging.py`, API и `main.py`, publisher, batch, worker и store, обучение и загрузка модели, зависимости, Compose и `.env.example`, парсер событий в `scripts/experiment.py`, настройки и скрипты в `observability/`. Пользовательские тома основного проекта не сбрасывались.

## Короткие ответы для защиты

- Мониторинг отвечает на заранее заданные вопросы по сигналам. Наблюдаемость позволяет расследовать заранее неизвестную причину по общему контексту. Метрики агрегируются по ограниченному числу рядов, логи растут с количеством записей, трейсы — с количеством выбранных задач и спанов.
- При pull Prometheus отличает провал scrape через `up`. При push отсутствие новых данных само по себе не доказывает падение источника, нужны срок актуальности и отдельный сигнал о завершении. Пакетный процесс на стенде отправляет финальные значения перед выходом. [Prometheus о push для batch](https://prometheus.io/docs/practices/pushing/)
- `traceparent` передаёт версию формата, trace ID, ID родительского спана и флаги. Parent задаёт одну причинную иерархию. Span link ссылается на связанный контекст без включения в его дерево, например при обработке группы сообщений из разных трейсов.
- Counter растёт до сброса процесса, gauge может уменьшаться, histogram хранит счётчики по границам. Поэтому квантили вычисляются по суммарным бакетам, а не средним локальных p95. Широкий бакет даёт грубую интерполяцию.
- Для SLO 99% error budget составляет 1% допустимых нарушений в окне. Сначала зафиксируйте знаменатель и учёт ошибок, затем измерьте долю задач, уложившихся в порог. Заполнение очереди и рост DLQ — наблюдаемые симптомы. `for` требует устойчивости условия и уменьшает дребезг.
- Loki индексирует лейблы потоков и ищет текст внутри отобранных записей. VictoriaLogs индексирует содержимое полей и отдельно определяет поля потока. Elasticsearch индексирует текст и/или точные значения по mapping. При 1 ТБ/сутки выбор зависит от доли полнотекстовых запросов, срока хранения и бюджета индекса. Для стенда запросы с узким диапазоном времени и фильтром сервиса подходят Loki. [VictoriaLogs](https://docs.victoriametrics.com/victorialogs/keyconcepts/) · [Elasticsearch](https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/keyword)
- Если система телеметрии замедлилась, bounded queues и таймауты ограничивают её влияние на задачи. При переполнении часть спанов и метрик может потеряться. Sentry группирует события ошибок в issues с повторяемостью, контекстом и состоянием расследования. Строка лога остаётся отдельным наблюдением.
