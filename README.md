# Issue Intelligence Platform

**Issue Intelligence Platform** — учебно-прикладной сервис для автоматизации triage программных ошибок.

На текущем этапе проект решает задачу **классификации приоритета bug report** по тексту `Summary + Description`, отслеживает ML-эксперименты в MLflow, хранит версии моделей в Model Registry и предоставляет inference через FastAPI.

Текущая MLflow-часть проекта реализует полный путь:

```text
Dataset
↓
EDA
↓
Experiment / Runs
↓
Metrics / Artifacts
↓
Model Registry
↓
Model Version / Alias
↓
FastAPI
↓
POST /process
```

Полноценная работа в real-time из IDE является следующим этапом развития проекта и пока не считается реализованной.

---

## Возможности

Проект умеет:

- загружать и подготавливать данные GitBugs / Apache Cassandra;
- выполнять EDA и сохранять результаты в MLflow;
- регистрировать dataset и связывать его с MLflow Runs;
- обучать baseline и несколько text-classification моделей;
- логировать параметры, метрики и диагностические артефакты;
- сравнивать Runs в MLflow UI;
- регистрировать модели в Model Registry;
- создавать несколько версий одной модели;
- выбирать рабочую модель через alias `champion`;
- при старте FastAPI разрешать alias `champion` через Model Registry и один раз загружать конкретную версию модели;
- выполнять inference через `POST /process`;
- переключать рабочую версию модели без изменения inference-кода.

---

## Архитектура

```text
                       FastAPI
                         │
                         │
                  POST /process
                         │
                         ▼
                 Model in memory
                         ▲
                         │ startup
                         │
                  MLflow Registry
                         │
                   alias champion
                         │
                         ▼
                    Model Version


                    MLflow Server
                   /             \
                  /               \
                 ▼                 ▼
        PostgreSQL             S3-compatible
        Backend Store          Artifact Store
        (mlflow-db)            (Silo)
```

### Используемый стек

| Компонент | Назначение |
|---|---|
| FastAPI | API приложения и `POST /process` |
| scikit-learn | TF-IDF и модели классификации |
| MLflow 3.16.1 | Experiments, Runs, Dataset Tracking, Artifacts, Model Registry |
| PostgreSQL `mlflow-db` | Backend Store MLflow |
| Silo | S3/MinIO-compatible Artifact Store |
| PostgreSQL `db` | БД основного приложения |
| Docker Compose | локальный запуск инфраструктуры |
| uv | управление Python-зависимостями |
| pytest | автоматические тесты |
| Ruff | lint и форматирование |

Локальные интерфейсы:

```text
FastAPI Swagger:  http://localhost:8000/docs
MLflow UI:        http://localhost:5000
Silo Console:     http://localhost:9001
```

---

## Dataset

Для текущей ML-задачи используется dataset **GitBugs**, subset **Apache Cassandra**.

GitBugs содержит реальные bug reports из open-source проектов.

Cassandra subset содержит около **4.6 тыс. issue reports** и включает поля:

```text
Summary
Issue id
Status
Priority
Resolution
Created
Resolved
Description
```

Для обучения используются:

```text
Input:
Summary + Description

Target:
Priority
```

Встречаются классы Priority:

```text
Normal
High
Low
Urgent
```

Cassandra выбран потому, что в этом subset присутствует поле `Priority`.

Рассматривавшийся VS Code subset для этой конкретной задачи не подходит: в используемой версии GitBugs необходимого target там нет.

Источник dataset зафиксирован на конкретном commit GitBugs, а не на `main`, чтобы эксперимент можно было воспроизвести.

---

## Dataset Tracking

Dataset регистрируется через MLflow Dataset API и связывается с соответствующими Runs.

MLflow хранит:

### `source`

Источник данных.

В проекте это URL Cassandra dataset из конкретной версии GitBugs.

### `digest`

Fingerprint содержимого dataset.

Если данные изменятся, digest также изменится.

### `lineage`

Связь между сущностями:

```text
Dataset
↓
Run
↓
Logged Model
↓
Registered Model
↓
Model Version
```

Это позволяет определить, на каких данных была обучена конкретная версия модели.

---

## EDA

EDA выполняется командой:

```powershell
uv run python -m issue_intelligence_platform.ml.eda
```

Анализируются:

- размер исходного dataset;
- количество подготовленных записей;
- пропущенные значения;
- распределение `Priority`;
- длина текстов;
- средняя, медианная и максимальная длина текста.

В MLflow сохраняются:

```text
priority_distribution.png
text_length_distribution.png
missing_values.png
eda_summary.json
```

---

## Эксперименты

MLflow Experiment:

```text
issue-priority-classification
```

Используется одинаковый train/test split:

```text
train: 80%
test:  20%

random_state = 42
stratify = Priority
```

Проведены три основных Run.

### Baseline

```text
DummyClassifier(strategy="most_frequent")
```

Baseline показывает качество самого простого решения без анализа текста.

### Model A

```text
TF-IDF
+
LogisticRegression
```

Используется:

```text
class_weight="balanced"
```

для компенсации дисбаланса классов.

### Model B

```text
TF-IDF
+
LinearSVC
```

Модель использует другой classifier и другой набор параметров TF-IDF.

---

## Оценка качества модели

Основная метрика проекта:

```text
Macro F1
```

Причина — сильный дисбаланс классов `Priority`.

Обычная Accuracy может быть высокой даже у модели, которая почти всегда предсказывает самый частый класс.

Для каждого класса вычисляются:

```text
Precision = TP / (TP + FP)

Recall = TP / (TP + FN)

F1 = 2 × Precision × Recall / (Precision + Recall)
```

После этого:

```text
Macro F1 =
среднее F1 по всем классам
```

Каждый класс получает одинаковый вес независимо от количества объектов.

Дополнительно используются:

- Accuracy;
- Weighted F1;
- Precision Macro;
- Recall Macro;
- Confusion Matrix;
- Classification Report.

### Фактические результаты

| Модель | Macro F1 | Accuracy |
|---|---:|---:|
| DummyClassifier | 0.2410 | 0.9307 |
| TF-IDF + LogisticRegression | **0.3466** | 0.8754 |
| TF-IDF + LinearSVC | 0.2878 | 0.9274 |

DummyClassifier имеет Accuracy около 93%, но Macro F1 всего 0.2410.

Это показывает, почему Accuracy недостаточно при несбалансированных классах.

По основной метрике лучший результат получила:

```text
TF-IDF + LogisticRegression
Macro F1 = 0.3466
```

Поэтому она была выбрана первоначальным кандидатом на `champion`.

---

## MLflow Artifacts

Training Runs сохраняют:

```text
classification_report.json
confusion_matrix.png
model
```

EDA Run сохраняет:

```text
priority_distribution.png
text_length_distribution.png
missing_values.png
eda_summary.json
```

Metadata MLflow хранится в отдельном PostgreSQL Backend Store.

Файлы и модели хранятся в S3-compatible Artifact Store.

---

## Model Registry

Registered Model:

```text
issue-priority-classifier
```

В Registry зарегистрировано несколько Model Versions:

```text
Version 1
Version 2
```

Каждая версия связана с исходным MLflow Run.

Рабочая версия выбирается через alias:

```text
champion
```

Приложение не содержит жёстко заданный номер версии.

При старте FastAPI оно:

```text
читает alias champion
↓
получает конкретную Model Version
↓
фиксирует номер версии
↓
загружает именно эту версию модели
```

Модель загружается по URI вида:

```text
models:/issue-priority-classifier/<VERSION>
```

---

## Inference API

Endpoint:

```http
POST /process
```

Пример запроса:

```json
{
  "summary": "Cassandra node fails during startup",
  "description": "The node cannot start after configuration changes."
}
```

Пример ответа:

```json
{
  "prediction": "Normal",
  "model_name": "issue-priority-classifier",
  "model_version": "2",
  "model_alias": "champion"
}
```

Модель загружается **один раз при startup FastAPI**.

`POST /process` выполняет inference уже загруженной моделью.

---

# Локальный запуск

## Требования

Необходимы:

- Python 3.11;
- uv;
- Docker;
- Docker Compose;
- доступ в интернет при первоначальной загрузке Docker images и dataset.

---

## 1. Клонировать проект

```powershell
git clone https://github.com/DaLex-ai/Issue-intelligence-platform.git
cd Issue-intelligence-platform
```

---

## 2. Создать `.env`

```powershell
Copy-Item .env.example .env
```

При необходимости изменить локальные пароли в `.env`.

Файл `.env` не коммитится.

---

## 3. Установить Python-зависимости

```powershell
uv sync
```

---

## 4. Запустить MLflow-инфраструктуру

На чистом окружении сначала запускается MLflow, потому что FastAPI ожидает уже существующий alias `champion` при startup.

```powershell
docker compose up --build -d mlflow
docker compose ps -a
```

Docker Compose автоматически поднимет необходимые зависимости MLflow:

```text
mlflow
mlflow-db
minio
minio-init
```

MLflow UI:

```text
http://localhost:5000
```

Silo Console:

```text
http://localhost:9001
```

---

## 5. Выполнить EDA

```powershell
uv run python -m issue_intelligence_platform.ml.eda
```

После выполнения в MLflow появится Run `eda` с dataset и artifacts.

---

## 6. Обучить модели

```powershell
uv run python -m issue_intelligence_platform.ml.train
```

Будут созданы Runs:

```text
baseline-dummy
tfidf-logistic-regression
tfidf-linear-svc
```

Команда выведет Run ID моделей.

---

## 7. Зарегистрировать модели

Подставить Run ID Logistic Regression и LinearSVC:

```powershell
uv run python -m issue_intelligence_platform.ml.registry `
  --logistic-run-id <LOGISTIC_RUN_ID> `
  --svc-run-id <SVC_RUN_ID>
```

После этого появятся:

```text
Registered Model:
issue-priority-classifier

Version 1
Version 2

alias:
champion
```

---

## 8. Запустить FastAPI

Теперь alias `champion` существует, поэтому можно запускать приложение:

```powershell
docker compose up --build -d app
docker compose ps -a
```

Swagger:

```text
http://localhost:8000/docs
```

---

## 9. Выполнить inference

PowerShell:

```powershell
$body = @{
    summary = "Cassandra node fails during startup"
    description = "The node cannot start after configuration changes."
} | ConvertTo-Json

irm -Method Post `
    -Uri http://localhost:8000/process `
    -ContentType "application/json" `
    -Body $body
```

Ответ содержит:

```text
prediction
model_name
model_version
model_alias
```

---

## Переключение Model Version

Допустим:

```text
champion → Version 1
```

FastAPI запускается и загружает Version 1.

Если затем в MLflow изменить:

```text
champion → Version 2
```

уже работающий FastAPI продолжит использовать Version 1.

Причина:

```text
FastAPI startup
↓
Version 1 загружена
↓
объект находится в RAM
```

Изменение alias в Registry само по себе не изменяет объект модели в памяти.

Чтобы загрузить новую версию:

```powershell
docker compose restart app
```

При новом startup приложение снова разрешит alias `champion` и загрузит новую Model Version.

Inference-код при этом не меняется.

---

## Почему не обновлять модель перед каждым запросом

Можно было бы перед каждым `/process` проверять:

```text
куда сейчас указывает champion?
```

и при необходимости заново загружать модель.

Но это создаёт проблемы:

- дополнительная latency;
- зависимость каждого inference от MLflow;
- лишние сетевые запросы;
- возможные race conditions;
- разные workers могут одновременно использовать разные модели;
- сложнее rollback;
- загрузка больших моделей может быть дорогой.

Более надёжный production-подход:

```text
новая Model Version
↓
смена alias
↓
запуск новых application instances
↓
загрузка модели
↓
readiness check
↓
переключение traffic
↓
остановка старых instances
```

То есть controlled rolling или blue-green deployment.

---

## Проверка проекта

Lint:

```powershell
uv run ruff check .
```

Format:

```powershell
uv run ruff format --check .
```

Tests:

```powershell
uv run pytest -v --cov=issue_intelligence_platform --cov-report=term-missing
```

Docker:

```powershell
docker compose ps -a
```

Ожидаемые сервисы:

```text
app          healthy
db           healthy
mlflow       healthy
mlflow-db    healthy
minio        healthy
minio-init   Exited (0)
```

`minio-init` — одноразовый сервис создания bucket, поэтому `Exited (0)` является нормальным успешным состоянием.

---

## План развития real-time режима

Сейчас приложение работает по схеме:

```text
HTTP request
↓
POST /process
↓
inference
↓
response
```

Следующий этап Issue Intelligence Platform — интеграция с VS Code.

Планируемая схема:

```text
VS Code Diagnostics / terminal error
↓
сбор технического контекста
↓
backend
↓
поиск релевантных исторических случаев
↓
retrieval / RAG
↓
evidence-grounded recommendation
↓
отображение результата в VS Code
```

Текущая Priority Classification является одной из ML-компонент будущей системы, но не заменяет retrieval/RAG и анализ способов исправления ошибок.

---

## Основные сущности MLflow

```text
Dataset
→ данные для обучения

Experiment
→ группа связанных экспериментов

Run
→ одна конкретная попытка обучения

Parameters
→ настройки Run

Metrics
→ числовые показатели качества

Artifacts
→ файлы, графики, отчёты, модели

Logged Model
→ модель, сохранённая внутри Run

Registered Model
→ логическое имя модели в Registry

Model Version
→ конкретная версия Registered Model

Alias
→ удобное имя рабочей версии, например champion
```

Полная цепочка проекта:

```text
GitBugs Cassandra
↓
MLflow Dataset
↓
Experiment
↓
Run
↓
Metrics / Artifacts
↓
Logged Model
↓
Registered Model
↓
Model Version
↓
champion
↓
FastAPI
↓
POST /process
```
