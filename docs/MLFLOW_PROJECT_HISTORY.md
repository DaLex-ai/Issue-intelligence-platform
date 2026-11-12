# MLflow: реализованный цикл и исторические результаты

Документ описывает проверяемый по исходному коду цикл; он не реконструирует отсутствующие журналы прошлых экспериментов.

## Реализация

- Источник GitBugs / Cassandra закреплён на конкретном Git commit в `ml/data.py`.
- Подготовка: Summary + Description; удаляются пустые targets и тексты.
- `ml.eda` логирует распределения и сводку данных.
- `ml.train` сравнивает DummyClassifier, TF-IDF + Logistic Regression и TF-IDF + LinearSVC на стратифицированном split 80/20, seed 42.
- Метрики: Macro F1, Weighted F1, Accuracy, Precision/Recall; артефакты: classification report, confusion matrix и модель с signature.
- `ml.registry` проверяет завершённые runs и Macro F1; из двух кандидатов назначает лучшего в champion.
- Воркер разрешает alias один раз и сохраняет фактическую model version.

## Исторические метрики

| Модель | Macro F1 | Accuracy |
|---|---:|---:|
| DummyClassifier | 0.2410 | 0.9307 |
| TF-IDF + Logistic Regression | 0.3466 | 0.8754 |
| TF-IDF + LinearSVC | 0.2878 | 0.9274 |

Числа перенесены из существующего README; обучение в ходе исправлений надёжности не повторялось. Полного экспорта исходных runs нет, поэтому происхождение чисел независимо не подтверждено.

## Ограничения оценки

Один random split по одному проекту не подтверждает переносимость. Выбор champion по test Macro F1 использует test для выбора модели; отдельная финальная оценка пока не реализована. Dataset Tracking логирует подготовленный набор целиком в training context, а не отдельные train/test snapshots. Это ограничение метаданных, а не обучение TF-IDF на test: Pipeline.fit вызывается только на train. Проверки дубликатов, временного split и устойчивости редких классов остаются будущей ML-задачей.
