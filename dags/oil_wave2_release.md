# oil_wave2_release

Ручной DAG подготовки, сверки и публикации отчётности нефтедобычи. Airflow: `http://10.0.2.13:8080`. Рабочий код: [oil_wave2_release.py](oil_wave2_release.py).

## INITIAL

Сначала выполнить source DAG `oil_gp_release` с `{}` и дождаться SUCCESS. Producer не должен создавать новые события между подготовкой источника и фиксацией cut. В Airflow открыть `oil_wave2_release` → Trigger DAG with config; выбрать новое свободное имя release:

```json
{
  "action": "build",
  "mode": "initial",
  "schema": "oil_r_efficiency_001"
}
```

DAG удерживает Kafka/GP fence, проверяет checkpoints и применение STG→DDS, сохраняет полный baseline, строит 21 release table. Ожидать `BUILD_RECONCILED` и `CATALOG_READY`. Manifest, baseline и snapshots сохранять для последующих REFRESH. Существующий namespace повторно не перезаписывается.

## Доступ и PUBLISH

В LakeKeeper UI (`https://10.0.2.13`) открыть warehouse `dlh` → новый release namespace → каждую из таблиц `agg_field_day`, `field_performance` → Permissions → Grant → Roles. Роли `oil_dlh_reader` назначить table-level `select`; не выдавать SELECT на весь namespace с raw/DDS.

Проверить текущую привязку dataset 8 в Superset. Пока она указывает на пустой буфер, запустить:

```json
{
  "action": "publish",
  "schema": "oil_r_efficiency_001",
  "dataset_id": 8,
  "database_id": 2,
  "expected_schema": "oil_demo_template",
  "expected_table": "field_performance",
  "table": "field_performance"
}
```

Ожидать `PUBLISH_VERIFIED` и `CATALOG_PUBLISHED`; обновить dashboard 6 (`https://10.0.2.24/superset/dashboard/6/`). Сравнение с GP корректно на одном cut. Права BI-пользователя и service identity подключения Superset регулируются отдельно от роли LakeKeeper.

## Поток изменений

Два действия выполняет оператор через CLI/API, панель Kafka предназначена только для наблюдения. На Kafka VM включить выделенный target connector:

```bash
curl -fsS -X PUT   http://127.0.0.1:18085/connectors/webinar-oil-wave2-iceberg/resume
curl -fsS   http://127.0.0.1:18085/connectors/webinar-oil-wave2-iceberg/status
```

После RUNNING один раз отправить управляемый набор изменений:

```bash
sudo -u kafka /opt/kafka-demo/oil-generator/run-oil-generator.sh   --phase delta --seed 42 --fields 5 --wells 40 --days 365 --kafka
```

Пять сообщений включают дословный replay, correction добычи +1.250 т, удаление простоя, late arrival и версию well. Новые и удалённые интервалы могут иметь одинаковую суммарную длительность; для проверки нужны строки/дни и история, а не только суммы. Это event delta Kafka, не CDC из WAL Greenplum.

В Airflow применить события через `oil_gp_release` с `{}`. Дождаться доставки всех новых offsets и в GP, и в target raw; только затем REFRESH:

```json
{
  "action": "build",
  "mode": "refresh",
  "schema": "oil_r_efficiency_002",
  "initial_schema": "oil_r_efficiency_001"
}
```

REFRESH использует тот же INITIAL baseline, новый raw snapshot и все позиции [start,end). Дырка offsets или несовпадение блокируют выпуск. После READY назначить reader SELECT на две новые serving tables и повторить PUBLISH со schema `oil_r_efficiency_002`, expected_schema `oil_r_efficiency_001`.

Первая панель dashboard 5 остаётся frozen baseline. Для её актуализации администратор отдельно переключает dataset 6 на `agg_field_day` нового сверенного release; ограниченный publisher этого не делает. GP продолжает работать для параллельной эксплуатации и возврата чтения.

## Восстановление

Не повторять CTAS вслепую. Проверить сохранённые manifest/evidence, состояние fence и завершённость старого запроса. Регистрация метаданных повторяется отдельно действиями `catalog_ready` / `catalog_published`; они не пересоздают данные. После crash lease требуется разбор оператором.

Изолированные INITIAL, обратимый PUBLISH и REFRESH с пустым новым окном проверены. Основной непустой REFRESH ещё не выполнялся. После настройки DAG оставлен paused; новый рабочий запуск выполняется явно оператором. Технические IDs connectors/connections сохранены для совместимости.
