# Нефтегазовый Lakehouse на Iceberg

Хранилище обеспечивает отчётность добычи и простоев по месторождениям и скважинам. Две бизнес-цепочки используют общий поток событий и исторические измерения. Данный репозиторий содержит целевые Iceberg-объекты, управляемый release workflow, доступы и целевые отчёты.

Статус настройки на 22 ноября 2026: первая цепочка перенесена как проверенный исторический baseline, вторая подготовлена к полному последовательному переносу. Непрерывное обновление первой целевой панели не включено. Существование кода, карточки каталога и физически загруженных данных — разные статусы.

## Навигация по сущностям

Каждая сущность находится в собственной папке с сохранённым именем файла. GitHub автоматически открывает её `README.md`.

```text
ddl/releases/field_performance/
├── field_performance.sql
└── README.md
dags/oil_wave2_release/
├── oil_wave2_release.py
└── README.md
```

В README каждой сущности указаны назначение и независимые статусы GitHub / Data Catalog / Lakehouse. Таблицы и views имеют карточки каталога; функции, grants, Java helpers и Python runtime не объявляются отдельными business assets. Их участие отражается DAG/connector/lineage и кодом.
## Структура репозитория

| Каталог | Назначение |
|---|---|
| [ddl/](ddl/) | Схемы, таблицы, последовательности и целевые CTAS. |
| [queries/](queries/) | Read-only песочница по слоям и проверки. |
| [dags/](dags/) | Управление source обработкой или target выпуском. |
| [grants/](grants/) | GP/Kafka права и CIAM-backed LakeKeeper роли. |
| [migration/](migration/) | Source fence, offsets и клиенты координации. |
| [superset/](superset/) | Определения datasets, charts и dashboards. |
| [deploy/](deploy/) | Установка SQL или сборка runtime. |
| [pipelines/](pipelines/) | SQL plan, runner, DQ и manifest helpers. |
| [publishing/](publishing/) | Контролируемая смена dataset и временная аутентификация. |
| [catalog/](catalog/) | Регистрация выпусков и scoped MCP overlay. |
| [access/](access/) | TLS transport, relay и verifier. |

## Сервисы и границы ответственности

| Сервис | Ответственность | Текущее участие |
|---|---|---|
| Greenplum / `og_dwh` | Исходное DWH: приём событий, история, SQL-обработка и исходные отчёты | Обе цепочки работают, источник не выводится из эксплуатации |
| Kafka / `oil.events.v1` | Долговечный журнал четырёх типов событий | 3 партиции, retention 30 суток; общий topic обоих consumers |
| Kafka Connect / GP sink | Транзакционная доставка в STG и checkpoint GP | Connector и task RUNNING |
| Kafka Connect / Iceberg sink | Независимая доставка в raw Iceberg | Connector PAUSED; offsets подготовлены, raw пустой |
| Airflow | Оркестрация исходной обработки и целевых выпусков | Два ручных DAG, без расписания; оба paused после проверок |
| CedrusData Engine | Чтение GP, создание Iceberg и целевые SQL-расчёты/сверки | Реальный вычислительный маршрут миграции |
| S3 + Apache Iceberg | Файлы данных, metadata и snapshots таблиц | Первая цепочка хранится постоянно; новые releases изолируются |
| LakeKeeper | Iceberg REST catalog, объекты и permissions | Права назначаются штатно через UI/Management API |
| CIAM | Аутентификация людей/сервисов и коды ролей | Reader/engineer и отдельные service identities |
| Superset | Datasets, графики и четыре пользовательские панели | Две исходные панели, одна опубликованная целевая, одна подготовленная |
| VK Data Catalog | Описания, глоссарий и связи источников/расчётов/отчётов | Карточки зарегистрированы по фактическим объектам; это не система авторизации |
| GitHub | Версионируемый код и описание каждой сущности | Не содержит credentials, runtime manifests или выгрузки каталога |
| MCP adapters / TLS gateway | Транспорт SQL к Cedrus и операций каталога; отдельные service identities | Source DAG использует native PostgreSQL Hook; target SQL идёт через Trino MCP, публикация BI — через Superset REST |
| JupyterHub / Spark | Исследования и альтернативные разработки | В выбранном исполняемом маршруте не участвуют |

Airflow и Superset уже находятся на платформе и обслуживают оба контура. Мигрируется бизнес-цепочка и её зависимости, а не установка всех сервисов заново. Сетевая связность Greenplum, Kafka и платформы подготовлена.

## Данные и модель хранилища

Три физических слоя: `oil_stg` → `oil_dds` → `oil_mart`. Serving-представления являются частью marts. В исходном baseline — 5 месторождений, 40 скважин, 365 производственных дат 2025 года, 14600 скважино-дней и 880 интервалов простоев. Масса — decimal в тоннах, время — UTC. Источник и ограничения использования описаны в [паспорте данных](DATA-PASSPORT.md).

**STG** сохраняет JSON и координаты доставки. Kafka checkpoint означает, что сообщения надёжно приняты в GP; он не доказывает применение DDS. Поэтому batch status проверяется отдельно.

**DDS** использует паттерны Raw Vault: 4 hubs бизнес-ключей, 3 links отношений, 4 satellites версий и дополнительный реестр принятых событий. `event_id` определяет дословный повтор; `entity_version` — порядок изменений; `op=D` сохраняет удаление в истории. Полное соответствие Data Vault 2.0 не подтверждено: применена адаптация с собственным event ledger и правилами валидации.

**Marts** использует исторические измерения и факты Kimball. `dim_field` и `dim_well` хранят интервалы действия версий. `fact_well_day` имеет зерно «скважина × дата», `fact_downtime_interval` — «один интервал простоя». Версии измерений факта добычи фиксируются явно. Дневные агрегаты имеют зерно «месторождение × дата».

Продуктивность = `SUM(oil_tonnes) / NULLIF(SUM(producing_well_days), 0)`. Это тонны на активный скважино-день, не фактически отработанные часы. Простои ограничены сутками UTC и не должны пересекаться. Полноценная нормализованная snowflake-модель отдельно не реализована: используются две связанные звезды с общими измерениями.

## Минимальный словарь

- **Событие** — сообщение об одном изменении бизнес-сущности; исправление старой даты является новым событием.
- **Партия / batch** — технический набор доставок, который source применяет одной транзакцией.
- **Зерно** — что означает одна строка факта: скважино-день или интервал простоя.
- **Hub / link / satellite** — постоянный бизнес-ключ / отношение ключей / история атрибутов.
- **SCD** — измерение с версиями и периодом действия; факт соединяется с исторически корректной версией.
- **Cut** — согласованная граница источника; здесь это вектор offsets и проверенное applied state.
- **Snapshot** — конкретная версия одной Iceberg-таблицы; несколько snapshots сами по себе не общая транзакция.
- **Release** — согласованный набор target-таблиц с evidence, который публикуется в BI отдельным действием.

Одно месторождение связано со многими скважинами; у одной скважины много дневных фактов и интервалов простоев. Версии атрибутов хранятся отдельно от постоянного бизнес-ключа.

## Producer, consumers и ingestion

Producer — программа `oil_events.py`, client ID `oil-events-producer`, отдельный TLS principal. Она формирует envelope с `schema_version`, `event_id`, `event_type`, `entity_id`, `entity_version`, `op`, `event_time`, `payload`. События четырёх типов: `field`, `well`, `production_day`, `downtime`; ключ Kafka — `event_type:entity_id`.

| Участник | Идентификатор | Роль и состояние |
|---|---|---|
| Producer | `oil-events-producer` | Реализация установлена; сейчас процесс остановлен, запуск явный |
| Consumer GP | `connect-webinar-oil-gp-ingest` | Читает общий topic, доставляет в GP; connector/task RUNNING |
| Consumer Iceberg | `connect-webinar-oil-wave2-iceberg` | Независимые offsets; connector PAUSED |
| GP worker coordination | `webinar-oil-connect` | Согласование workers, отличается от business consumer group |
| Iceberg worker coordination | `webinar-oil-wave2-connect` | Независимый worker целевого направления |

Connect workers дополнительно пишут собственные config/offset/status topics; Iceberg connector использует control topic `webinar-oil-wave2-iceberg-control`. Это технические журналы, не дополнительные бизнес-источники. В Data Catalog зарегистрирован business topic; служебные topics не имеют отдельных карточек.

Два consumers не делят сообщения между собой: у каждого своя group, поэтому оба получают все записи topic. Включение Iceberg consumer не отключает GP consumer и не переключает producer на новый адрес. Поток изменений здесь — события Kafka, не чтение WAL Greenplum.

Retention 30 суток ограничивает время повторной доставки. Уже сохранённые Iceberg snapshots не заменяют отсутствующие raw offsets при REFRESH. Schema JSON документирует контракт; автоматическое enforcement Schema Registry отдельно не настроено.

## Первая цепочка: добыча по месторождениям

Бизнес-результат — тонны добычи, активные скважино-дни и продуктивность по месторождениям/датам.

```text
field + well + production_day → oil.events.v1 → GP Connect
→ oil_stg.event_log → v_event_parsed → apply_batch
→ event + 3 hubs + 2 links + 3 satellites
→ refresh_production → dim_field + dim_well
→ fact_well_day → agg_field_day → v_field_production
→ Superset dataset 4 / dashboard 3
```

### Как перенесена

1. Выбран согласованный source cut: topic identity, позиции всех партиций, STG checkpoints и применение DDS.
2. Через Cedrus прочитан GP catalog `greenplum_gp`. В Iceberg скопированы STG-события и DDS-история только типов field/well/production_day.
3. Выполнены SQL-расчёты target измерений, факта и агрегата. Source PL/pgSQL не запускается в Lakehouse; его логика адаптирована к Cedrus SQL.
4. Первая волна содержит 14 таблиц: 1 STG + 9 DDS + 4 marts. Для всех выполнен двусторонний `EXCEPT ALL`; расхождений 0. Baseline добычи — 356937.73 т.
5. Target serving использует физическую `dlh.oil_mart.agg_field_day`. Источник `v_field_production` не копируется как view: установленный путь LakeKeeper/OPA имеет ограничение SELECT для target views.
6. Настроены роли/права, создан отдельный Superset dataset 6 / dashboard 5 через Cedrus, зарегистрированы реальные объекты и lineage.
7. Исходный GP dashboard 3 продолжает работать. Первая целевая панель остаётся frozen baseline: непрерывный REFRESH этой панели не включён.

### Какие 14 таблиц уже в Iceberg

- `oil_stg.event_log` — отфильтрованный исходный поток первой цепочки.
- `oil_dds.event`; `hub_field`, `hub_well`, `hub_production_day`; `link_well_field`, `link_production_day_well`; `sat_field`, `sat_well`, `sat_daily_production`.
- `oil_mart.dim_field`, `dim_well`, `fact_well_day`, `agg_field_day`.

Технические таблицы GP batch/checkpoint, sequence и coalesce не переносятся один-к-одному: целевая доставка использует свой Connect offset store и release manifests. GP распределение/индексы/процедурный runtime также не переносятся буквально. Iceberg не обеспечивает те же constraints, поэтому полнота и согласованность проверяются DQ.

## Вторая цепочка: эффективность добычи и простои

Она добавляет к добыче интервалы простоев, причины и часы. Её результат зависит от общих field/well/production_day и от корректного активного скважино-дня.

```text
downtime → Kafka → STG → apply_batch
→ event + hub_downtime + link_downtime_well + sat_downtime
→ refresh_downtime (после refresh_production)
→ dim_downtime_reason + fact_downtime_interval
→ agg_field_downtime_day
→ LEFT JOIN agg_field_day → v_field_performance
→ Superset dataset 5 / dashboard 4
```

### Что готово до первого запуска

Отдельный target worker/connector, raw `dlh.oil_stg.event_log_wave2`, native CIAM identity, scoped grants, пустой serving-буфер и dataset 8 / dashboard 6. Connector PAUSED, raw пустой, framework offsets сохранены. DAG и весь runtime установлены; постоянного `oil_r_*` release пока нет. SQL в Git — подготовленный код, а не признак уже созданной таблицы.

### INITIAL: историческая цепочка целиком

Source DAG применяет READY партии и пересчитывает обе source витрины. Target DAG получает Kafka lease, запрещает producer write, приостанавливает GP sink и удерживает общий GP advisory lock. Он требует совпадения checkpoints и broker cut, отсутствия OPEN/READY/FAILED и полного применения STG в DDS.

В `oil_i_<id>.baseline_event` сохраняется полный GP event ledger на этой границе, включая downtime. Фиксируются baseline/raw snapshots. Затем в новом `oil_r_<id>` создаётся 21 таблица:

- `canonical_input`, `canonical_event` — baseline плюс ограниченная delta, разбор и исключение дословных повторов;
- 4 hubs, 3 links, 4 satellites — логические Vault-проекции истории;
- `dim_field`, `dim_well`, `dim_downtime_reason`;
- `fact_well_day`, `fact_downtime_interval`;
- `agg_field_day`, `agg_field_downtime_day`, `field_performance`.

Baseline — дополнительная входная таблица вне этих 21. В release создаётся полный согласованный набор зависимостей отчёта, а не только таблицы downtime. Target Vault projections не воспроизводят GP audit-поля побайтово; история документов и бизнес-результат сверяются отдельно. В частности target `sat_production_day` соответствует истории GP `sat_daily_production` с другим представлением payload.

READY допустим только после всех DQ, сверок и повторной проверки snapshots. После этого освобождается fence и регистрируется реальная структура в каталоге. Несколько Iceberg commits не становятся одной транзакцией: изоляция обеспечивается новым namespace и отдельным шагом публикации.

### PUBLISH

В LakeKeeper выдать reader SELECT на две serving tables. DAG вызывает verifier, проверяет ожидаемую прежнюю привязку dataset 8 и переключает её на `<release>.field_performance`. Выполняются запрос к dataset, readback и обновление lineage. При ошибке поддерживается rollback привязки. Физические данные предыдущего выпуска не переписываются.

### REFRESH: изменения после первоначальной границы

Включить отдельный Iceberg consumer до отправки/применения новой delta. Producer и Kafka topic остаются прежними. Один подготовленный набор содержит 5 сообщений: correction +1.250 т для первого well/day, логическое удаление простоя, позднюю версию well с valid_from 2025-07-01, late arrival простоя на другую дату и дословный повтор correction.

Применить source партии, дождаться raw доставки всех новых offsets. Новый REFRESH использует **тот же INITIAL baseline** плюс raw snapshot и полуоткрытые окна [start,end) всех партиций. Missing offset, конфликт документа или расхождение останавливают выпуск. После READY — права на новый release и явный PUBLISH с ожидаемым предыдущим schema.

Число/сумма часов простоев могут не измениться: удалённый и добавленный интервалы могут иметь одинаковую длительность, но разные даты. Поэтому сверяются история и дневные строки, а не только итоговые KPI. Дословный replay не должен удваивать добычу.

### Что фактически проверено

Изолированные INITIAL, обратимый PUBLISH с четырьмя chart jobs и REFRESH с пустым новым окном завершились успешно. Отдельно проверены SQL U/D/replay и Kafka→Iceberg/restart/replay на изолированном topic. Основной полный непустой REFRESH ещё не выполнен; вторую бизнес-волну оставили для последовательного прохода оператором. Пробные releases удалены, dataset 8 возвращён к пустому буферу.

## Общие зависимости и взаимное влияние

1. Обе цепочки используют один business topic, общие field/well/production events, hubs и измерения. Downtime-отчёт зависит от результата production расчёта.
2. Source `oil_gp_release` при новых партиях вызывает обе refresh функции. Correction добычи или версия well могут изменить оба исходных отчёта.
3. Миграционный fence временно останавливает **запись/приём** общего источника. Старые отчёты могут читаться, но свежесть обеих цепочек на время build не увеличивается. Это реальный эксплуатационный эффект.
4. Постоянные таблицы первой волны в `oil_stg/oil_dds/oil_mart` не переписываются второй волной: та создаёт свой `oil_r_*`. Это защищает первый опубликованный baseline, но требует дополнительных S3/compute ресурсов.
5. Dashboard 5 не обновляется автоматически вместе со второй панелью. После delta сравнивать его с обновлённым GP как с синхронной копией нельзя.
6. Чтобы показать оба отчёта на новом cut, администратор после сверки/grants отдельно переводит dataset 6 на `agg_field_day` нового release. Ограниченный publisher управляет только dataset 8.

Целевой объект передачи — отчёт со всей цепочкой зависимостей: события → история → бизнес-логика → serving → доступы → BI → lineage. Общие зависимости переиспользуются концептуально, но согласованный release materializes собственную версию для независимой публикации.

## Доступы: CIAM → LakeKeeper → данные и BI

CIAM хранит identities и коды ролей. LakeKeeper сопоставляет их своим ролям через `source-id` и проверяет permissions на warehouse/namespaces/tables. Существуют `oil_dlh_reader` и `oil_dlh_engineer`.

| Роль / identity | Назначение |
|---|---|
| `oil_dlh_reader` | Чтение опубликованных marts; без записи и доступа к STG/DDS |
| `oil_dlh_engineer` | Обработка трёх слоёв: describe/select/create/modify |
| Iceberg sink service identity | Отдельное право modify только на `event_log_wave2`; modify включает rename/drop по нативной модели |
| `oil_migration_publisher` в Superset | Ограниченное изменение dataset 8, проверка запросов и rollback; без полного Admin/SQL Lab |
| GP Kafka writer | Запись STG, batch и checkpoint; не выполняет бизнес-расчёты |
| GP Airflow runner | Вызов ограниченных SECURITY DEFINER функций и чтение контрольных таблиц; без прямого DML |
| Cedrus GP reader | Read-only чтение источника для среза/сверки |

Для каждого нового release в LakeKeeper UI: `Warehouses → dlh → <release> → <serving table> → Permissions → Grant → Roles`. Reader получает table-level `select` на `agg_field_day` и `field_performance`; SELECT на весь release namespace не выдаётся, потому что там находятся canonical/DDS данные.

Независимый read-only verifier перед PUBLISH проверяет текущие grants, UUID и snapshot IDs. Вручную написанный JSON о правах не считается подтверждением. Назначения не пишутся напрямую в OpenFGA.

Superset RBAC регулирует доступ к панели отдельно. Его соединение с Cedrus использует сервисную identity: per-user impersonation не включён. Добавление CIAM роли не выдаёт автоматически доступ к dashboard. Для демонстрации запрета нужен пользователь без административных ролей; effective role checks не равны проверенному входу конкретного человека.

GP GRANT и пароли пользователей не импортировались буквально. Целевые роли и назначения настроены по смыслу доступа. Data Catalog показывает происхождение данных, но не предоставляет право читать строки. Правила интеграции сверены со штатными HOWTO 26.2.1; фактическая версия CIAM Flow на стенде — 26.1.0.

## Качество, восстановление и наблюдаемость

- Проверяются envelope, конфликты event_id/entity_version, SCD-однозначность, полнота фактов, статус скважины и допустимость интервалов.
- Сверяются полная история документов и бизнес-таблицы через двусторонний EXCEPT ALL, а также строки, NULL и агрегаты.
- Cut — вектор exclusive Kafka offsets + topic/cluster ID + GP applied state; дата события или время на часах его не заменяют.
- Manifest фиксирует входы и snapshots; evidence — этапы, DQ и target snapshots. Они находятся на durable Airflow PVC, не в Git.
- `catalog_ready`/`catalog_published` повторяют только регистрацию; CTAS вслепую не повторяется. Неизвестный исход запроса и crash lease разбираются оператором.
- Старый GP-контур и предыдущие releases сохраняются для параллельного чтения и возврата. Публикация не равна выводу GP из эксплуатации.

В текущих target CTAS задан формат Parquet; явное partitioning Iceberg в этих SQL не сконфигурировано. GP distribution keys не являются Iceberg partition spec.

Наблюдаемость: Airflow task logs и release evidence; Kafka offsets/status/lag; Iceberg snapshot IDs; Superset dataset binding; Data Catalog lineage. Панель Kafka read-only и не управляет коннекторами.

Ограничения: Kafka один broker/RF=1; промышленная HA/DR и нагрузочная сертификация не заявлены. Для UI нужен VPN/маршрут, авторизация и доверенный CA платформы. Автоматическая интеграция JSON→BI column lineage не настроена: зарегистрированы проверенные зависимости/табличные column mappings и описания обработки. Наличие карточки каталога не подтверждает ни загрузку данных, ни право их читать.

## Сценарий показа и операционный порядок: 22 шага

Первую волну показывают как уже выполненный перенос; вторую проводят от исходной цепочки до целевого отчёта. Два порядка требуют внимания: роли выдаются **до публикации BI**, а target consumer включается **до доставки delta**, даже если архитектурное объяснение этих пунктов идёт позже.

| Шаг | Что показать и сделать |
|---|---|
| 1. Два контура | GP — источник, Data Platform — целевой Lakehouse. Оба контура остаются работающими. |
| 2. Сервисы | Проверить UI, состояние, версии, VPN/CA; DAG paused — это отсутствие расписания, не ошибка готовности. |
| 3. AS IS | Источник → Kafka → GP sink → STG → DDS → marts → Superset; Airflow управляет обработкой. |
| 4. Слои GP | Показать реальный JSON, batch/checkpoint, историю, hubs/links/sats, SCD и зерно фактов. |
| 5. TO BE | S3/Iceberg хранит данные; LakeKeeper — каталог/permissions; Cedrus — SQL; Airflow — выпуск; BI — потребление. |
| 6. Catalog | Из исходного отчёта пройти к таблицам, расчётам и topic. Пояснить ручную регистрацию и границы column lineage. |
| 7. Граница цепочки | Первая — добыча; вторая — добыча с простоями. Архив legacy_month не требуется обеим. |
| 8. Стратегия | Установить зависимости, порядок, cut, DQ, rollback и критерии бизнес-приёмки. |
| 9. Исходный код | Открыть entity folders GP: DDL, functions, views, DAG, topic, connector и BI. |
| 10. Target DDL | Показать существующие 14 таблиц первой волны и подготовленные release SQL второй. Код не равен живому объекту. |
| 11. Mapping | Сравнить GP distribution/PLpgSQL/grants/views с Iceberg/Cedrus/CIAM-LK/physical serving. |
| 12. Cut | Source DAG `{}`; target INITIAL берёт живой fence и фиксирует offsets/checkpoints/snapshots. Исторический cut не использовать вслепую. |
| 13. Перенос данных | Первую волну уже загружали Cedrus. Для второй INITIAL создаёт полный event baseline и новый изолированный namespace. |
| 14. Сверка | DQ и двусторонние сравнения на одной границе; до READY не публиковать. |
| 15. Бизнес-логика | Сравнить source PL/pgSQL с target CTAS/SQL. Логика подготовлена в Git до запуска, выполняется после подготовки входов. |
| 16. Расчёты | Airflow выполняет зависимости, создаёт 21 target table и фиксирует evidence; дождаться BUILD_RECONCILED/CATALOG_READY. |
| 17. BI | До PUBLISH дать reader table-level SELECT; затем переключить dataset 8 и сравнить dashboard 4/6 на одном cut. |
| 18. Догон изменений | До отправки delta включить target connector. Отправить 5 сообщений, source DAG, дождаться raw offsets, выполнить REFRESH и сверки. |
| 19. Новый consumer | Показать независимые groups и параллельную доставку. Producer/topic не переключаются, GP consumer остаётся. |
| 20. Роли и права | Показать CIAM роли и LakeKeeper UI; объяснить separate Superset RBAC и service identity. Реальные grants должны уже существовать перед PUBLISH. |
| 21. Итог | Проверить все слои, snapshots, dataset binding и lineage; GP не выводить. При необходимости отдельно актуализировать dataset 6 на новый cut. |
| 22. Следующая волна | Выбрать другой отчёт в каталоге, определить уже общие зависимости, новые данные и эксплуатационные обязательства. |

Изменение порядка действий ради презентации не должно нарушать реальные prerequisites. Точные INITIAL/REFRESH/PUBLISH JSON и Kafka команды находятся в README целевого DAG.

## Установка и изменение кода

После разнесения исходников runtime собирается в прежний плоский managed-каталог:

```bash
python deploy/package_runtime/package_runtime.py \
  --output /tmp/oil-runtime-package
```

Команда только собирает пустой output-каталог, не меняет сервисы и не запускает DAG. Сохраняются имена импортов/исполняемых файлов установленного worker. Admin verifier и Catalog overlay загружают источники из соседних entity folders. Точные действия оператора: [README DAG](dags/oil_wave2_release/README.md).

Release SQL содержит параметры и пример namespace; его нельзя исполнять подряд без manifest, source cut и проверок. Baseline и snapshots INITIAL должны сохраняться для всех последующих REFRESH.

## Интерфейсы

- Airflow: http://10.0.2.13:8080
- LakeKeeper UI: https://10.0.2.13
- CIAM: https://dp-103.mcs.local/iam-admin/
- Cedrus: https://10.0.2.17
- Data Catalog: http://10.0.2.11:8585
- Superset: https://10.0.2.24
- GP dashboards: [3 — добыча](https://10.0.2.24/superset/dashboard/3/), [4 — простои](https://10.0.2.24/superset/dashboard/4/)
- DLH dashboards: [5 — baseline добычи](https://10.0.2.24/superset/dashboard/5/), [6 — подготовленный отчёт](https://10.0.2.24/superset/dashboard/6/)

Адреса 10.0.2.x требуют доступа в сеть платформы. Секреты, SSH keys и client credentials в README и Git не публикуются.

## Полный указатель сущностей

Ссылки ведут в папки с автоматически открывающимся описанием; служебные реализации не обозначены как перенесённые бизнес-таблицы.

| Сущность | Назначение |
|---|---|
| [access/transport/configure_fence_relay](access/transport/configure_fence_relay/) | Настраивает ограниченный SSH relay от Airflow к Kafka fence с forced command и закреплёнными host keys. |
| [access/transport/configure_runtime_tls](access/transport/configure_runtime_tls/) | Настраивает HTTPS+Bearer gateway для вызовов runtime к SQL MCP, каталогу и проверке serving-доступов. |
| [access/verifier/configure_lakekeeper_verifier](access/verifier/configure_lakekeeper_verifier/) | Разворачивает verifier с TLS, авторизацией и ограничением сетевого доступа. Секреты хранятся в Kubernetes Secret. |
| [access/verifier/lakekeeper_verifier](access/verifier/lakekeeper_verifier/) | Read-only сервис проверки актуальных snapshots, table-level grants и отсутствия избыточного родительского доступа перед публикацией. |
| [catalog/catalog_launcher_overlay](catalog/catalog_launcher_overlay/) | Расширяет MCP каталога узкими операциями удаления BI-lineage и обновления описаний разрешённых BI-объектов. |
| [catalog/deploy_catalog_overlay](catalog/deploy_catalog_overlay/) | Подключает versioned overlay к MCP через ConfigMap. После Helm/XaaS reconcile требуется сверка и при необходимости reapply. |
| [catalog/runtime_catalog](catalog/runtime_catalog/) | Регистрирует только фактически созданный и сверенный release. После подтверждённой публикации обновляет lineage к графикам и описание текущего источника. |
| [dags/oil_wave2_release](dags/oil_wave2_release/) | Ручной DAG подготовки, сверки и публикации отчётности нефтедобычи. Airflow: `http://10.0.2.13:8080`. Рабочий код: oil_wave2_release.py. |
| [ddl/ingestion/event_log_wave2](ddl/ingestion/event_log_wave2/) | Отдельная raw Iceberg-таблица Kafka sink. Хранит Kafka-координаты и полный JSON, не изменяя ранее принятый baseline первой цепочки. |
| [ddl/releases/agg_field_day](ddl/releases/agg_field_day/) | Дневная витрина зерна «месторождение × дата»: тонны, активные скважино-дни и тонны на активный скважино-день. |
| [ddl/releases/agg_field_downtime_day](ddl/releases/agg_field_downtime_day/) | Дневная сумма часов допустимых непересекающихся простоев по месторождению. |
| [ddl/releases/baseline_event](ddl/releases/baseline_event/) | Неизменяемая копия полного реестра GP events на INITIAL cut. Один и тот же snapshot используется последующими REFRESH. |
| [ddl/releases/canonical_event](ddl/releases/canonical_event/) | Убирает дословные повторы документов и разбирает envelope. Конфликтующие ID/версии не замалчиваются: их блокируют DQ gates. |
| [ddl/releases/canonical_input](ddl/releases/canonical_input/) | Объединяет исходный baseline и raw-события из всех полуоткрытых диапазонов offsets [start,end), зафиксированных в manifest. |
| [ddl/releases/dim_downtime_reason](ddl/releases/dim_downtime_reason/) | Справочник причин: EQUIPMENT, POWER, PLANNED. Нормализует группировку простоев. |
| [ddl/releases/dim_field](ddl/releases/dim_field/) | Историческое измерение месторождения. Интервал действия версии — [valid_from, valid_to); удаление сохраняется отдельным признаком. |
| [ddl/releases/dim_well](ddl/releases/dim_well/) | Историческое измерение скважины, её принадлежности месторождению и статуса. Версия выбирается на производственную дату. |
| [ddl/releases/fact_downtime_interval](ddl/releases/fact_downtime_interval/) | Факт зерна «интервал простоя». Содержит скважину, месторождение, начало/конец, причину и длительность в часах. |
| [ddl/releases/fact_well_day](ddl/releases/fact_well_day/) | Факт зерна «скважина × производственная дата». Содержит тонны, статус и версии измерений well/field, действовавшие на дату. |
| [ddl/releases/field_performance](ddl/releases/field_performance/) | Физическая serving-таблица второй цепочки. Объединяет добычу и простои по дате/месторождению и используется целевым BI. |
| [ddl/releases/hub_downtime](ddl/releases/hub_downtime/) | Хранит бизнес-идентификатор интервала простоя и детерминированный hash key. Не содержит изменяемых описательных атрибутов. |
| [ddl/releases/hub_field](ddl/releases/hub_field/) | Хранит бизнес-идентификатор месторождения и детерминированный hash key. Не содержит изменяемых описательных атрибутов. |
| [ddl/releases/hub_production_day](ddl/releases/hub_production_day/) | Хранит бизнес-идентификатор скважино-дня добычи и детерминированный hash key. Не содержит изменяемых описательных атрибутов. |
| [ddl/releases/hub_well](ddl/releases/hub_well/) | Хранит бизнес-идентификатор скважины и детерминированный hash key. Не содержит изменяемых описательных атрибутов. |
| [ddl/releases/link_downtime_well](ddl/releases/link_downtime_well/) | Хранит историческую связь интервала простоя со скважиной по техническим ключам. Изменяемые атрибуты находятся в satellites. |
| [ddl/releases/link_production_day_well](ddl/releases/link_production_day_well/) | Хранит историческую связь скважино-дня со скважиной по техническим ключам. Изменяемые атрибуты находятся в satellites. |
| [ddl/releases/link_well_field](ddl/releases/link_well_field/) | Хранит историческую связь скважины с месторождением по техническим ключам. Изменяемые атрибуты находятся в satellites. |
| [ddl/releases/sat_downtime](ddl/releases/sat_downtime/) | Сохраняет версии атрибутов интервала простоя, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document. |
| [ddl/releases/sat_field](ddl/releases/sat_field/) | Сохраняет версии атрибутов месторождения, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document. |
| [ddl/releases/sat_production_day](ddl/releases/sat_production_day/) | Сохраняет версии атрибутов дневной добычи, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document. |
| [ddl/releases/sat_well](ddl/releases/sat_well/) | Сохраняет версии атрибутов скважины, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document. |
| [ddl/serving/field_performance](ddl/serving/field_performance/) | Физическая serving-таблица второй цепочки. Объединяет добычу и простои по дате/месторождению и используется целевым BI. |
| [ddl/serving/oil_demo_template](ddl/serving/oil_demo_template/) | Namespace пустой serving-таблицы, используемой до первого подтверждённого выпуска второй цепочки. |
| [ddl/wave1/schemas/oil_dds](ddl/wave1/schemas/oil_dds/) | Схема интеграции и истории бизнес-сущностей с hubs, links и satellites. |
| [ddl/wave1/schemas/oil_mart](ddl/wave1/schemas/oil_mart/) | Схема измерений, фактов и serving-агрегатов управленческой отчётности. |
| [ddl/wave1/schemas/oil_stg](ddl/wave1/schemas/oil_stg/) | Схема приёма неизменённых событий и технического контроля загрузки. |
| [ddl/wave1/tables/oil_dds/event](ddl/wave1/tables/oil_dds/event/) | Реестр принятых событий DDS. event_id определяет повтор, entity_version задаёт порядок изменений, event_document позволяет сверить историю. |
| [ddl/wave1/tables/oil_dds/hub_field](ddl/wave1/tables/oil_dds/hub_field/) | Хранит бизнес-идентификатор месторождения и детерминированный hash key. Не содержит изменяемых описательных атрибутов. |
| [ddl/wave1/tables/oil_dds/hub_production_day](ddl/wave1/tables/oil_dds/hub_production_day/) | Хранит бизнес-идентификатор скважино-дня добычи и детерминированный hash key. Не содержит изменяемых описательных атрибутов. |
| [ddl/wave1/tables/oil_dds/hub_well](ddl/wave1/tables/oil_dds/hub_well/) | Хранит бизнес-идентификатор скважины и детерминированный hash key. Не содержит изменяемых описательных атрибутов. |
| [ddl/wave1/tables/oil_dds/link_production_day_well](ddl/wave1/tables/oil_dds/link_production_day_well/) | Хранит историческую связь скважино-дня со скважиной по техническим ключам. Изменяемые атрибуты находятся в satellites. |
| [ddl/wave1/tables/oil_dds/link_well_field](ddl/wave1/tables/oil_dds/link_well_field/) | Хранит историческую связь скважины с месторождением по техническим ключам. Изменяемые атрибуты находятся в satellites. |
| [ddl/wave1/tables/oil_dds/sat_daily_production](ddl/wave1/tables/oil_dds/sat_daily_production/) | Сохраняет версии атрибутов дневной добычи, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document. |
| [ddl/wave1/tables/oil_dds/sat_field](ddl/wave1/tables/oil_dds/sat_field/) | Сохраняет версии атрибутов месторождения, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document. |
| [ddl/wave1/tables/oil_dds/sat_well](ddl/wave1/tables/oil_dds/sat_well/) | Сохраняет версии атрибутов скважины, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document. |
| [ddl/wave1/tables/oil_mart/agg_field_day](ddl/wave1/tables/oil_mart/agg_field_day/) | Дневная витрина зерна «месторождение × дата»: тонны, активные скважино-дни и тонны на активный скважино-день. |
| [ddl/wave1/tables/oil_mart/dim_field](ddl/wave1/tables/oil_mart/dim_field/) | Историческое измерение месторождения. Интервал действия версии — [valid_from, valid_to); удаление сохраняется отдельным признаком. |
| [ddl/wave1/tables/oil_mart/dim_well](ddl/wave1/tables/oil_mart/dim_well/) | Историческое измерение скважины, её принадлежности месторождению и статуса. Версия выбирается на производственную дату. |
| [ddl/wave1/tables/oil_mart/fact_well_day](ddl/wave1/tables/oil_mart/fact_well_day/) | Факт зерна «скважина × производственная дата». Содержит тонны, статус и версии измерений well/field, действовавшие на дату. |
| [ddl/wave1/tables/oil_stg/event_log](ddl/wave1/tables/oil_stg/event_log/) | Принимает исходные JSON-события вместе с topic, partition, offset и ID партии. Сохраняет доставку до применения бизнес-логики. |
| [deploy/package_runtime](deploy/package_runtime/) | Собирает categorized source в плоский каталог managed Airflow, сохраняя действующие имена импортов. Публикацию и перезапуск сервисов не выполняет. |
| [grants/lakekeeper/grant_ingestion](grants/lakekeeper/grant_ingestion/) | Назначает отдельной CIAM service identity право modify только на выделенную raw-таблицу Iceberg. |
| [grants/lakekeeper/grant_serving](grants/lakekeeper/grant_serving/) | Admin-утилита назначения reader SELECT на две serving-таблицы конкретного готового release; проверяет snapshots. Основной операторский способ — LakeKeeper UI. |
| [grants/lakekeeper/lakekeeper_roles](grants/lakekeeper/lakekeeper_roles/) | Сопоставляет CIAM-backed reader/engineer роли с разрешениями LakeKeeper через штатный Management API. |
| [migration/fence/fence_controller](migration/fence/fence_controller/) | Адаптер ограниченного SSH-вызова acquire/status/release удалённого контроллера fence. |
| [migration/fence/fence_probe_socket](migration/fence/fence_probe_socket/) | Проверяет действительность fence через локальный канал управляющей задачи; используется runner перед операциями. |
| [migration/fence/source_fence](migration/fence/source_fence/) | Удерживает живую границу на время build: lease Kafka, запрет producer write, pause GP sink и GP advisory lock. При потере подтверждения прекращает работу. |
| [pipelines/bounded_delta](pipelines/bounded_delta/) | Проверяет структуру manifest, идентификаторы таблиц, snapshots и offset-диапазоны; атомарно сохраняет evidence. |
| [pipelines/cedrus_release_runner](pipelines/cedrus_release_runner/) | Создаёт 21 release-таблицу в порядке зависимостей через Cedrus. Проверяет snapshots, непрерывность offsets и результаты; сохраняет evidence, поддерживает ограниченное восстановление. |
| [pipelines/dq_sql](pipelines/dq_sql/) | Генерирует read-only проверки конфликтов, ссылочной полноты, SCD, статусов, интервалов, истории событий и двусторонние EXCEPT ALL с GP. |
| [pipelines/sql_plan](pipelines/sql_plan/) | Генерирует параметризованный CTAS для 11 Vault-проекций и 8 marts. Допускает только проверенное имя нового release namespace. |
| [publishing/publish_dataset](publishing/publish_dataset/) | Переключает только разрешённый Superset dataset при совпадении прежней привязки. Проверяет новый запрос и выполняет rollback при ошибке. |
| [publishing/superset_auth](publishing/superset_auth/) | Получает короткий JWT ограниченного publisher по HTTPS, держит его во временном файле и удаляет после операции. |
| [queries/sandbox/production_report](queries/sandbox/production_report/) | Read-only запрос к опубликованному baseline первой цепочки через Cedrus. Сравнение с GP корректно только на одной границе. |
| [queries/validation/03_wave1_reconcile](queries/validation/03_wave1_reconcile/) | Read-only SQL-проверки качества, полноты и согласованности результатов двух контуров. |
| [superset/charts/performance_by_field](superset/charts/performance_by_field/) | График «Показатели по месторождениям». Использует dataset 8; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/charts/performance_downtime_total](superset/charts/performance_downtime_total/) | График «Часы простоев». Использует dataset 8; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/charts/performance_oil_total](superset/charts/performance_oil_total/) | График «Добыча, т». Использует dataset 8; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/charts/performance_trend](superset/charts/performance_trend/) | График «Динамика простоев по месторождениям». Использует dataset 8; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/charts/production_active_well_days](superset/charts/production_active_well_days/) | График «Активные скважино-дни». Использует dataset 6; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/charts/production_by_field](superset/charts/production_by_field/) | График «Показатели по месторождениям». Использует dataset 6; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/charts/production_oil_total](superset/charts/production_oil_total/) | График «Добыча, т». Использует dataset 6; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/charts/production_trend](superset/charts/production_trend/) | График «Динамика добычи по месторождениям». Использует dataset 6; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/dashboards/performance](superset/dashboards/performance/) | Панель «Эффективность добычи и простои · Lakehouse». Содержит сохранённые графики, layout и фильтры региона/месторождения. Источник определяется соответствующим dataset; публикация release выполняется отдельно. |
| [superset/dashboards/production](superset/dashboards/production/) | Панель «Добыча по месторождениям · Lakehouse». Содержит сохранённые графики, layout и фильтры региона/месторождения. Источник определяется соответствующим dataset; публикация release выполняется отдельно. |
| [superset/datasets/performance](superset/datasets/performance/) | Dataset 8 читает oil_demo_template.field_performance. Определяет временное поле, колонки и доступные метрики для графиков. |
| [superset/datasets/production](superset/datasets/production/) | Dataset 6 читает oil_mart.agg_field_day. Определяет временное поле, колонки и доступные метрики для графиков. |

Смежный репозиторий: [Greenplum](https://github.com/ncuho/dwh-greenplum-scripts).


## Начальное состояние миграции

22 ноября 2026 целевые бизнес-таблицы и прежние целевые BI/Catalog объекты удалены. В репозитории сохранён подготовленный код двух цепочек, наличие кода не означает перенесённые данные. Production строит отдельный выпуск из 14 таблиц, performance — из 21; baseline и raw находятся вне этого числа. Целевой dataset/dashboard создаётся после сверки и выдачи SELECT, числовые ID определяются публикацией. Исходный GP продолжает работать.

`operations/deploy_release` публикует DAG и библиотеки в действующий S3-префикс Airflow. Расчётный маршрут — CedrusData Engine/Trino, а не Spark; Spark-код является альтернативной реализацией для отдельной проверки.
