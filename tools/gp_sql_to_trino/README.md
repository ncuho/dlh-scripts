# GP ad-hoc → Trino

`gp_sql_to_trino.py` — небольшой SQLGlot-конвертер SELECT/WITH/UNION. Читает dialect postgres, выдаёт форматированный Trino SQL и заново парсит результат. Запросы не исполняет. Версия SQLGlot закреплена в requirements.txt.

```bash
python3 -m pip install -r requirements.txt
python3 gp_sql_to_trino.py query_gp.sql --lakehouse -o query_trino.sql
```

`--lakehouse` меняет квалифицированные oil_stg/oil_dds/oil_mart на dlh.bronze/dlh.silver/dlh.gold. Без флага меняется только диалект. Имена CTE, строковые литералы и бизнес-формулы не заменяются текстовым поиском.

Неизвестные функции, GP system catalogs, SELECT INTO, модифицирующие CTE, DML, DDL и процедуры отклоняются. Это перевод синтаксиса, не доказательство равенства результатов: проверьте типы, time zone, integer/decimal division, NULL и результаты на одной границе данных. Существование таблиц и права проверяются отдельно.

Явные PostgreSQL casts `text::json`/`text::jsonb` переводятся в `JSON_PARSE(text)`: Trino CAST(varchar AS JSON) дал бы JSON-строку, а не объект. Вход такого cast должен быть текстовым; redundant casts уже JSON-колонок требуют отдельной проверки типов. Для временных меток без указанной точности сохраняется микросекундная точность timestamp(6). Неограниченный numeric требует явных precision/scale. Пример доставки событий фиксирует UTC перед выделением даты.

Примеры запросов по текущему DWH находятся в `queries/adhoc_gp/` исходного репозитория; их переведённые версии — в `queries/adhoc_trino/` этого репозитория.
