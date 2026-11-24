# Первичная загрузка Greenplum → Lakehouse

`oil_layered_migration` — только INITIAL, JSON `{"mode":"initial"}`. Одна task держит общий read-only snapshot GP и копирует все 20 объектов через CedrusData Engine. Spark и пересчёт бизнес-логики здесь не запускаются. Порядок родителей, проверки PK/FK и 65 сверок обязательны; неясные исходы не повторяются вслепую.

После READY этот DAG повторно не запускают. Изменения догоняет `oil_dlh_catchup`; постоянную обработку выполняет `oil_dlh_release` через Spark каждую минуту. Оба обновляют те же таблицы, без `_next` и повторной выдачи grants. Все три DAG используют writer pool `oil_dlh_writer` с одним слотом.
