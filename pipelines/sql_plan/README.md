# План построения выпуска

`plan(schema, chain)` возвращает упорядоченный SQL CTAS для Cedrus. `production` содержит 12 доменных таблиц; вместе с canonical_input/canonical_event получается 14. `performance` добавляет семь зависимостей простоев, всего 21. Входной baseline и общий raw учитываются отдельно.
