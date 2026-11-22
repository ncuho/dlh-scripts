# Выпуски отчётности нефтедобычи

Ручной DAG `oil_wave2_release` строит independent INITIAL/REFRESH для `chain=production` (14 таблиц) или `chain=performance` (21). Общий producer fence и GP advisory lock удерживаются одной задачей; manifest фиксирует Kafka identity/offsets и Iceberg snapshots. Нельзя продолжать REFRESH из baseline другой цепочки.

Параметры build: `action`, `chain`, `mode`, новый `schema`; для refresh также `initial_schema`. Пустой raw-приёмник и права ingestion создаются до build. Старые действия publish с фиксированным dataset8 относятся к прежней подготовке; с пустого контура используйте актуальный нативный publisher Platform Bridge/операторского CLI, создающий отдельный BI.
