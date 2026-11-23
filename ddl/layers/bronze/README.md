# bronze

Landing Kafka и Stage: доставки, партии, offsets и проекция envelope.

INITIAL использует исходные имена таблиц; отдельный REFRESH — суффикс `_next`. DDL не загружает данные и не выдаёт роли: grants настраиваются в LakeKeeper через CIAM-backed identities.
