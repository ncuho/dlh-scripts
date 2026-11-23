# gold

Kimball: SCD2-измерения, суточный факт, агрегат и serving отчёта добычи.

INITIAL использует исходные имена таблиц; отдельный REFRESH — суффикс `_next`. DDL не загружает данные и не выдаёт роли: grants настраиваются в LakeKeeper через CIAM-backed identities.
