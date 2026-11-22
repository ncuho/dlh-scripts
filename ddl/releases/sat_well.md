# sat_well

Сохраняет версии атрибутов скважины, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document.

Контур: immutable release второй цепочки; параметры берутся из согласованного manifest.

Исходный код: [sat_well.sql](sat_well.sql).
