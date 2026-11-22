# sat_production_day

Сохраняет версии атрибутов дневной добычи, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document.

Контур: immutable release второй цепочки; параметры берутся из согласованного manifest.

Исходный код: [sat_production_day.sql](sat_production_day.sql).
