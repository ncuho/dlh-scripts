# canonical_event

Убирает дословные повторы документов и разбирает envelope. Конфликтующие ID/версии не замалчиваются: их блокируют DQ gates.

Контур: immutable release второй цепочки; параметры берутся из согласованного manifest.

Исходный код: [canonical_event.sql](canonical_event.sql).
