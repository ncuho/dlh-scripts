# SELECT на опубликованный выпуск

Скрипт принимает ready evidence через stdin внутри штатного token proxy и назначает существующей CIAM-backed роли oil_dlh_reader точечный SELECT. Для production — agg_field_day, для performance — agg_field_day и field_performance. UUID и snapshot читаются заново из LakeKeeper. Токен не выводится, OpenFGA напрямую не редактируется.
