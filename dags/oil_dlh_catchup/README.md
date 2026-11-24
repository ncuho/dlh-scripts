# oil_dlh_catchup

Отдельный DAG выведен из регистрации. Его история в Airflow сохраняется.

Догон выполняет `oil_dlh_release` с JSON `{"mode":"catchup","overlap_offsets":10}`. После READY/58 проверок тот же DAG работает по расписанию с `{}` (режим release, 53 проверки, без GP). Код обработки общий, таблицы и checkpoints сохраняются.
