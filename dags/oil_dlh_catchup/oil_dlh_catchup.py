# Исторический DAG выведен из регистрации; прежние runs сохраняются в Airflow.
# Используйте oil_dlh_release: ручной conf {"mode":"catchup","overlap_offsets":10},
# затем обычное расписание с mode=release по умолчанию. Здесь DAG не создаётся.
