# Ручной запуск обработки DLH

С Mac, где уже работает SSH к кластеру:

```bash
python3 tools/run_oil_dlh/run_oil_dlh.py release --check
python3 tools/run_oil_dlh/run_oil_dlh.py catchup
python3 tools/run_oil_dlh/run_oil_dlh.py release
```

Это альтернативы нажатию Trigger соответствующего DAG. Вызывается тот же `execute()` в штатном worker, с теми же SQL, проверками и блокировкой writer. Локальные Python-пакеты Spark/Airflow и копирование секретов не нужны. Результаты сохраняются в `/opt/airflow/logs/oil_dlh_release` на worker. Airflow Run для CLI не создаётся; прогресс виден в терминале и receipt. Потерянный ответ не означает rollback: перед повтором проверьте результат по напечатанному run ID.
