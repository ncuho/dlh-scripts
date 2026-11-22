# Spark Connect transport

Клиент удалённого Spark для Airflow-задач. Открывает собственный notebook/kernel через выделенный Jupyter MCP, подключается штатным `dp103_spark.connect_dlh()`, выполняет переданный видимый SQL и закрывает свою сессию. Секрет читается из mounted Secret; в коде и логах его нет. Бизнес-логики и автоматического повтора записи в клиенте нет.

Код: [spark_connect_client.py](spark_connect_client.py). При установке в Airflow копируется под именем `bridge_project_spark.py` в `managed/native_oil_spark/`, как импортировано в DAG. Endpoint относится к текущему зарегистрированному окружению; для другого развёртывания нужен проверенный transport binding.
