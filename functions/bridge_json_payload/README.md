# bridge_json_payload

JVM UDF `bridge.runtime.JsonPayload` возвращает точный текст объекта payload из JSON envelope. В GP этот текст участвует в hashdiff; обычная пересериализация JSON в Spark меняет пробелы/порядок и способна изменить хеш. Функция проверяет JSON и сохраняет исходную лексическую форму. Регистрируется только в Spark-сессии через `oil_layered_spark.SparkClient.open()`, затем удаляется при закрытии. Постоянной функцией LakeKeeper не является.

Сборка: `mvn -f functions/bridge_json_payload/pom.xml package`. Артефакт `target/bridge-json-payload-v1.jar` устанавливается в `/tmp/bridge-json-payload-v1.jar` на Jupyter runtime. В текущем кластере уже установлен и проверен двумя успешными DAG. Код Spark и обработки привязан к фактическим Airflow pipelines в Data Catalog.
