# Публикация проверенного DAG

`publish_reviewed_dag(artifact_id="oil-production-1.1")` читает `oil_production_spark_candidate.py` на стороне Airflow MCP и проверяет SHA-256 `1cde59335dc80a7c4883b8e1c50dc415d470876cea29ffa70ef3acf0fe688f35`. Модель не передаёт исходный Python. Иной artifact_id или изменённый файл отклоняются до обращения к S3.

`dag_publish.py` сохраняет прежний generic publish_dag и добавляет более узкую операцию. Существующий S3-клиент пишет в content-addressed `DAGs/managed/<name>__<sha>.py`; повтор возвращает already_published. Новый DAG на паузе, с одной task и прежним расписанием. Публикация не запускает DAG и не создаёт таблицы.

На стенде применяется ConfigMap `airflow-mcp-reviewed-publisher`, два read-only subPath mounts и env REVIEWED_DAG_DIR. `deployment-patch.json` описывает этот overlay без credentials. Для воспроизведения из этой папки в настроенном Kubernetes-контексте:

```bash
kubectl -n airflowmcpcluster-wnhlmhxd create configmap airflow-mcp-reviewed-publisher \
  --from-file=dag_publish.py --from-file=oil_production_spark_candidate.py \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n airflowmcpcluster-wnhlmhxd patch deployment airflow-mcp \
  --type=strategic --patch-file deployment-patch.json
kubectl -n airflowmcpcluster-wnhlmhxd rollout status deployment/airflow-mcp
```

Если обновляется содержимое ConfigMap при тех же subPath mounts, требуется rollout restart MCP, поскольку subPath не обновляет уже смонтированный файл. Не перезапускайте Airflow ради MCP. При будущем Helm-релизе перенесите overlay в штатные values. Учётные данные, ACL и существующий prefix publisher не меняются.
