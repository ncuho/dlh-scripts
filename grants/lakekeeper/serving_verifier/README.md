# Проверка действующих прав

Read-only verifier подтверждает table UUID, snapshot и effective SELECT CIAM-backed reader. Для production проверяет agg_field_day, для performance также field_performance; запрещает расширенные права на родительских объектах. Развёрнут sidecar штатного token proxy; секреты остаются внутри платформы.
