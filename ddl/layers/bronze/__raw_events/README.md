# bronze.__raw_events

Landing для независимого Kafka Connect sink. Сохраняет исходный JSON и координаты доставки. Runtime читает закреплённый snapshot с перекрытием offsets и проверяет полноту окна. Таблица не очищается откатом INITIAL/REFRESH и не является control table source GP. Сервисной identity sink выдаётся MODIFY через LakeKeeper; бизнес-читателю raw недоступен.
