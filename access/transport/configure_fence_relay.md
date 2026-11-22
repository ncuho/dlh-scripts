# configure_fence_relay

Настраивает ограниченный SSH relay от Airflow к Kafka fence с forced command и закреплёнными host keys.

Исходный код: [configure_fence_relay.py](configure_fence_relay.py).

До запуска задайте `OIL_GP_SSH_CONFIG` — путь к существующей SSH-конфигурации административного доступа.
