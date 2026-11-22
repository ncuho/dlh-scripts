# source_fence

Удерживает живую границу на время build: lease Kafka, запрет producer write, pause GP sink и GP advisory lock. При потере подтверждения прекращает работу.

Исходный код: [source_fence.py](source_fence.py).
