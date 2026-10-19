# Результаты проверки

Дата: 13 октября 2026 года.

- `mvn -B package`: BUILD SUCCESS для всех шести Maven-модулей; четыре исполняемых JAR собраны.
- 13 тестов прошли, ошибок и пропущенных тестов нет:
  - OrderStoreTest — 2;
  - SupplierClientTest — 4;
  - DeliveryStoreTest — 3;
  - GatewayTest — 4.
- Проверены retry, timeout, Circuit Breaker, идемпотентность, откат DB-транзакции,
  история статусов, передача HTTP-тела/идентификатора запроса и SSE через Gateway.
- Отдельный тест получает SSE-событие до закрытия upstream-соединения.
- `docker compose config --quiet` проходит для корневого Compose и конфигурации PW8.
- PowerShell Parser не обнаружил синтаксических ошибок в scripts/demo.ps1.

Полная проверка через Docker Compose не выполнена: Docker Desktop сообщает running,
но Linux Engine возвращает 500 Internal Server Error на `docker version` и
`docker compose up --build -d`. Сборка образов и запуск контейнеров не состоялись.
Тесты DB используют H2 в PostgreSQL mode; они не заменяют проверку настоящих PostgreSQL/RabbitMQ.

После восстановления Docker Linux Engine выполнить из корневой папки:

```powershell
docker compose up --build -d
docker compose ps
Invoke-RestMethod http://localhost:8080/api/health | ConvertTo-Json -Depth 10
./PW8/scripts/demo.ps1 -IncludeFailureDemo
```
