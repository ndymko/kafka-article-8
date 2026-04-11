# Kafka Event-Driven Microservices Platform

Пример из статьи ["Kafka для начинающих: Apache Avro и Schema Registry (практика)"](https://habr.com/ru/article/edit/1018944/) на Хабре

## Состав
- `order-service` - идемпотентный продюсер
- `inventory-service` — идемпотентный продюсер и консьюмер
- `notification-service` — идемпотентный консьюмер
- `analytics-service` — дополнительный консьюмер без свойства идемпотентности
- `dlt-processor-service` — консьюмер, потребляющий "мёртвые сообщения"
- `docker-compose.yml` — кластер Kafka из трёх брокеров + Kafka-UI + базы данных PostgreSQL

## Как запустить

1. Клонировать репозиторий
```bash
git clone https://github.com/Mitohondriyaa/kafka-article-6
cd kafka-article-6
```
2. Поднять контейнеры
```bash
docker compose up -d
```
3. Выполнить SQL-скрипты в папке `sql-scripts`
4. Запустить микросервисы (при желании можно запускать консьюмеров в нескольких инстансах)
```bash
cd order-service
./mvnw spring-boot:run

cd inventory-service
./mvnw spring-boot:run

cd notification-service
./mvnw spring-boot:run

cd analytics-service
./mvnw spring-boot:run

cd dlt-processor-service
./mvnw spring-boot:run
```
5. Развлекаться