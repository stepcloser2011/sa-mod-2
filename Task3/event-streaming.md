**4\. Потоковая обработка данных**

Потоковая обработка событий отделяет синхронный RTB-путь (аукцион) от асинхронных бизнес-процессов (финансы, статистика, обновление профилей, аналитика).

В качестве центральной шины используется Apache Kafka с Avro-сериализацией и Schema Registry.

**4.1 Топики Kafka**

**Топик**

**Ключ**

**Назначение**

rtb.bid-won

auction\_id

Публикуется сервисом рекламных кампаний после каждого выигранного аукциона. Содержит campaign\_id, user\_id, win\_price, timestamp, targeting\_context. Используется для биллинга, статистики, инвентаря.

tracking.impression

impression\_id

Фиксирует показ рекламы. Содержит auction\_id, creative\_id, user\_id, viewability\_data. Генерируется сервисом ставок после ответа.

tracking.click

click\_id

Фиксирует клик по рекламе. Содержит impression\_id, click\_url, timestamp.

campaign.updated

campaign\_id

Оповещает об изменении кампании (бюджет, таргетинг, ставки, статус). Используется для обновления кешей (Redis) и пересчёта инвентаря.

finance.transaction

transaction\_id

Финансовые операции (списания, пополнения). Содержит user\_id, amount, type, status. Для аудита и синхронизации.

cdc.campaigns

campaign\_id

изменения таблиц кампаний.

cdc.finance

transaction\_id

Сырые изменения финансовых записей.

**4.2. Схема событий: Avro**

**Avro (рекомендуется как основной вариант):** сырые события (показы, клики, конверсии), ставки, лимиты — всё, где важна производительность, строгая схема и эволюция версий.

**Плюсы:**

*   Компактность (бинарный формат, меньше сетевой трафик и дискового места).
*   Строгая схема + версионирование (Schema Registry) — защита от «битых» событий.
*   Быстрая сериализация/десериализация.
*   Централизованное управление схемами через Schema Registry, валидация совместимости.

**Минусы:**

Сложнее отлаживать «в лоб» (нужен Schema Registry и инструменты).

  
**Базовые поля (в каждом событии):**

avro

{

"type": "record",

"name": "BaseEvent",

"fields": \[

{"name": "event\_id", "type": "string"},

{"name": "timestamp", "type": "long"},

{"name": "source\_service", "type": "string"},

{"name": "trace\_id", "type": "string"},

{"name": "schema\_version", "type": "int"}

\]

}

Каждое событие расширяет эти поля специфическими атрибутами.

**4.3. Группы потребителей**

Каждый микросервис подписывается как отдельная группа, что позволяет параллельно читать одни и те же топики без влияния на смещения других групп.

**Группа потребителей**

**Топики**

**Назначение**

stats-service-group

rtb.bid-won, tracking.impression, tracking.click

Агрегация и запись в ClickHouse.

billing-service-group

rtb.bid-won, finance.transaction

Запуск саг по списанию средств.

inventory-service-group

rtb.bid-won, campaign.updated

Обновление материализованных представлений инвентаря.

profile-service-group

tracking.impression, tracking.click, campaign.updated

Обогащение профилей поведенческими данными.

analytics-service-group

tracking.impression, tracking.click

Построение дашбордов (опционально, если не читает из ClickHouse напрямую).

cache-updater-group

cdc.campaigns, cdc.users, campaign.updated, user.profile.enriched

Обновление кеша Redis и локальных кешей.

**4.4. Политика хранения (Retention)**

**Топик**

**Retention (время)**

**Retention (объём)**

**Обоснование**

rtb.bid-won

**7 дней**

500 GB

восстановление после сбоев.

tracking.impression

**7 дней**

1 TB

Аналогично.

tracking.click

**7 дней**

500 GB

Аналогично.

campaign.updated

**14 дней**

100 GB

Сохранение последнего состояния кампании для новых потребителей (compaction).

finance.transaction

**30 дней**

200 GB

Финансовый аудит.