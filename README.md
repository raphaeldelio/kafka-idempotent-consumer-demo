# Kafka Idempotent Consumer Demo
This repo accompanies the article [Kafka Idempotent Consumer & Transactional Outbox](https://medium.com/lydtech-consulting/kafka-idempotent-consumer-transactional-outbox-74b304815550).

The purpose of this demo is to prove that an idempotent consumer can be implemented using Kafka and Spring Boot and the Transactional Outbox Pattern. 

The idempotency is ensured for database writes and message production. It does not ensure idempotency for third party service calls.

These flows are basically:
1. Reading a message from a Kafka topic.
2. Writing to a database.
3. Writing to a Kafka topic.

Let's break them through to see how they achieve it.

## Non-Idempotent Flow
1. Read message from inbound topic.
2. Begins database transaction.
3. Write to database.
4. Write to outbound topic.
5. Commit database transaction.
6. Acknowledge message from inbound topic.

Takeways:
- Message is redelivered if it fails anywhere before step 6.
- Generates duplicate messages on outbound topic if process fails after step 4. 
- Inserts duplicate in the database if process fails after step 5.

This approach can lead to duplicate messages in the outbound topic and duplicate database records.

## Idempotent Flow 
1. Read message from inbound topic.
2. Begins database transaction.
3. Write message ID to the database and flush. This is the idempotency check.
4. Write to database.
5. Write to outbound topic.
6. Commit database transaction.
7. Acknowledge message from inbound topic.

Takeaways:
- Message is redelivered if it fails anywhere before step 7.
- Generates duplicate messages on outbound topic if process fails after step 5.
- Does not insert duplicate in the database if process fails at any point.

This approach can lead to duplicate messages in the outbound topic but not duplicate database records.

## Idempotent Consumer & Transactional Outbox Flow
1. Read message from inbound topic.
2. Begins database transaction.
3. Write message ID to the database and flush. This is the idempotency check.
4. Write to database.
5. Write to outbound table on the database.
6. Commit database transaction.
7. Acknowledge message from inbound topic.
8. Debezium writes an event to the outbound topic via Change Data Capture (CDC).

Takeaways:
- Message is redelivered if it fails anywhere before step 7.
- Does not generate duplicate messages on outbound topic or duplicate database records if process fails at any point.

This approach does not lead to duplicate messages in the outbound topic or duplicate database records.

## Important
- None of these approaches ensure idempotency for third party service calls.
- Debezium shouldn't generate duplicate messages on the outbound topic if configured correctly. (see [Debezium](https://debezium.io/blog/2023/06/22/towards-exactly-once-delivery/#:~:text=Exactly%2Donce%20delivery%20(or%20semantic,will%20be%20delivered%20exactly%20once.) docs).
- However, to ensure deduplication, all consumers should be idempotent.

## Running The Demo
TBD