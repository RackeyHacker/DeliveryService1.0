package ru.pw8.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.WebFilter;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableScheduling
@Import(ApiErrors.class)
public class Infrastructure {

    public static final String EXCHANGE = "business.events";
    private static final Logger log = LoggerFactory.getLogger(Infrastructure.class);

    @Bean
    Jackson2JsonMessageConverter converter(ObjectMapper mapper) {
        return new Jackson2JsonMessageConverter(mapper);
    }

    @Bean
    Declarables topology() {
        var deliveryOrders =
                QueueBuilder.durable("delivery.orders")
                        .deadLetterExchange("business.dead")
                        .deadLetterRoutingKey("dead")
                        .build();
        var orderDeliveries =
                QueueBuilder.durable("order.deliveries")
                        .deadLetterExchange("business.dead")
                        .deadLetterRoutingKey("dead")
                        .build();

        return new Declarables(
                new TopicExchange(EXCHANGE, true, false),
                new DirectExchange("business.dead", true, false),
                deliveryOrders,
                orderDeliveries,
                new Queue("business.dead.queue", true),
                new Binding(
                        "delivery.orders",
                        Binding.DestinationType.QUEUE,
                        EXCHANGE,
                        "order.created",
                        null),
                new Binding(
                        "order.deliveries",
                        Binding.DestinationType.QUEUE,
                        EXCHANGE,
                        "delivery.changed",
                        null),
                new Binding(
                        "business.dead.queue",
                        Binding.DestinationType.QUEUE,
                        "business.dead",
                        "dead",
                        null));
    }

    public static String requestId(String value) {
        return value != null && value.matches("[A-Za-z0-9._-]{1,80}")
                ? value
                : UUID.randomUUID().toString();
    }

    @Bean
    WebFilter traceFilter() {
        return (exchange, chain) -> {
            String id = requestId(exchange.getRequest().getHeaders().getFirst("X-Request-ID"));
            var request = exchange.getRequest().mutate().header("X-Request-ID", id).build();
            var traced = exchange.mutate().request(request).build();

            traced.getResponse().getHeaders().set("X-Request-ID", id);
            log.info(
                    "requestId={} {} {}",
                    id,
                    exchange.getRequest().getMethod(),
                    exchange.getRequest().getPath());

            return chain.filter(traced);
        };
    }

    @Bean
    OutboxPublisher publisher(
            JdbcTemplate jdbc,
            TransactionTemplate transactions,
            RabbitTemplate rabbit,
            ObjectMapper mapper) {

        rabbit.setMandatory(true);
        return new OutboxPublisher(jdbc, transactions, rabbit, mapper);
    }

    public static void enqueue(
            JdbcTemplate jdbc, ObjectMapper mapper, String routingKey, Event event) {

        try {
            jdbc.update(
                    "INSERT INTO outbox(event_id,routing_key,payload) VALUES (?,?,?)",
                    event.eventId(),
                    routingKey,
                    mapper.writeValueAsString(event));
        } catch (JsonProcessingException error) {
            throw new IllegalStateException(error);
        }
    }

    public static class OutboxPublisher {

        private final JdbcTemplate jdbc;
        private final TransactionTemplate transactions;
        private final RabbitTemplate rabbit;
        private final ObjectMapper mapper;

        OutboxPublisher(
                JdbcTemplate jdbc,
                TransactionTemplate transactions,
                RabbitTemplate rabbit,
                ObjectMapper mapper) {

            this.jdbc = jdbc;
            this.transactions = transactions;
            this.rabbit = rabbit;
            this.mapper = mapper;
        }

        @Scheduled(fixedDelay = 1000)
        public void publish() {
            try {
                transactions.executeWithoutResult(status -> publishBatch());
            } catch (Exception error) {
                log.warn("Outbox retry: {}", error.getMessage());
            }
        }

        private void publishBatch() {
            // Locks prevent concurrent publishers from taking the same outbox rows.
            var rows =
                    jdbc.queryForList(
                            "SELECT * FROM outbox ORDER BY sequence LIMIT 20 FOR UPDATE SKIP"
                                + " LOCKED");

            for (var row : rows) {
                publishEvent(row.get("routing_key").toString(), row.get("payload").toString());
            }
        }

        private void publishEvent(String routingKey, String payload) {
            try {
                Event event = mapper.readValue(payload, Event.class);
                CorrelationData correlation = new CorrelationData(event.eventId());

                rabbit.convertAndSend(
                        EXCHANGE,
                        routingKey,
                        event,
                        message -> addEventHeaders(message, event),
                        correlation);

                // Keep the outbox entry unless the broker confirms successful routing.
                var confirmation = correlation.getFuture().get(3, TimeUnit.SECONDS);
                if (!confirmation.isAck() || correlation.getReturned() != null) {
                    throw new IllegalStateException("Message not routed or acknowledged");
                }

                jdbc.update("DELETE FROM outbox WHERE event_id=?", event.eventId());
                log.info(
                        "requestId={} published={} orderId={}",
                        event.requestId(),
                        event.type(),
                        event.orderId());
            } catch (Exception error) {
                throw new IllegalStateException("Broker unavailable; outbox retained", error);
            }
        }

        private Message addEventHeaders(Message message, Event event) {
            message.getMessageProperties().setHeader("X-Request-ID", event.requestId());
            message.getMessageProperties().setMessageId(event.eventId());
            return message;
        }
    }
}
