package ru.pw8.delivery;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

import ru.pw8.common.Event;
import ru.pw8.common.Infrastructure;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

@Service
public class DeliveryStore {

    private static final Logger log = LoggerFactory.getLogger(DeliveryStore.class);

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final ObjectMapper mapper;

    public DeliveryStore(JdbcTemplate jdbc, TransactionTemplate transactions, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.mapper = mapper;
    }

    public record Delivery(String id, String orderId, String address, String status, int version) {}

    public List<Delivery> list() {
        return jdbc.query("SELECT * FROM deliveries ORDER BY created_at DESC", this::mapDelivery);
    }

    public Delivery byOrder(String orderId) {
        return jdbc
                .query("SELECT * FROM deliveries WHERE order_id=?", this::mapDelivery, orderId)
                .stream()
                .findFirst()
                .orElseThrow(
                        () ->
                                new ResponseStatusException(
                                        HttpStatus.NOT_FOUND, "Delivery not found"));
    }

    @RabbitListener(queues = "delivery.orders")
    public void orderCreated(Event event) {
        if (!"OrderCreated".equals(event.type())
                || event.orderId() == null
                || event.address() == null) {
            throw new IllegalArgumentException("Invalid order event");
        }

        transactions.executeWithoutResult(transaction -> createDelivery(event));
    }

    public Delivery change(String orderId, String next, String requestId) {
        return transactions.execute(transaction -> changeStatus(orderId, next, requestId));
    }

    private void createDelivery(Event event) {
        int processed =
                jdbc.update(
                        "INSERT INTO inbox(event_id) VALUES (?) ON CONFLICT DO NOTHING",
                        event.eventId());

        // RabbitMQ may redeliver an event that was already processed.
        if (processed == 0) {
            return;
        }

        int inserted =
                jdbc.update(
                        """
                        INSERT INTO deliveries (
                            id, order_id, address, status, version, request_id
                        )
                        VALUES (?, ?, ?, 'DELIVERY_CREATED', 1, ?)
                        ON CONFLICT DO NOTHING
                        """,
                        UUID.randomUUID().toString(),
                        event.orderId(),
                        event.address(),
                        event.requestId());

        // A different event ID for the same order must not create a second delivery.
        if (inserted == 0) {
            return;
        }

        emit(event.orderId(), "DELIVERY_CREATED", 1, event.requestId());
        log.info("requestId={} delivery created orderId={}", event.requestId(), event.orderId());
    }

    private Delivery changeStatus(String orderId, String next, String requestId) {
        List<Delivery> deliveries =
                jdbc.query(
                        "SELECT * FROM deliveries WHERE order_id=? FOR UPDATE",
                        this::mapDelivery,
                        orderId);

        if (deliveries.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Delivery not found");
        }

        Delivery current = deliveries.get(0);
        if (current.status().equals(next)) {
            return current;
        }

        if (!isTransitionAllowed(current.status(), next)) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT,
                    "Allowed transitions: DELIVERY_CREATED -> IN_TRANSIT -> DELIVERED");
        }

        int version = current.version() + 1;
        jdbc.update(
                """
                UPDATE deliveries
                SET status=?, version=?, request_id=?
                WHERE order_id=?
                """,
                next,
                version,
                requestId,
                orderId);

        emit(orderId, next, version, requestId);
        log.info("requestId={} orderId={} delivery status={}", requestId, orderId, next);

        return byOrder(orderId);
    }

    private boolean isTransitionAllowed(String current, String next) {
        return (current.equals("DELIVERY_CREATED") && next.equals("IN_TRANSIT"))
                || (current.equals("IN_TRANSIT") && next.equals("DELIVERED"));
    }

    private void emit(String orderId, String status, int version, String requestId) {
        Event event =
                new Event(
                        UUID.randomUUID().toString(),
                        "DeliveryChanged",
                        orderId,
                        null,
                        status,
                        version,
                        requestId);
        Infrastructure.enqueue(jdbc, mapper, "delivery.changed", event);
    }

    private Delivery mapDelivery(ResultSet result, int rowNumber) throws SQLException {
        return new Delivery(
                result.getString("id"),
                result.getString("order_id"),
                result.getString("address"),
                result.getString("status"),
                result.getInt("version"));
    }
}
