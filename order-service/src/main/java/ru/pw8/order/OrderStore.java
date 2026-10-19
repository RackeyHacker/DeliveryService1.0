package ru.pw8.order;

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

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

@Service
public class OrderStore {

    private static final Logger log = LoggerFactory.getLogger(OrderStore.class);

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final ObjectMapper mapper;

    public OrderStore(JdbcTemplate jdbc, TransactionTemplate transactions, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.mapper = mapper;
    }

    public record Order(
            String id,
            long productId,
            int quantity,
            BigDecimal total,
            String address,
            String status,
            String requestId) {}

    public record StatusEvent(long id, String status, String requestId, String createdAt) {}

    public Order create(
            long productId, int quantity, String address, BigDecimal price, String requestId) {

        return transactions.execute(
                transaction -> saveOrder(productId, quantity, address, price, requestId));
    }

    public Order get(String id) {
        return jdbc.query("SELECT * FROM orders WHERE id=?", this::mapOrder, id).stream()
                .findFirst()
                .orElseThrow(
                        () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found"));
    }

    public List<Order> list() {
        return jdbc.query("SELECT * FROM orders ORDER BY created_at DESC", this::mapOrder);
    }

    public List<StatusEvent> events(String id, long after) {
        return jdbc.query(
                "SELECT * FROM order_events WHERE order_id=? AND id>? ORDER BY id",
                this::mapStatusEvent,
                id,
                after);
    }

    @RabbitListener(queues = "order.deliveries")
    public void deliveryChanged(Event event) {
        if (!"DeliveryChanged".equals(event.type()) || !validStatus(event)) {
            throw new IllegalArgumentException("Invalid delivery event");
        }

        transactions.executeWithoutResult(transaction -> applyDeliveryChange(event));
    }

    private Order saveOrder(
            long productId, int quantity, String address, BigDecimal price, String requestId) {

        String id = UUID.randomUUID().toString();
        BigDecimal total = price.multiply(BigDecimal.valueOf(quantity));

        jdbc.update(
                """
                INSERT INTO orders (
                    id, product_id, quantity, total, address, status, request_id
                )
                VALUES (?, ?, ?, ?, ?, 'CREATED', ?)
                """,
                id,
                productId,
                quantity,
                total,
                address,
                requestId);

        appendHistory(id, "CREATED", requestId);

        Event event =
                new Event(
                        UUID.randomUUID().toString(),
                        "OrderCreated",
                        id,
                        address,
                        "CREATED",
                        0,
                        requestId);
        Infrastructure.enqueue(jdbc, mapper, "order.created", event);

        log.info("requestId={} created orderId={}", requestId, id);
        return get(id);
    }

    private void applyDeliveryChange(Event event) {
        // Lock this order while checking the inbox and updating its version.
        List<Integer> versions =
                jdbc.queryForList(
                        "SELECT delivery_version FROM orders WHERE id=? FOR UPDATE",
                        Integer.class,
                        event.orderId());

        if (versions.isEmpty()) {
            throw new IllegalStateException("Order not found yet");
        }

        int processedEvents =
                jdbc.queryForObject(
                        "SELECT COUNT(*) FROM inbox WHERE event_id=?",
                        Integer.class,
                        event.eventId());
        if (processedEvents > 0) {
            return;
        }

        jdbc.update("INSERT INTO inbox(event_id) VALUES (?)", event.eventId());

        // A delayed event must not roll the order status back.
        if (event.version() <= versions.get(0)) {
            return;
        }

        jdbc.update(
                "UPDATE orders SET status=?,delivery_version=? WHERE id=?",
                event.status(),
                event.version(),
                event.orderId());
        appendHistory(event.orderId(), event.status(), event.requestId());

        log.info(
                "requestId={} orderId={} status={}",
                event.requestId(),
                event.orderId(),
                event.status());
    }

    private void appendHistory(String orderId, String status, String requestId) {
        jdbc.update(
                "INSERT INTO order_events(order_id,status,request_id) VALUES (?,?,?)",
                orderId,
                status,
                requestId);
    }

    private boolean validStatus(Event event) {
        return (event.version() == 1 && "DELIVERY_CREATED".equals(event.status()))
                || (event.version() == 2 && "IN_TRANSIT".equals(event.status()))
                || (event.version() == 3 && "DELIVERED".equals(event.status()));
    }

    private Order mapOrder(ResultSet result, int rowNumber) throws SQLException {
        return new Order(
                result.getString("id"),
                result.getLong("product_id"),
                result.getInt("quantity"),
                result.getBigDecimal("total"),
                result.getString("address"),
                result.getString("status"),
                result.getString("request_id"));
    }

    private StatusEvent mapStatusEvent(ResultSet result, int rowNumber) throws SQLException {
        return new StatusEvent(
                result.getLong("id"),
                result.getString("status"),
                result.getString("request_id"),
                result.getTimestamp("created_at").toInstant().toString());
    }
}
