package ru.pw8.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;

import ru.pw8.common.Event;

import java.math.BigDecimal;
import java.util.UUID;

class OrderStoreTest {
    JdbcTemplate jdbc;
    OrderStore store;

    @BeforeEach
    void setup() {
        var ds =
                new DriverManagerDataSource(
                        "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
                        "sa",
                        "");
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(ds);
        jdbc = new JdbcTemplate(ds);
        store =
                new OrderStore(
                        jdbc,
                        new TransactionTemplate(new DataSourceTransactionManager(ds)),
                        new ObjectMapper());
    }

    @Test
    void createPersistsOrderHistoryAndOutboxTogether() {
        var order = store.create(1, 2, "Москва", new BigDecimal("10.50"), "trace");
        assertThat(order.total()).isEqualByComparingTo("21.00");
        assertThat(store.events(order.id(), 0))
                .extracting(OrderStore.StatusEvent::status)
                .containsExactly("CREATED");
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox", Integer.class)).isEqualTo(1);
    }

    @Test
    void duplicateAndOlderDeliveryEventsNeverRegressStatus() {
        var order = store.create(1, 1, "Москва", BigDecimal.TEN, "trace");
        var delivered =
                new Event("e3", "DeliveryChanged", order.id(), null, "DELIVERED", 3, "trace");
        store.deliveryChanged(delivered);
        store.deliveryChanged(delivered);
        store.deliveryChanged(
                new Event(
                        "e1", "DeliveryChanged", order.id(), null, "DELIVERY_CREATED", 1, "trace"));
        assertThat(store.get(order.id()).status()).isEqualTo("DELIVERED");
        var events = store.events(order.id(), 0);
        assertThat(events)
                .extracting(OrderStore.StatusEvent::status)
                .containsExactly("CREATED", "DELIVERED");
        assertThat(store.events(order.id(), events.get(0).id())).hasSize(1);
    }
}
