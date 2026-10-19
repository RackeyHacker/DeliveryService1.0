package ru.pw8.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

import ru.pw8.common.Event;

import java.util.UUID;

class DeliveryStoreTest {
    JdbcTemplate jdbc;
    DeliveryStore store;

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
                new DeliveryStore(
                        jdbc,
                        new TransactionTemplate(new DataSourceTransactionManager(ds)),
                        new ObjectMapper());
    }

    Event event(String id) {
        return new Event(id, "OrderCreated", "order-1", "Москва", "CREATED", 0, "trace-test");
    }

    @Test
    void duplicateEventsAndDifferentEventsForSameOrderCreateOneDelivery() {
        store.orderCreated(event("event-1"));
        store.orderCreated(event("event-1"));
        store.orderCreated(event("event-2"));
        assertThat(store.list()).hasSize(1);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM inbox", Integer.class)).isEqualTo(2);
    }

    @Test
    void transitionsAreOrderedAndRepeatedStatusDoesNotPublishAgain() {
        store.orderCreated(event("event-1"));
        assertThatThrownBy(() -> store.change("order-1", "DELIVERED", "trace"))
                .isInstanceOf(ResponseStatusException.class);
        assertThat(store.change("order-1", "IN_TRANSIT", "trace").version()).isEqualTo(2);
        store.change("order-1", "IN_TRANSIT", "trace");
        assertThat(store.change("order-1", "DELIVERED", "trace").version()).isEqualTo(3);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM outbox", Integer.class)).isEqualTo(3);
        assertThatThrownBy(() -> store.change("order-1", "IN_TRANSIT", "trace"))
                .isInstanceOf(ResponseStatusException.class);
    }

    @Test
    void failedOutboxSerializationRollsBackDeliveryAndInbox() throws Exception {
        var broken = org.mockito.Mockito.mock(ObjectMapper.class);
        org.mockito.Mockito.when(broken.writeValueAsString(org.mockito.ArgumentMatchers.any()))
                .thenThrow(new JsonProcessingException("failure") {});
        store =
                new DeliveryStore(
                        jdbc,
                        new TransactionTemplate(
                                new DataSourceTransactionManager(jdbc.getDataSource())),
                        broken);
        assertThatThrownBy(() -> store.orderCreated(event("event-1")))
                .isInstanceOf(IllegalStateException.class);
        assertThat(store.list()).isEmpty();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM inbox", Integer.class)).isZero();
    }
}
