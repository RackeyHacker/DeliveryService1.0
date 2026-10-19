package ru.pw8.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

import reactor.test.StepVerifier;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

class SupplierClientTest {
    HttpServer server;
    AtomicInteger count;
    SupplierClient client;

    @BeforeEach
    void setup() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        count = new AtomicInteger();
        client = new SupplierClient("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void retriesTransientFailureAndPropagatesRequestId() {
        server.createContext(
                "/suppliers/products/1",
                exchange -> {
                    assertThat(exchange.getRequestHeaders().getFirst("X-Request-ID"))
                            .isEqualTo("trace-1");
                    int status = count.incrementAndGet() == 1 ? 503 : 200;
                    byte[] body =
                            (status == 503
                                            ? "{}"
                                            : "{\"id\":1,\"supplierId\":1,\"name\":\"Product\",\"price\":10,\"availableQuantity\":5}")
                                    .getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(status, body.length);
                    exchange.getResponseBody().write(body);
                    exchange.close();
                });
        server.start();
        StepVerifier.create(client.product(1, "trace-1"))
                .assertNext(product -> assertThat(product.availableQuantity()).isEqualTo(5))
                .verifyComplete();
        assertThat(count.get()).isEqualTo(2);
    }

    @Test
    void missingProductIsNotRetried() {
        server.createContext(
                "/suppliers/products/1",
                exchange -> {
                    count.incrementAndGet();
                    exchange.sendResponseHeaders(404, -1);
                    exchange.close();
                });
        server.start();
        StepVerifier.create(client.product(1, "trace"))
                .expectErrorSatisfies(
                        error ->
                                assertThat(
                                                ((ResponseStatusException) error)
                                                        .getStatusCode()
                                                        .value())
                                        .isEqualTo(404))
                .verify();
        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    void repeatedFailureOpensCircuitAndRejectsWithoutHttpCall() {
        server.createContext(
                "/suppliers/products/1",
                exchange -> {
                    count.incrementAndGet();
                    exchange.sendResponseHeaders(503, -1);
                    exchange.close();
                });
        server.start();
        for (int i = 0; i < 4; i++)
            StepVerifier.create(client.product(1, "trace"))
                    .expectErrorSatisfies(
                            error ->
                                    assertThat(
                                                    ((ResponseStatusException) error)
                                                            .getStatusCode()
                                                            .value())
                                            .isEqualTo(503))
                    .verify();
        assertThat(client.state()).isEqualTo("OPEN");
        int before = count.get();
        StepVerifier.create(client.product(1, "trace"))
                .expectError(ResponseStatusException.class)
                .verify();
        assertThat(count.get()).isEqualTo(before);
    }

    @Test
    void slowSupplierHasBoundedTimeout() {
        server.createContext(
                "/suppliers/products/1",
                exchange -> {
                    try {
                        Thread.sleep(6000);
                        exchange.sendResponseHeaders(200, -1);
                    } catch (Exception ignored) {
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        StepVerifier.create(client.product(1, "trace"))
                .expectError(ResponseStatusException.class)
                .verify(Duration.ofSeconds(6));
    }
}
