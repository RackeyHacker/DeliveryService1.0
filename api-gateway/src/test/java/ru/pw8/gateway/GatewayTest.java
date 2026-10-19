package ru.pw8.gateway;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import reactor.test.StepVerifier;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GatewayTest {
    static final HttpServer upstream;
    static final CountDownLatch streamClosed = new CountDownLatch(1);

    static {
        try {
            upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            upstream.setExecutor(Executors.newCachedThreadPool());
            upstream.createContext(
                    "/orders/stream/events",
                    exchange -> {
                        exchange.getResponseHeaders().set("Content-Type", "text/event-stream");
                        exchange.sendResponseHeaders(200, 0);
                        exchange.getResponseBody()
                                .write(
                                        "id:1\nevent:status\ndata:{\"status\":\"CREATED\"}\n\n"
                                                .getBytes(StandardCharsets.UTF_8));
                        exchange.getResponseBody().flush();
                        try {
                            streamClosed.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        exchange.close();
                    });
            upstream.createContext(
                    "/orders/1/events",
                    exchange -> {
                        byte[] bytes =
                                "id:1\nevent:status\ndata:{\"status\":\"CREATED\"}\n\nid:2\nevent:status\ndata:{\"status\":\"DELIVERED\"}\n\n"
                                        .getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().set("Content-Type", "text/event-stream");
                        exchange.sendResponseHeaders(200, 0);
                        exchange.getResponseBody().write(bytes);
                        exchange.close();
                    });
            upstream.createContext(
                    "/orders",
                    exchange -> {
                        byte[] body = exchange.getRequestBody().readAllBytes();
                        exchange.getResponseHeaders().set("Content-Type", "application/json");
                        exchange.getResponseHeaders()
                                .set(
                                        "X-Request-ID",
                                        exchange.getRequestHeaders().getFirst("X-Request-ID"));
                        exchange.sendResponseHeaders(201, body.length);
                        exchange.getResponseBody().write(body);
                        exchange.close();
                    });
            upstream.createContext(
                    "/actuator/health",
                    exchange -> {
                        byte[] body = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().set("Content-Type", "application/json");
                        exchange.sendResponseHeaders(200, body.length);
                        exchange.getResponseBody().write(body);
                        exchange.close();
                    });
            upstream.start();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        String url = "http://127.0.0.1:" + upstream.getAddress().getPort();
        registry.add("ORDER_URL", () -> url);
        registry.add("SUPPLIER_URL", () -> url);
        registry.add("DELIVERY_URL", () -> url);
    }

    @Autowired WebTestClient client;

    @AfterAll
    static void stop() {
        streamClosed.countDown();
        upstream.stop(0);
        ((ExecutorService) upstream.getExecutor()).shutdownNow();
    }

    @Test
    void flushesSseWhileUpstreamConnectionRemainsOpen() {
        var response =
                client.get()
                        .uri("/api/orders/stream/events")
                        .exchange()
                        .expectStatus()
                        .isOk()
                        .returnResult(String.class);
        try {
            StepVerifier.create(response.getResponseBody())
                    .expectNext("{\"status\":\"CREATED\"}")
                    .thenCancel()
                    .verify(Duration.ofSeconds(2));
        } finally {
            streamClosed.countDown();
        }
    }

    @Test
    void forwardsBodyStatusAndRequestId() {
        client.post()
                .uri("/api/orders")
                .header("X-Request-ID", "gateway-test")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"productId\":1}")
                .exchange()
                .expectStatus()
                .isCreated()
                .expectHeader()
                .valueEquals("X-Request-ID", "gateway-test")
                .expectBody()
                .json("{\"productId\":1}");
    }

    @Test
    void forwardsSseEventsWithoutLosingContentType() {
        client.get()
                .uri("/api/orders/1/events")
                .exchange()
                .expectStatus()
                .isOk()
                .expectHeader()
                .contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .expectBody(String.class)
                .value(
                        body ->
                                org.assertj.core.api.Assertions.assertThat(body)
                                        .contains("CREATED", "DELIVERED"));
    }

    @Test
    void reportsAllComponentStates() {
        client.get()
                .uri("/api/health")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$.gateway.status")
                .isEqualTo("UP")
                .jsonPath("$.orders.status")
                .isEqualTo("UP")
                .jsonPath("$.suppliers.status")
                .isEqualTo("UP")
                .jsonPath("$.deliveries.status")
                .isEqualTo("UP");
    }
}
