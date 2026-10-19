package ru.pw8.order;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.netty.channel.ChannelOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.server.ResponseStatusException;

import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.util.retry.Retry;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

@Component
public class SupplierClient {

    private static final Logger log = LoggerFactory.getLogger(SupplierClient.class);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(2);

    private final WebClient client;
    private final CircuitBreaker breaker = createCircuitBreaker();

    public SupplierClient(@Value("${supplier.url}") String url) {
        HttpClient httpClient =
                HttpClient.create()
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
                        .responseTimeout(REQUEST_TIMEOUT);

        client =
                WebClient.builder()
                        .baseUrl(url)
                        .clientConnector(new ReactorClientHttpConnector(httpClient))
                        .build();

        breaker.getEventPublisher()
                .onStateTransition(event -> log.warn("Supplier circuit breaker {}", event));
    }

    public record Product(
            long id, long supplierId, String name, BigDecimal price, int availableQuantity) {}

    public Mono<Product> product(long id, String requestId) {
        Retry retry =
                Retry.backoff(1, Duration.ofMillis(200))
                        .jitter(0)
                        .filter(SupplierClient::transientFailure)
                        .onRetryExhaustedThrow((spec, signal) -> signal.failure());

        return Mono.defer(() -> requestProduct(id, requestId))
                .retryWhen(retry)
                .transformDeferred(CircuitBreakerOperator.of(breaker))
                .onErrorMap(this::supplierError);
    }

    public String state() {
        return breaker.getState().name();
    }

    static boolean transientFailure(Throwable error) {
        boolean serverError =
                error instanceof WebClientResponseException response
                        && response.getStatusCode().is5xxServerError();

        return error instanceof TimeoutException
                || error instanceof WebClientRequestException
                || serverError;
    }

    private Mono<Product> requestProduct(long id, String requestId) {
        log.info("requestId={} supplier lookup productId={}", requestId, id);

        return client.get()
                .uri("/suppliers/products/{id}", id)
                .header("X-Request-ID", requestId)
                .retrieve()
                .bodyToMono(Product.class)
                .timeout(REQUEST_TIMEOUT);
    }

    private ResponseStatusException supplierError(Throwable error) {
        if (error instanceof WebClientResponseException response
                && response.getStatusCode().value() == 404) {
            return new ResponseStatusException(HttpStatus.NOT_FOUND, "Product not found");
        }

        return new ResponseStatusException(
                HttpStatus.SERVICE_UNAVAILABLE, "Supplier unavailable. Retry later.");
    }

    private static CircuitBreaker createCircuitBreaker() {
        CircuitBreakerConfig config =
                CircuitBreakerConfig.custom()
                        .slidingWindowSize(4)
                        .minimumNumberOfCalls(4)
                        .failureRateThreshold(50)
                        .waitDurationInOpenState(Duration.ofSeconds(10))
                        .permittedNumberOfCallsInHalfOpenState(2)
                        .recordException(SupplierClient::transientFailure)
                        .build();

        return CircuitBreaker.of("supplier", config);
    }
}
