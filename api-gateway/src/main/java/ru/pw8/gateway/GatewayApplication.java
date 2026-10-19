package ru.pw8.gateway;

import io.netty.channel.ChannelOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@SpringBootApplication
@RestController
public class GatewayApplication {

    private static final Logger log = LoggerFactory.getLogger(GatewayApplication.class);
    private static final String REQUEST_ID_HEADER = "X-Request-ID";

    // These headers describe a single connection and must not be forwarded.
    private static final Set<String> HOP_BY_HOP_HEADERS =
            Set.of(
                    "connection",
                    "keep-alive",
                    "proxy-authenticate",
                    "proxy-authorization",
                    "te",
                    "trailer",
                    "transfer-encoding",
                    "upgrade",
                    "host");

    private final WebClient client;
    private final Map<String, String> upstreams;

    public GatewayApplication(
            @Value("${ORDER_URL:http://order-service:8080}") String order,
            @Value("${DELIVERY_URL:http://delivery-service:8080}") String delivery,
            @Value("${SUPPLIER_URL:http://supplier-service:8080}") String supplier) {

        upstreams = Map.of("orders", order, "deliveries", delivery, "suppliers", supplier);

        HttpClient httpClient =
                HttpClient.create()
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
                        .responseTimeout(Duration.ofSeconds(15));

        client =
                WebClient.builder()
                        .clientConnector(new ReactorClientHttpConnector(httpClient))
                        .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }

    @Bean
    WebFilter trace() {
        return (exchange, chain) -> {
            String candidate = exchange.getRequest().getHeaders().getFirst(REQUEST_ID_HEADER);
            String requestId =
                    candidate != null && candidate.matches("[A-Za-z0-9._-]{1,80}")
                            ? candidate
                            : UUID.randomUUID().toString();

            var request =
                    exchange.getRequest().mutate().header(REQUEST_ID_HEADER, requestId).build();
            var traced = exchange.mutate().request(request).build();

            traced.getResponse().getHeaders().set(REQUEST_ID_HEADER, requestId);
            log.info(
                    "requestId={} {} {}",
                    requestId,
                    exchange.getRequest().getMethod(),
                    exchange.getRequest().getPath());

            return chain.filter(traced);
        };
    }

    @RequestMapping({
        "/api/orders", "/api/orders/**",
        "/api/deliveries", "/api/deliveries/**",
        "/api/suppliers", "/api/suppliers/**"
    })
    public Mono<Void> proxy(ServerWebExchange exchange) {
        String path = exchange.getRequest().getURI().getRawPath().substring(4);
        String service = path.substring(1).split("/", 2)[0];
        String query = exchange.getRequest().getURI().getRawQuery();
        String uri = upstreams.get(service) + path + (query == null ? "" : "?" + query);

        return client.method(exchange.getRequest().getMethod())
                .uri(URI.create(uri))
                .headers(headers -> copyHeaders(exchange.getRequest().getHeaders(), headers))
                .body(BodyInserters.fromDataBuffers(exchange.getRequest().getBody()))
                .exchangeToMono(response -> forwardResponse(response, exchange))
                .onErrorResume(error -> serviceUnavailable(exchange, service, error));
    }

    @GetMapping("/api/health")
    public Mono<Map<String, Object>> health() {
        return Flux.fromIterable(upstreams.entrySet())
                .flatMap(
                        entry ->
                                checkHealth(entry.getValue())
                                        .map(result -> Map.entry(entry.getKey(), result)))
                .collectMap(Map.Entry::getKey, entry -> (Object) entry.getValue())
                .map(
                        result -> {
                            result.put("gateway", Map.of("status", "UP"));
                            return result;
                        });
    }

    private void copyHeaders(HttpHeaders source, HttpHeaders target) {
        source.forEach(
                (name, values) -> {
                    if (!HOP_BY_HOP_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
                        target.put(name, values);
                    }
                });
    }

    private Mono<Void> forwardResponse(ClientResponse response, ServerWebExchange exchange) {
        exchange.getResponse().setStatusCode(response.statusCode());
        copyHeaders(response.headers().asHttpHeaders(), exchange.getResponse().getHeaders());

        Flux<DataBuffer> body = response.bodyToFlux(DataBuffer.class);
        boolean isEventStream =
                response.headers()
                        .contentType()
                        .map(type -> MediaType.TEXT_EVENT_STREAM.isCompatibleWith(type))
                        .orElse(false);

        if (isEventStream) {
            // Flush each SSE chunk immediately, without waiting for the stream to end.
            return exchange.getResponse().writeAndFlushWith(body.map(Mono::just));
        }

        return exchange.getResponse().writeWith(body);
    }

    private Mono<Void> serviceUnavailable(
            ServerWebExchange exchange, String service, Throwable error) {

        String requestId = exchange.getRequest().getHeaders().getFirst(REQUEST_ID_HEADER);
        log.warn("requestId={} upstream={} unavailable: {}", requestId, service, error.toString());

        if (exchange.getResponse().isCommitted()) {
            return Mono.error(error);
        }

        exchange.getResponse().setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);
        exchange.getResponse().getHeaders().remove(HttpHeaders.CONTENT_LENGTH);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);

        String body =
                "{\"status\":503,\"message\":\"Service temporarily unavailable\","
                        + "\"requestId\":\""
                        + requestId
                        + "\"}";
        DataBuffer buffer =
                exchange.getResponse().bufferFactory().wrap(body.getBytes(StandardCharsets.UTF_8));

        return exchange.getResponse().writeWith(Mono.just(buffer));
    }

    private Mono<Map<String, Object>> checkHealth(String serviceUrl) {
        return client.get()
                .uri(serviceUrl + "/actuator/health")
                .exchangeToMono(
                        response ->
                                response.bodyToMono(
                                                new ParameterizedTypeReference<
                                                        Map<String, Object>>() {})
                                        .defaultIfEmpty(Map.of("status", "DOWN")))
                .timeout(Duration.ofSeconds(2))
                .onErrorReturn(Map.of("status", "UNAVAILABLE"));
    }
}
