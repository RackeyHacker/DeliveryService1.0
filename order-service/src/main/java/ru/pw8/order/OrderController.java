package ru.pw8.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import ru.pw8.order.OrderStore.Order;
import ru.pw8.order.OrderStore.StatusEvent;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderStore store;
    private final SupplierClient supplier;

    public OrderController(OrderStore store, SupplierClient supplier) {
        this.store = store;
        this.supplier = supplier;
    }

    public record CreateOrder(
            @Positive long productId,
            @Min(1) @Max(1000) int quantity,
            @NotBlank @Size(max = 500) String address) {}

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Order> create(
            @Valid @RequestBody CreateOrder body, @RequestHeader("X-Request-ID") String requestId) {

        return supplier.product(body.productId(), requestId)
                .flatMap(product -> saveOrder(body, product, requestId));
    }

    @GetMapping
    public Mono<List<Order>> list() {
        return Mono.fromCallable(store::list).subscribeOn(Schedulers.boundedElastic());
    }

    @GetMapping("/{id}")
    public Mono<Order> get(@PathVariable String id) {
        return Mono.fromCallable(() -> store.get(id)).subscribeOn(Schedulers.boundedElastic());
    }

    @GetMapping("/resilience")
    public Map<String, String> resilience() {
        return Map.of("supplierCircuitBreaker", supplier.state());
    }

    @GetMapping(value = "/{id}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<StatusEvent>> events(
            @PathVariable String id,
            @RequestHeader(value = "Last-Event-ID", defaultValue = "0") long after) {

        if (after < 0) {
            return Flux.error(
                    new ResponseStatusException(
                            HttpStatus.BAD_REQUEST, "Last-Event-ID must be nonnegative"));
        }

        // Check that the order exists before opening the event stream.
        return get(id).flatMapMany(order -> statusStream(id, after));
    }

    private Mono<Order> saveOrder(
            CreateOrder body, SupplierClient.Product product, String requestId) {

        if (product.availableQuantity() < body.quantity()) {
            return Mono.error(
                    new ResponseStatusException(
                            HttpStatus.CONFLICT, "Insufficient product availability"));
        }

        // JDBC is blocking, so database work runs outside the HTTP event loop.
        return Mono.fromCallable(
                        () ->
                                store.create(
                                        body.productId(),
                                        body.quantity(),
                                        body.address(),
                                        product.price(),
                                        requestId))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private Flux<ServerSentEvent<StatusEvent>> statusStream(String orderId, long after) {
        return Flux.defer(
                () -> {
                    AtomicLong cursor = new AtomicLong(after);
                    return Flux.merge(statusUpdates(orderId, cursor), heartbeats());
                });
    }

    private Flux<ServerSentEvent<StatusEvent>> statusUpdates(String orderId, AtomicLong cursor) {

        return Flux.interval(Duration.ZERO, Duration.ofSeconds(1))
                .concatMap(tick -> loadEvents(orderId, cursor.get()))
                .concatMapIterable(events -> events)
                .map(event -> statusEvent(event, cursor));
    }

    private Mono<List<StatusEvent>> loadEvents(String orderId, long after) {
        return Mono.fromCallable(() -> store.events(orderId, after))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private ServerSentEvent<StatusEvent> statusEvent(StatusEvent event, AtomicLong cursor) {
        cursor.set(event.id());

        return ServerSentEvent.builder(event).id(Long.toString(event.id())).event("status").build();
    }

    private Flux<ServerSentEvent<StatusEvent>> heartbeats() {
        return Flux.interval(Duration.ofSeconds(10))
                .map(tick -> ServerSentEvent.<StatusEvent>builder().comment("keepalive").build());
    }
}
