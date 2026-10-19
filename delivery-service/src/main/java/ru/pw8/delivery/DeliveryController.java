package ru.pw8.delivery;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;

@RestController
@RequestMapping("/deliveries")
public class DeliveryController {
    private final DeliveryStore store;

    public DeliveryController(DeliveryStore store) {
        this.store = store;
    }

    public record ChangeStatus(@NotNull @Pattern(regexp = "IN_TRANSIT|DELIVERED") String status) {}

    @GetMapping
    public Mono<List<DeliveryStore.Delivery>> list() {
        return Mono.fromCallable(store::list).subscribeOn(Schedulers.boundedElastic());
    }

    @GetMapping("/by-order/{orderId}")
    public Mono<DeliveryStore.Delivery> get(@PathVariable String orderId) {
        return Mono.fromCallable(() -> store.byOrder(orderId))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @PatchMapping("/by-order/{orderId}/status")
    public Mono<DeliveryStore.Delivery> status(
            @PathVariable String orderId,
            @Valid @RequestBody ChangeStatus body,
            @RequestHeader("X-Request-ID") String requestId) {
        return Mono.fromCallable(() -> store.change(orderId, body.status(), requestId))
                .subscribeOn(Schedulers.boundedElastic());
    }
}
