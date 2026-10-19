package ru.pw8.common;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;

import java.util.Map;

@RestControllerAdvice
public class ApiErrors {
    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<Map<String, Object>> status(
            ResponseStatusException e, ServerWebExchange exchange) {
        return ResponseEntity.status(e.getStatusCode())
                .body(
                        Map.of(
                                "status",
                                e.getStatusCode().value(),
                                "message",
                                e.getReason() == null ? "Request failed" : e.getReason(),
                                "requestId",
                                exchange.getRequest().getHeaders().getFirst("X-Request-ID")));
    }

    @ExceptionHandler(WebExchangeBindException.class)
    public ResponseEntity<Map<String, Object>> validation(Exception e, ServerWebExchange exchange) {
        return ResponseEntity.badRequest()
                .body(
                        Map.of(
                                "status",
                                400,
                                "message",
                                "Invalid request fields",
                                "requestId",
                                exchange.getRequest().getHeaders().getFirst("X-Request-ID")));
    }
}
