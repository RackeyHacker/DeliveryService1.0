package ru.pw8.common;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record Event(
        String eventId,
        String type,
        String orderId,
        String address,
        String status,
        int version,
        String requestId) {}
