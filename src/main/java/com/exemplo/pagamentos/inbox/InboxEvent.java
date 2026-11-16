package com.exemplo.pagamentos.inbox;

import com.fasterxml.jackson.annotation.JsonRawValue;

import java.time.Instant;

public record InboxEvent(
        String eventId,
        InboxStatus status,
        @JsonRawValue String payload,
        String owner,
        Instant lockedUntil,
        int attempts,
        Instant nextAttemptAt,
        String reason,
        String detail,
        String coreReference,
        String resolutionNote,
        Instant createdAt,
        Instant updatedAt) {}
