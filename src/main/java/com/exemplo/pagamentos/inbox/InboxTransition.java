package com.exemplo.pagamentos.inbox;

import java.time.Instant;

public record InboxTransition(
        InboxStatus fromStatus,
        InboxStatus toStatus,
        String reason,
        String owner,
        int attempts,
        Instant at) {}
