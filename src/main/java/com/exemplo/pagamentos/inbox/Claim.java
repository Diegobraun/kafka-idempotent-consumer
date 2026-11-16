package com.exemplo.pagamentos.inbox;

import com.exemplo.pagamentos.domain.PagamentoEvent;

import java.util.UUID;

public record Claim(String eventId, UUID leaseToken, PagamentoEvent event) {}
