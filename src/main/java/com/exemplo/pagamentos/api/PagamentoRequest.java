package com.exemplo.pagamentos.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record PagamentoRequest(
        String eventId,
        @NotBlank String contaId,
        @NotNull @Positive BigDecimal valor,
        Integer repeticoes) {}
