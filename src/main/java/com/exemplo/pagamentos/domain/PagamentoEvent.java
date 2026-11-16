package com.exemplo.pagamentos.domain;

import java.math.BigDecimal;

public record PagamentoEvent(String eventId, String contaId, BigDecimal valor) {

    public void validar() {
        if (eventId == null || eventId.isBlank() || eventId.length() > 64) {
            throw new IllegalArgumentException("eventId obrigatório com até 64 caracteres");
        }
        if (contaId == null || contaId.isBlank()) {
            throw new IllegalArgumentException("contaId obrigatório");
        }
        if (valor == null || valor.signum() <= 0) {
            throw new IllegalArgumentException("valor deve ser positivo");
        }
    }
}
