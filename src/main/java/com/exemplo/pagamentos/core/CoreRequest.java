package com.exemplo.pagamentos.core;

import java.math.BigDecimal;

public record CoreRequest(String referencia, String contaId, BigDecimal valor) {}
