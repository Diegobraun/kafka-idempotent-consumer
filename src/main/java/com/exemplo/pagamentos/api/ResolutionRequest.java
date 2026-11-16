package com.exemplo.pagamentos.api;

import com.exemplo.pagamentos.inbox.Resolution;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record ResolutionRequest(@NotNull Resolution acao, @NotBlank String nota) {}
