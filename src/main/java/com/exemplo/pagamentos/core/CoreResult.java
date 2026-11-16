package com.exemplo.pagamentos.core;

public sealed interface CoreResult {

    record Success(String reference) implements CoreResult {}

    record Rejected(String detail) implements CoreResult {}

    record NotSent(String detail) implements CoreResult {}

    record Ambiguous(String detail) implements CoreResult {}
}
