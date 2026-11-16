package com.exemplo.pagamentos.inbox;

public record ClaimResult(ClaimDecision decision, Claim claim) {

    public static ClaimResult of(ClaimDecision decision) {
        return new ClaimResult(decision, null);
    }
}
