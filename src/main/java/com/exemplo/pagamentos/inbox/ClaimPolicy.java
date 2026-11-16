package com.exemplo.pagamentos.inbox;

public final class ClaimPolicy {

    private ClaimPolicy() {}

    public static ClaimDecision decide(InboxStatus status, boolean leaseValid) {
        if (status.isTerminalForConsumer()) {
            return ClaimDecision.SKIP_TERMINAL;
        }
        if (leaseValid) {
            return ClaimDecision.SKIP_LEASED;
        }
        return switch (status) {
            case RECEIVED -> ClaimDecision.RECLAIMED;
            case IN_FLIGHT -> ClaimDecision.PARKED_IN_FLIGHT;
            default -> throw new IllegalStateException("Status inesperado: " + status);
        };
    }
}
