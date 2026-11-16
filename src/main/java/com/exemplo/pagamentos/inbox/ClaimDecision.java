package com.exemplo.pagamentos.inbox;

public enum ClaimDecision {
    CLAIMED_NEW,
    RECLAIMED,
    SKIP_TERMINAL,
    SKIP_LEASED,
    PARKED_IN_FLIGHT;

    public boolean shouldExecute() {
        return this == CLAIMED_NEW || this == RECLAIMED;
    }
}
