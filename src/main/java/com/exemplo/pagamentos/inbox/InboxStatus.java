package com.exemplo.pagamentos.inbox;

public enum InboxStatus {
    RECEIVED,
    IN_FLIGHT,
    DONE,
    FAILED,
    UNKNOWN;

    public boolean isTerminalForConsumer() {
        return this == DONE || this == FAILED || this == UNKNOWN;
    }
}
