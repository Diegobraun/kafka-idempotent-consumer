package com.exemplo.pagamentos.inbox;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class ClaimPolicyTest {

    @ParameterizedTest(name = "{0} leaseValida={1} -> {2}")
    @CsvSource({
            "DONE,      false, SKIP_TERMINAL",
            "DONE,      true,  SKIP_TERMINAL",
            "FAILED,    false, SKIP_TERMINAL",
            "UNKNOWN,   false, SKIP_TERMINAL",
            "RECEIVED,  true,  SKIP_LEASED",
            "IN_FLIGHT, true,  SKIP_LEASED",
            "RECEIVED,  false, RECLAIMED",
            "IN_FLIGHT, false, PARKED_IN_FLIGHT"
    })
    void decide(InboxStatus status, boolean leaseValid, ClaimDecision esperado) {
        assertThat(ClaimPolicy.decide(status, leaseValid)).isEqualTo(esperado);
    }
}
