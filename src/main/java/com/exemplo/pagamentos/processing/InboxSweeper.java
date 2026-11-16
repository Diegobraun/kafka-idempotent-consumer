package com.exemplo.pagamentos.processing;

import com.exemplo.pagamentos.config.AppProperties;
import com.exemplo.pagamentos.inbox.Claim;
import com.exemplo.pagamentos.inbox.InboxStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class InboxSweeper {

    private static final Logger log = LoggerFactory.getLogger(InboxSweeper.class);

    private final InboxStore store;
    private final InboxProcessor processor;
    private final AppProperties props;

    public InboxSweeper(InboxStore store, InboxProcessor processor, AppProperties props) {
        this.store = store;
        this.processor = processor;
        this.props = props;
    }

    public SweepReport sweep() {
        int parked = store.parkExpiredInFlight();
        if (parked > 0) {
            log.warn("{} eventos IN_FLIGHT com lease expirada movidos para UNKNOWN", parked);
        }

        List<Claim> claims = store.claimDueReceived(
                props.instanceId(), props.inbox().leaseSeconds(), props.inbox().sweeperBatchSize());
        int failures = 0;
        for (Claim claim : claims) {
            try {
                processor.execute(claim);
            } catch (RuntimeException e) {
                failures++;
                log.error("Falha retomando evento {}, será tentado de novo após a lease", claim.eventId(), e);
            }
        }
        if (!claims.isEmpty()) {
            log.info("Sweeper retomou {} eventos RECEIVED ({} falhas)", claims.size(), failures);
        }
        return new SweepReport(parked, claims.size(), failures);
    }

    public record SweepReport(int parked, int resumed, int failures) {}
}
