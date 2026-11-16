package com.exemplo.pagamentos.processing;

import com.exemplo.pagamentos.config.AppProperties;
import com.exemplo.pagamentos.core.CoreClient;
import com.exemplo.pagamentos.core.CoreResult;
import com.exemplo.pagamentos.domain.PagamentoEvent;
import com.exemplo.pagamentos.inbox.Claim;
import com.exemplo.pagamentos.inbox.ClaimResult;
import com.exemplo.pagamentos.inbox.InboxStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class InboxProcessor {

    private static final Logger log = LoggerFactory.getLogger(InboxProcessor.class);

    private final InboxStore store;
    private final CoreClient core;
    private final AppProperties props;

    public InboxProcessor(InboxStore store, CoreClient core, AppProperties props) {
        this.store = store;
        this.core = core;
        this.props = props;
    }

    public ClaimResult handle(PagamentoEvent event) {
        event.validar();
        ClaimResult result = store.claim(event, props.instanceId(), props.inbox().leaseSeconds());
        log.info("Evento {} decisão={}", event.eventId(), result.decision());
        if (result.decision().shouldExecute()) {
            execute(result.claim());
        }
        return result;
    }

    public void execute(Claim claim) {
        String id = claim.eventId();
        if (!store.markInFlight(id, claim.leaseToken(), props.inbox().leaseSeconds())) {
            log.warn("Evento {} lease perdida antes da chamada ao core, nada enviado", id);
            return;
        }

        CoreResult result = core.lancar(claim.event());

        boolean registrado = switch (result) {
            case CoreResult.Success s -> store.completeSuccess(id, s.reference());
            case CoreResult.Rejected r -> store.completeRejected(id, r.detail());
            case CoreResult.NotSent n -> store.releaseNotSent(
                    id, claim.leaseToken(), n.detail(),
                    props.inbox().maxNotSentAttempts(), props.inbox().notSentBackoff());
            case CoreResult.Ambiguous a -> store.markAmbiguous(id, claim.leaseToken(), a.detail());
        };

        if (registrado) {
            log.info("Evento {} resultado do core={}", id, result);
        } else {
            log.warn("Evento {} resultado do core={} não registrado, status mudou durante a chamada", id, result);
        }
    }
}
