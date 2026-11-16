package com.exemplo.pagamentos.processing;

import com.exemplo.pagamentos.inbox.InboxStatus;
import com.exemplo.pagamentos.inbox.InboxStore;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class InboxMetrics implements MeterBinder {

    private final InboxStore store;
    private final Map<InboxStatus, AtomicLong> counts = new EnumMap<>(InboxStatus.class);
    private final AtomicReference<Double> oldestUnknown = new AtomicReference<>(0.0);
    private final AtomicReference<Double> oldestReceived = new AtomicReference<>(0.0);

    public InboxMetrics(InboxStore store) {
        this.store = store;
        for (InboxStatus status : InboxStatus.values()) {
            counts.put(status, new AtomicLong());
        }
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        counts.forEach((status, value) -> Gauge.builder("inbox.events", value, AtomicLong::get)
                .tag("status", status.name())
                .description("Eventos na inbox por status")
                .register(registry));
        Gauge.builder("inbox.oldest.age.seconds", oldestUnknown, AtomicReference::get)
                .tag("status", InboxStatus.UNKNOWN.name())
                .description("Idade do evento UNKNOWN mais antigo")
                .register(registry);
        Gauge.builder("inbox.oldest.age.seconds", oldestReceived, AtomicReference::get)
                .tag("status", InboxStatus.RECEIVED.name())
                .description("Idade do evento RECEIVED mais antigo")
                .register(registry);
    }

    public void refresh() {
        store.countByStatus().forEach((status, total) -> counts.get(status).set(total));
        oldestUnknown.set(store.oldestAgeSeconds(InboxStatus.UNKNOWN));
        oldestReceived.set(store.oldestAgeSeconds(InboxStatus.RECEIVED));
    }
}
