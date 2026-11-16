package com.exemplo.pagamentos.processing;

import com.exemplo.pagamentos.config.AppProperties;
import com.exemplo.pagamentos.inbox.InboxStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

@Configuration
@ConditionalOnProperty(prefix = "app.inbox", name = "sweeper-enabled", havingValue = "true", matchIfMissing = true)
public class InboxScheduling implements SchedulingConfigurer {

    private static final Logger log = LoggerFactory.getLogger(InboxScheduling.class);

    private final InboxSweeper sweeper;
    private final InboxMetrics metrics;
    private final InboxStore store;
    private final AppProperties props;

    public InboxScheduling(InboxSweeper sweeper, InboxMetrics metrics, InboxStore store, AppProperties props) {
        this.sweeper = sweeper;
        this.metrics = metrics;
        this.store = store;
        this.props = props;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        registrar.addFixedDelayTask(this::sweepAndRefresh, props.inbox().sweeperInterval());
        registrar.addCronTask(this::purge, props.inbox().purgeCron());
    }

    private void sweepAndRefresh() {
        try {
            sweeper.sweep();
            metrics.refresh();
        } catch (RuntimeException e) {
            log.error("Falha no sweeper da inbox", e);
        }
    }

    private void purge() {
        int removidos = store.purgeTerminal(props.inbox().retention());
        log.info("Limpeza da inbox removeu {} eventos DONE/FAILED", removidos);
    }
}
