package com.exemplo.pagamentos;

import com.exemplo.pagamentos.domain.PagamentoEvent;
import com.exemplo.pagamentos.inbox.Claim;
import com.exemplo.pagamentos.inbox.ClaimDecision;
import com.exemplo.pagamentos.inbox.ClaimResult;
import com.exemplo.pagamentos.inbox.InboxEvent;
import com.exemplo.pagamentos.inbox.InboxStatus;
import com.exemplo.pagamentos.inbox.InboxStore;
import com.exemplo.pagamentos.inbox.InboxTransition;
import com.exemplo.pagamentos.inbox.Resolution;
import com.exemplo.pagamentos.processing.InboxProcessor;
import com.exemplo.pagamentos.processing.InboxSweeper;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
        "app.inbox.sweeper-enabled=false",
        "app.core.connect-timeout=500ms",
        "app.core.read-timeout=1s",
        "app.inbox.lease=3s",
        "app.inbox.not-sent-backoff=1s",
        "app.inbox.max-not-sent-attempts=3"
})
@Import(TestcontainersConfig.class)
@Testcontainers(disabledWithoutDocker = true)
class InboxFlowIntegrationTest {

    private static final String TOPIC = "pagamentos";
    private static final double LEASE = 3.0;

    @RegisterExtension
    static WireMockExtension core = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
            .build();

    @DynamicPropertySource
    static void coreUrl(DynamicPropertyRegistry registry) {
        registry.add("app.core.base-url", core::baseUrl);
    }

    @Autowired
    KafkaTemplate<String, PagamentoEvent> kafka;

    @Autowired
    InboxProcessor processor;

    @Autowired
    InboxSweeper sweeper;

    @Autowired
    InboxStore store;

    @Autowired
    JdbcClient jdbc;

    @BeforeEach
    void setUp() {
        core.resetAll();
        coreResponde(201, 0);
    }

    @Test
    void mensagemDuplicadaNoKafkaChamaCoreUmaVez() {
        var event = novoEvento();
        var sentinela = new PagamentoEvent(UUID.randomUUID().toString(), event.contaId(), BigDecimal.ONE);

        for (int i = 0; i < 3; i++) {
            kafka.send(TOPIC, event.contaId(), event);
        }
        kafka.send(TOPIC, sentinela.contaId(), sentinela);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(status(sentinela)).isEqualTo(InboxStatus.DONE));

        assertThat(status(event)).isEqualTo(InboxStatus.DONE);
        verificaChamadasAoCore(event, 1);
    }

    @Test
    void crashEntreRegistrarEChamarCoreEhRetomadoPeloSweeper() {
        var event = novoEvento();
        store.claim(event, "instancia-que-caiu", LEASE);
        expiraLease(event);

        sweeper.sweep();

        assertThat(status(event)).isEqualTo(InboxStatus.DONE);
        verificaChamadasAoCore(event, 1);
    }

    @Test
    void crashEntreRegistrarEChamarCoreEhRetomadoNaReentrega() {
        var event = novoEvento();
        store.claim(event, "instancia-que-caiu", LEASE);
        expiraLease(event);

        ClaimResult result = processor.handle(event);

        assertThat(result.decision()).isEqualTo(ClaimDecision.RECLAIMED);
        assertThat(status(event)).isEqualTo(InboxStatus.DONE);
        verificaChamadasAoCore(event, 1);
    }

    @Test
    void crashDuranteChamadaAoCoreViraUnknownNoSweeperSemReenviar() {
        var event = novoEvento();
        Claim claim = store.claim(event, "instancia-que-caiu", LEASE).claim();
        store.markInFlight(event.eventId(), claim.leaseToken(), LEASE);
        expiraLease(event);

        sweeper.sweep();

        InboxEvent row = store.find(event.eventId()).orElseThrow();
        assertThat(row.status()).isEqualTo(InboxStatus.UNKNOWN);
        assertThat(row.reason()).isEqualTo("LEASE_EXPIRED_IN_FLIGHT");
        verificaChamadasAoCore(event, 0);
    }

    @Test
    void reentregaComChamadaIniciadaViraUnknownSemReenviar() {
        var event = novoEvento();
        Claim claim = store.claim(event, "instancia-que-caiu", LEASE).claim();
        store.markInFlight(event.eventId(), claim.leaseToken(), LEASE);
        expiraLease(event);

        ClaimResult result = processor.handle(event);

        assertThat(result.decision()).isEqualTo(ClaimDecision.PARKED_IN_FLIGHT);
        assertThat(store.find(event.eventId()).orElseThrow().reason()).isEqualTo("REDELIVERED_IN_FLIGHT");
        verificaChamadasAoCore(event, 0);
    }

    @Test
    void reentregaComLeaseValidaDeOutraInstanciaNaoMexeNoEvento() {
        var event = novoEvento();
        store.claim(event, "instancia-a", LEASE);

        ClaimResult result = processor.handle(event);

        assertThat(result.decision()).isEqualTo(ClaimDecision.SKIP_LEASED);
        assertThat(status(event)).isEqualTo(InboxStatus.RECEIVED);
        verificaChamadasAoCore(event, 0);
    }

    @Test
    void timeoutNoCoreViraUnknown() {
        coreResponde(201, 2_000);
        var event = novoEvento();

        processor.handle(event);

        InboxEvent row = store.find(event.eventId()).orElseThrow();
        assertThat(row.status()).isEqualTo(InboxStatus.UNKNOWN);
        assertThat(row.reason()).isEqualTo("CORE_AMBIGUOUS");
        verificaChamadasAoCore(event, 1);
    }

    @Test
    void erroDeNegocioViraFailed() {
        coreResponde(422, 0);
        var event = novoEvento();

        processor.handle(event);

        InboxEvent row = store.find(event.eventId()).orElseThrow();
        assertThat(row.status()).isEqualTo(InboxStatus.FAILED);
        assertThat(row.reason()).isEqualTo("CORE_REJECTED");
    }

    @Test
    void naoEnviadoVoltaParaReceivedComBackoffEDepoisConclui() {
        coreResponde(429, 0);
        var event = novoEvento();

        processor.handle(event);

        InboxEvent row = store.find(event.eventId()).orElseThrow();
        assertThat(row.status()).isEqualTo(InboxStatus.RECEIVED);
        assertThat(row.reason()).isEqualTo("NOT_SENT");
        assertThat(row.attempts()).isEqualTo(1);
        assertThat(row.nextAttemptAt()).isAfter(row.updatedAt());

        sweeper.sweep();
        assertThat(store.find(event.eventId()).orElseThrow().attempts()).isEqualTo(1);

        core.resetAll();
        coreResponde(201, 0);
        tornaDevido(event);
        sweeper.sweep();

        assertThat(status(event)).isEqualTo(InboxStatus.DONE);
    }

    @Test
    void naoEnviadoAposMaximoDeTentativasViraFailed() {
        coreResponde(429, 0);
        var event = novoEvento();

        processor.handle(event);
        for (int i = 0; i < 2; i++) {
            tornaDevido(event);
            sweeper.sweep();
        }

        InboxEvent row = store.find(event.eventId()).orElseThrow();
        assertThat(row.status()).isEqualTo(InboxStatus.FAILED);
        assertThat(row.reason()).isEqualTo("NOT_SENT_MAX_ATTEMPTS");
        assertThat(row.attempts()).isEqualTo(3);
    }

    @Test
    void respostaTardiaDeSucessoResolveUnknown() {
        var event = novoEvento();
        Claim claim = store.claim(event, "instancia-lenta", LEASE).claim();
        store.markInFlight(event.eventId(), claim.leaseToken(), LEASE);
        expiraLease(event);
        store.parkExpiredInFlight();
        assertThat(status(event)).isEqualTo(InboxStatus.UNKNOWN);

        store.completeSuccess(event.eventId(), "core-tardio");

        InboxEvent row = store.find(event.eventId()).orElseThrow();
        assertThat(row.status()).isEqualTo(InboxStatus.DONE);
        assertThat(row.reason()).isEqualTo("RESOLVED_BY_LATE_RESPONSE");
        assertThat(row.coreReference()).isEqualTo("core-tardio");
    }

    @Test
    void leasePerdidaNaoPermiteMarcarInFlight() {
        var event = novoEvento();
        Claim antigo = store.claim(event, "instancia-a", LEASE).claim();
        expiraLease(event);
        store.claim(event, "instancia-b", LEASE);

        assertThat(store.markInFlight(event.eventId(), antigo.leaseToken(), LEASE)).isFalse();
    }

    @Test
    void analistaPodeMandarUnknownDeVoltaParaFila() {
        coreResponde(500, 0);
        var event = novoEvento();
        processor.handle(event);
        assertThat(status(event)).isEqualTo(InboxStatus.UNKNOWN);

        assertThat(store.resolve(event.eventId(), Resolution.RETRY, "Confirmado no extrato que não executou")).isTrue();
        core.resetAll();
        coreResponde(201, 0);
        sweeper.sweep();

        assertThat(status(event)).isEqualTo(InboxStatus.DONE);
        assertThat(store.transitions(event.eventId()))
                .extracting(InboxTransition::toStatus)
                .containsExactly(
                        InboxStatus.RECEIVED, InboxStatus.IN_FLIGHT, InboxStatus.UNKNOWN,
                        InboxStatus.RECEIVED, InboxStatus.IN_FLIGHT, InboxStatus.DONE);
    }

    @Test
    void resolucaoManualSoValeParaUnknown() {
        var event = novoEvento();
        processor.handle(event);

        assertThat(store.resolve(event.eventId(), Resolution.RETRY, "tentativa indevida")).isFalse();
        assertThat(status(event)).isEqualTo(InboxStatus.DONE);
    }

    @Test
    void processamentoConcorrenteDoMesmoEventoChamaCoreUmaVez() throws Exception {
        coreResponde(201, 300);
        var event = novoEvento();
        int threads = 8;
        var largada = new CountDownLatch(1);

        try (var executor = Executors.newFixedThreadPool(threads)) {
            List<Future<ClaimResult>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(executor.submit(() -> {
                    largada.await();
                    return processor.handle(event);
                }));
            }
            largada.countDown();

            long executaram = 0;
            for (var f : futures) {
                if (f.get().decision().shouldExecute()) {
                    executaram++;
                }
            }
            assertThat(executaram).isEqualTo(1);
        }

        assertThat(status(event)).isEqualTo(InboxStatus.DONE);
        verificaChamadasAoCore(event, 1);
    }

    private PagamentoEvent novoEvento() {
        return new PagamentoEvent(UUID.randomUUID().toString(), "conta-" + UUID.randomUUID(), new BigDecimal("100.00"));
    }

    private InboxStatus status(PagamentoEvent event) {
        return store.find(event.eventId()).map(InboxEvent::status).orElse(null);
    }

    private void coreResponde(int status, int delayMs) {
        core.stubFor(post("/lancamentos").willReturn(aResponse()
                .withStatus(status)
                .withFixedDelay(delayMs)
                .withHeader("Content-Type", "application/json")
                .withBody(status < 300 ? "{\"id\":\"" + UUID.randomUUID() + "\"}" : "{\"erro\":\"simulado\"}")));
    }

    private void verificaChamadasAoCore(PagamentoEvent event, int vezes) {
        core.verify(vezes, postRequestedFor(urlEqualTo("/lancamentos"))
                .withRequestBody(matchingJsonPath("$.referencia", equalTo(event.eventId()))));
    }

    private void expiraLease(PagamentoEvent event) {
        jdbc.sql("update inbox_events set locked_until = now() - interval '1 second' where event_id = :id")
                .param("id", event.eventId())
                .update();
    }

    private void tornaDevido(PagamentoEvent event) {
        jdbc.sql("update inbox_events set next_attempt_at = now() where event_id = :id")
                .param("id", event.eventId())
                .update();
    }
}
