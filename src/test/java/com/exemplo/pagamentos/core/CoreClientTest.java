package com.exemplo.pagamentos.core;

import com.exemplo.pagamentos.config.AppProperties;
import com.exemplo.pagamentos.domain.PagamentoEvent;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.time.Duration;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

class CoreClientTest {

    @RegisterExtension
    static WireMockExtension core = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
            .build();

    private final PagamentoEvent event = new PagamentoEvent("evt-1", "conta-1", new BigDecimal("10.00"));

    private CoreClient client(String baseUrl) {
        return new CoreClient(new AppProperties.Core(baseUrl, Duration.ofMillis(500), Duration.ofMillis(500)));
    }

    private CoreClient client() {
        return client(core.baseUrl());
    }

    @Test
    void sucessoEnviaReferenciaEChaveDeIdempotencia() {
        core.stubFor(post("/lancamentos").willReturn(aResponse()
                .withStatus(201)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"id\":\"core-123\"}")));

        assertThat(client().lancar(event)).isEqualTo(new CoreResult.Success("core-123"));
        core.verify(postRequestedFor(urlEqualTo("/lancamentos"))
                .withHeader("Idempotency-Key", equalTo("evt-1"))
                .withRequestBody(equalToJson("{\"referencia\":\"evt-1\",\"contaId\":\"conta-1\",\"valor\":10.00}")));
    }

    @Test
    void erroDeNegocioEhRejeicaoDefinitiva() {
        core.stubFor(post("/lancamentos").willReturn(aResponse().withStatus(422).withBody("conta bloqueada")));

        assertThat(client().lancar(event)).isInstanceOf(CoreResult.Rejected.class);
    }

    @Test
    void tooManyRequestsNaoFoiExecutado() {
        core.stubFor(post("/lancamentos").willReturn(aResponse().withStatus(429)));

        assertThat(client().lancar(event)).isInstanceOf(CoreResult.NotSent.class);
    }

    @Test
    void conflitoEhAmbiguo() {
        core.stubFor(post("/lancamentos").willReturn(aResponse().withStatus(409)));

        assertThat(client().lancar(event)).isInstanceOf(CoreResult.Ambiguous.class);
    }

    @Test
    void erro5xxEhAmbiguo() {
        core.stubFor(post("/lancamentos").willReturn(aResponse().withStatus(500)));

        assertThat(client().lancar(event)).isInstanceOf(CoreResult.Ambiguous.class);
    }

    @Test
    void readTimeoutEhAmbiguo() {
        core.stubFor(post("/lancamentos").willReturn(aResponse().withStatus(201).withFixedDelay(1_500)));

        assertThat(client().lancar(event)).isInstanceOf(CoreResult.Ambiguous.class);
    }

    @Test
    void conexaoResetadaAposEnvioEhAmbigua() {
        core.stubFor(post("/lancamentos").willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));

        assertThat(client().lancar(event)).isInstanceOf(CoreResult.Ambiguous.class);
    }

    @Test
    void conexaoRecusadaNuncaSaiuDoCliente() throws IOException {
        int portaFechada;
        try (ServerSocket socket = new ServerSocket(0)) {
            portaFechada = socket.getLocalPort();
        }

        assertThat(client("http://localhost:" + portaFechada).lancar(event)).isInstanceOf(CoreResult.NotSent.class);
    }

    @Test
    void hostInexistenteNuncaSaiuDoCliente() {
        assertThat(client("http://host-que-nao-existe.invalid").lancar(event)).isInstanceOf(CoreResult.NotSent.class);
    }
}
