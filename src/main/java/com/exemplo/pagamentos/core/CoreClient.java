package com.exemplo.pagamentos.core;

import com.exemplo.pagamentos.config.AppProperties;
import com.exemplo.pagamentos.domain.PagamentoEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;

@Component
public class CoreClient {

    private final RestClient rest;

    @Autowired
    public CoreClient(AppProperties props) {
        this(props.core());
    }

    public CoreClient(AppProperties.Core core) {
        HttpClient http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(core.connectTimeout())
                .build();
        var factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(core.readTimeout());
        this.rest = RestClient.builder()
                .baseUrl(core.baseUrl())
                .requestFactory(factory)
                .build();
    }

    public CoreResult lancar(PagamentoEvent event) {
        try {
            CoreResponse response = rest.post()
                    .uri("/lancamentos")
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("Idempotency-Key", event.eventId())
                    .body(new CoreRequest(event.eventId(), event.contaId(), event.valor()))
                    .retrieve()
                    .body(CoreResponse.class);
            return new CoreResult.Success(response == null ? null : response.id());
        } catch (RuntimeException e) {
            return CoreOutcomeClassifier.classify(e);
        }
    }
}
