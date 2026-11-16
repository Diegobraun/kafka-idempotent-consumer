package com.exemplo.pagamentos.api;

import com.exemplo.pagamentos.config.AppProperties;
import com.exemplo.pagamentos.domain.PagamentoEvent;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
public class PagamentoController {

    private final KafkaTemplate<String, PagamentoEvent> kafka;
    private final String topic;

    public PagamentoController(KafkaTemplate<String, PagamentoEvent> kafka, AppProperties props) {
        this.kafka = kafka;
        this.topic = props.kafka().topic();
    }

    @PostMapping("/pagamentos")
    public ResponseEntity<Map<String, Object>> publicar(@Valid @RequestBody PagamentoRequest request) {
        String eventId = request.eventId() != null ? request.eventId() : UUID.randomUUID().toString();
        int repeticoes = request.repeticoes() != null ? Math.max(1, request.repeticoes()) : 1;
        var event = new PagamentoEvent(eventId, request.contaId(), request.valor());

        for (int i = 0; i < repeticoes; i++) {
            kafka.send(topic, event.contaId(), event);
        }

        return ResponseEntity.accepted().body(Map.of("eventId", eventId, "enviados", repeticoes));
    }
}
