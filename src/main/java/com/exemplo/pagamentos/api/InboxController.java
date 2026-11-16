package com.exemplo.pagamentos.api;

import com.exemplo.pagamentos.inbox.InboxEvent;
import com.exemplo.pagamentos.inbox.InboxStatus;
import com.exemplo.pagamentos.inbox.InboxStore;
import com.exemplo.pagamentos.inbox.InboxTransition;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/inbox")
public class InboxController {

    private final InboxStore store;

    public InboxController(InboxStore store) {
        this.store = store;
    }

    @GetMapping
    public List<InboxEvent> listar(@RequestParam(defaultValue = "UNKNOWN") InboxStatus status,
                                   @RequestParam(defaultValue = "50") int limit) {
        return store.listByStatus(status, Math.min(limit, 500));
    }

    @GetMapping("/resumo")
    public Map<InboxStatus, Long> resumo() {
        return store.countByStatus();
    }

    @GetMapping("/{eventId}")
    public ResponseEntity<Detalhe> detalhe(@PathVariable String eventId) {
        return store.find(eventId)
                .map(e -> ResponseEntity.ok(new Detalhe(e, store.transitions(eventId))))
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{eventId}/resolucao")
    public ResponseEntity<Map<String, String>> resolver(@PathVariable String eventId,
                                                        @Valid @RequestBody ResolutionRequest request) {
        if (store.resolve(eventId, request.acao(), request.nota())) {
            return ResponseEntity.ok(Map.of("eventId", eventId, "acao", request.acao().name()));
        }
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("erro", "Evento inexistente ou fora do status UNKNOWN"));
    }

    public record Detalhe(InboxEvent evento, List<InboxTransition> transicoes) {}
}
