package com.exemplo.pagamentos.fakecore;

import com.exemplo.pagamentos.core.CoreRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Profile("fake-core")
@RestController
@RequestMapping("/fake-core/lancamentos")
public class FakeCoreController {

    private static final Logger log = LoggerFactory.getLogger(FakeCoreController.class);

    private final Map<String, AtomicInteger> execucoes = new ConcurrentHashMap<>();

    @PostMapping
    public ResponseEntity<Map<String, Object>> lancar(@RequestBody CoreRequest request) throws InterruptedException {
        String conta = request.contaId();

        if (conta.startsWith("rejeita")) {
            return ResponseEntity.unprocessableEntity().body(Map.of("erro", "conta bloqueada"));
        }
        if (conta.startsWith("limite")) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(Map.of("erro", "limite de requisições"));
        }

        int vezes = execucoes.computeIfAbsent(request.referencia(), k -> new AtomicInteger()).incrementAndGet();
        log.info("FAKE CORE executou lançamento referencia={} conta={} valor={} execucoes={}",
                request.referencia(), conta, request.valor(), vezes);

        if (conta.startsWith("erro")) {
            return ResponseEntity.internalServerError().body(Map.of("erro", "falha após executar"));
        }
        if (conta.startsWith("lento")) {
            Thread.sleep(15_000);
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("id", UUID.randomUUID().toString()));
    }

    @GetMapping("/{referencia}")
    public Map<String, Object> execucoes(@PathVariable String referencia) {
        return Map.of("referencia", referencia,
                "execucoes", execucoes.getOrDefault(referencia, new AtomicInteger()).get());
    }
}
