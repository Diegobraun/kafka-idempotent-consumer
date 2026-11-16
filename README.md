# kafka-idempotent-consumer

Consumer Kafka que chama um sistema externo (core) **assumido como não idempotente**, com Spring Boot 3.5, Spring Kafka, JDBC e PostgreSQL.

Objetivo: **nunca executar duas vezes no core e nunca perder um evento em silêncio**. Quando não é possível saber se o core executou, o evento fica estacionado e visível para análise, em vez de ser reenviado ou esquecido.

## O problema

```
recebe mensagem ──> grava no banco ──> chama core ──> grava resultado ──> ack
                                  ▲               ▲
                         crash aqui: seguro    crash aqui: ambíguo
                         (core não recebeu)    (core pode ter executado)
```

Desenhos comuns falham assim:

- tratam "linha existe no banco" como "já executei" e descartam reentregas de eventos que nunca chegaram ao core;
- mandam para análise manual todo evento reentregue, inclusive os que comprovadamente não saíram;
- não distinguem reentrega por crash de reentrega por rebalance, gerando falsos erros e conflitos de status.

## Máquina de estados

```
            ┌──────────── NOT_SENT (backoff) ─────────────┐
            ▼                                             │
        RECEIVED ──commit──> IN_FLIGHT ──chamada ao core──┼──> DONE
            ▲                    │                        ├──> FAILED   (rejeição de negócio)
            │                    │                        └──> UNKNOWN  (resultado ambíguo)
            │                    └── crash / lease expirada ─────> UNKNOWN
            │
            └──────── analista: RETRY ──────── UNKNOWN ──> analista: DONE / FAILED
```

| Status | Significado | Garantia |
|---|---|---|
| `RECEIVED` | Registrado, chamada **não** saiu | Seguro executar, automático |
| `IN_FLIGHT` | Chamada pode ter saído | Nunca reenviado automaticamente |
| `DONE` | Core confirmou | Terminal |
| `FAILED` | Rejeição definitiva do core ou envio impossível após N tentativas | Terminal, core não executou |
| `UNKNOWN` | Não se sabe se o core executou | Estacionado, exige análise |

O ponto central é o **commit de `IN_FLIGHT` em transação própria imediatamente antes do HTTP**. Assim `RECEIVED` prova que a chamada não saiu e pode ser retomado sem risco.

## Lease

Cada execução adquire uma lease: `lease_token` (UUID único por claim) e `locked_until` (relógio do banco, `now()`).

- Toda transição de quem está executando (`RECEIVED → IN_FLIGHT`, `IN_FLIGHT → RECEIVED/UNKNOWN`) exige o token atual. Uma instância que perdeu a lease não consegue alterar o evento.
- Reentrega com lease válida de outra instância (rebalance com o primeiro ainda processando): decisão `SKIP_LEASED`, ack e nada é alterado.
- Lease expirada em `RECEIVED`: o evento é retomado (`RECLAIMED`).
- Lease expirada em `IN_FLIGHT`: o evento vai para `UNKNOWN`.
- A aplicação recusa subir se `app.inbox.lease <= connect-timeout + read-timeout`, garantindo que a lease nunca expire com uma chamada ainda em andamento.
- O tempo é sempre o do banco, imune a diferença de relógio entre instâncias.

Resultados definitivos do core vencem a lease: se a resposta de sucesso chega depois que o evento virou `UNKNOWN`, ele passa para `DONE` com `reason = RESOLVED_BY_LATE_RESPONSE`.

## Decisão ao receber uma mensagem

| Status atual | Lease válida | Decisão | Efeito |
|---|---|---|---|
| inexistente | – | `CLAIMED_NEW` | Grava `RECEIVED` e executa |
| `DONE` / `FAILED` / `UNKNOWN` | – | `SKIP_TERMINAL` | Ack |
| `RECEIVED` / `IN_FLIGHT` | sim | `SKIP_LEASED` | Ack, outra instância cuida |
| `RECEIVED` | não | `RECLAIMED` | Nova lease e executa |
| `IN_FLIGHT` | não | `PARKED_IN_FLIGHT` | `UNKNOWN` com `REDELIVERED_IN_FLIGHT` |

A leitura do status é feita com `select ... for update` e a inserção com `on conflict do nothing`, então duas instâncias recebendo o mesmo evento ao mesmo tempo nunca executam ambas.

## Classificação da resposta do core

| Resposta | Classificação | Status |
|---|---|---|
| 2xx | `Success` | `DONE` |
| 4xx (exceto abaixo) | `Rejected` | `FAILED` |
| 429 | `NotSent` | `RECEIVED` com backoff |
| 408, 409 | `Ambiguous` | `UNKNOWN` |
| 5xx | `Ambiguous` | `UNKNOWN` |
| Connection refused, connect timeout, host desconhecido | `NotSent` | `RECEIVED` com backoff |
| Read timeout, connection reset | `Ambiguous` | `UNKNOWN` |

Na dúvida, a classificação é `Ambiguous`. O cliente HTTP do JDK não faz retry de `POST`, e nenhuma camada de retry HTTP é usada.

`NotSent` volta para `RECEIVED` com backoff exponencial (`not-sent-backoff * 2^(tentativa-1)`) e é retomado pelo sweeper. Após `max-not-sent-attempts` vira `FAILED` com `NOT_SENT_MAX_ATTEMPTS`, seguro porque nenhuma tentativa chegou ao core.

Toda chamada envia `referencia = eventId` no corpo e o header `Idempotency-Key`. Se o core ignorar, nada muda. Se suportar, ganha-se idempotência de graça. A referência permite conciliar casos `UNKNOWN` com o extrato do core.

## Sweeper

Executa a cada `app.inbox.sweeper-interval`:

1. `IN_FLIGHT` com lease expirada vira `UNKNOWN` (`LEASE_EXPIRED_IN_FLIGHT`).
2. `RECEIVED` sem lease válida e com `next_attempt_at` vencido é reivindicado com `for update skip locked` e executado.

Assim o banco é a fonte da verdade: um evento gravado é sempre retomado ou estacionado, mesmo que o Kafka nunca o reentregue.

## Auditoria e análise

Um trigger grava cada mudança de status em `inbox_transitions` (de, para, motivo, instância, tentativa, horário).

| Endpoint | Uso |
|---|---|
| `GET /inbox?status=UNKNOWN` | Fila de análise |
| `GET /inbox/resumo` | Contagem por status |
| `GET /inbox/{eventId}` | Evento com histórico de transições |
| `POST /inbox/{eventId}/resolucao` | `{"acao":"DONE\|FAILED\|RETRY","nota":"..."}`, só a partir de `UNKNOWN` |

`RETRY` devolve o evento para `RECEIVED`, para quando o analista confirmar no core que a operação não foi executada.

Métricas (`/actuator/metrics`):

- `inbox.events{status}`
- `inbox.oldest.age.seconds{status=UNKNOWN|RECEIVED}`, base para alertar quando um evento fica parado.

Os endpoints de análise não têm autenticação neste exemplo.

## Estrutura

```
src/main/java/com/exemplo/pagamentos
├── api/          publicação de eventos de teste e endpoints de análise
├── config/       propriedades (com validação da lease) e Kafka
├── consumer/     @KafkaListener com ack manual
├── core/         cliente HTTP do core e classificação de resultado
├── domain/       evento
├── fakecore/     core simulado (profile fake-core)
├── inbox/        estados, política de claim e SQL da inbox
└── processing/   processador, sweeper, métricas e agendamento
```

## Rodando

Pré-requisitos: Java 21+, Maven, Docker.

```bash
docker compose up -d
mvn spring-boot:run -Dspring-boot.run.profiles=fake-core
```

O core simulado decide o comportamento pelo prefixo de `contaId` e conta quantas vezes executou cada referência:

| `contaId` começa com | Comportamento | Status esperado |
|---|---|---|
| qualquer outro | executa, 201 | `DONE` |
| `rejeita` | 422 sem executar | `FAILED` |
| `limite` | 429 sem executar | `RECEIVED` com backoff, até `FAILED` |
| `erro` | executa e responde 500 | `UNKNOWN` |
| `lento` | executa e demora 15s (acima do read timeout) | `UNKNOWN` |

```bash
curl -X POST localhost:8080/pagamentos -H 'Content-Type: application/json' \
  -d '{"eventId":"evt-1","contaId":"c1","valor":100.00,"repeticoes":5}'

curl localhost:8080/fake-core/lancamentos/evt-1
curl localhost:8080/inbox/evt-1

curl -X POST localhost:8080/pagamentos -H 'Content-Type: application/json' \
  -d '{"eventId":"evt-2","contaId":"lento-1","valor":50.00}'

curl 'localhost:8080/inbox?status=UNKNOWN'
curl -X POST localhost:8080/inbox/evt-2/resolucao -H 'Content-Type: application/json' \
  -d '{"acao":"DONE","nota":"Confirmado no extrato do core"}'
```

`evt-1` deve mostrar `execucoes: 1` mesmo com 5 envios.

## Configuração

| Propriedade | Padrão | Descrição |
|---|---|---|
| `app.instance-id` | `${HOSTNAME}` | Identificação da instância nos registros |
| `app.core.connect-timeout` | `2s` | |
| `app.core.read-timeout` | `10s` | Abaixo do SLA real do core gera `UNKNOWN` desnecessário |
| `app.inbox.lease` | `30s` | Precisa ser maior que connect + read timeout |
| `app.inbox.sweeper-interval` | `10s` | |
| `app.inbox.sweeper-batch-size` | `50` | |
| `app.inbox.max-not-sent-attempts` | `5` | |
| `app.inbox.not-sent-backoff` | `10s` | Base do backoff exponencial |
| `app.inbox.retention` | `30d` | Só `DONE` e `FAILED` são removidos, `UNKNOWN` nunca |

`max.poll.interval.ms` (300s) com `max-poll-records` (10) acomoda o pior caso de 10 chamadas lentas ao core sem rebalance. O shutdown é graceful, aguardando chamadas em andamento para não fabricar `UNKNOWN` em deploys.

## Testes

```bash
mvn test
```

Sem Docker:

- `ClaimPolicyTest`: tabela de decisão do claim.
- `CoreClientTest`: classificação com WireMock e falhas reais de rede (porta fechada, host inexistente, reset, timeout).

Com Docker (Testcontainers com PostgreSQL e Kafka, pulados sem Docker), `InboxFlowIntegrationTest`:

- mensagem duplicada no Kafka chama o core uma vez;
- crash entre registrar e chamar é retomado pelo sweeper e pela reentrega;
- crash durante a chamada vira `UNKNOWN` sem reenviar, tanto pelo sweeper quanto pela reentrega;
- reentrega com lease válida de outra instância não altera o evento;
- timeout vira `UNKNOWN`, 422 vira `FAILED`, 429 volta para `RECEIVED` com backoff e depois conclui;
- envio impossível após o máximo de tentativas vira `FAILED`;
- resposta tardia de sucesso resolve `UNKNOWN`;
- instância com lease perdida não consegue marcar `IN_FLIGHT`;
- resolução manual com `RETRY` e histórico completo de transições;
- 8 threads processando o mesmo evento chamam o core uma vez.

## Limite

Sem idempotência ou consulta por referência no core, o caso `UNKNOWN` não tem solução automática: só quem tem a informação é o core. Este projeto garante que esse caso seja raro (apenas a janela em que a requisição está no ar), visível (status, métrica, histórico) e resolvível (referência enviada ao core e endpoint de resolução).
