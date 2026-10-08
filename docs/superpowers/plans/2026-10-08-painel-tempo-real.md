# Painel em tempo real — plano de implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** painel web que gera pedidos e mostra, ao vivo, o caminho de cada um pelo RabbitMQ e os contadores das filas.

**Architecture:** cada componente publica telemetria em `orders.telemetry` (topic) por um canal separado; um servidor `http` nativo consome a telemetria, monta o estado dos pedidos e o envia ao navegador por SSE; contadores vêm da API de management.

**Tech Stack:** Node 20, TypeScript, amqplib, tsx, Jest/ts-jest, HTML/CSS/JS sem build.

**Spec:** `docs/superpowers/specs/2026-10-08-painel-tempo-real-design.md`

## Global Constraints

- Sem dependências novas de runtime (usar `http` e `fetch` nativos).
- Telemetria nunca interfere no pedido: canal AMQP próprio, não persistente, erros só logados.
- Porta do painel publicada só em `127.0.0.1:3000`.
- `POST /api/orders`: `count` 1–50, `failureRate` 0–100, `outcome` ∈ `random`/`success`/`fail-once`/`fail-always`.
- Histórico em memória: últimos 200 pedidos.
- Ordem no worker em sucesso: processar → publicar `OrderProcessed` → confirmar → registrar no store → `ack`.

## Review Focus

1. **Exchange de telemetria inexistente** — publicar num exchange que não existe fecha o canal AMQP; a telemetria usa canal separado e todo emissor declara o exchange antes. Teste: `emit` não lança quando `publish` lança (Task 1).
2. **Telemetria fora de ordem** — eventos de componentes diferentes chegam fora de ordem; o estado é derivado das etapas ordenadas por `at` e posição canônica da etapa. Teste em `order-store` (Task 4).
3. **Corpo inválido no POST** — JSON malformado, tipos errados, valores fora da faixa → 400 com mensagem, nunca 500. Testes em `order-factory` (Task 5).
4. **Dashboard sobe antes do broker / broker cai** — SSE informa `broker: down`, tenta reconectar a cada 3 s, sem derrubar o processo. Verificação ponta a ponta (Task 7).
5. **RPC sem resposta** — timeout de 5 s vira 504 e a tela mostra o erro. Verificação ponta a ponta (Task 7).

---

### Task 1: telemetria

**Files:** Create `src/lib/telemetry.ts`, `tests/lib/telemetry.test.ts`; Modify `src/lib/config.ts`.

**Produces:**
- `EXCHANGES.ORDERS_TELEMETRY = 'orders.telemetry'`; `type SimulatedOutcome = 'success' | 'fail-once' | 'fail-always'` em `config.ts`.
- `type TelemetryStage`, `type TelemetryComponent`, `interface TelemetryEvent { orderId; messageId?; component; stage; attempt?; at; detail? }`.
- `type TelemetryInput = Omit<TelemetryEvent, 'component' | 'at'>`; `type Emit = (e: TelemetryInput) => void`; `const noopEmit: Emit`.
- `assertTelemetryExchange(channel): Promise<void>`; `emitter(channel, component): Emit`; `openTelemetry(connection, component): Promise<Emit>` (canal próprio + declaração do exchange).

- [ ] Testes: routing key `<component>.<stage>`, corpo com `at` ISO, `persistent: false`; não lança quando `publish` lança.
- [ ] Implementar; rodar; commit `feat: add telemetry emitter`.

### Task 2: worker com simulação, `OrderProcessed` e telemetria

**Files:** Modify `src/work-queue/worker.ts`, `tests/work-queue/worker.test.ts`.

**Consumes:** `Emit`, `noopEmit`, `SimulatedOutcome`, `EXCHANGES.ORDERS_EVENTS`.
**Produces:** `type OrderProcessor = (order, msg) => Promise<void>`; `createSimulatedProcessor(sleep?)`; `handleMessage(channel, msg, processor?, store?, emit?)`.

- [ ] Testes do processador simulado: sem cabeçalho e `success` concluem; `fail-once` falha com `x-retry-count` 0 e conclui com 1; `fail-always` sempre falha.
- [ ] Testes do worker: sucesso faz `publish` em `orders.events` com `type: 'OrderProcessed'` e `messageId: '<id>:processed'`, ordem `publish → waitForConfirms → ack`; evento não confirmado → `nack(msg, false, true)` e chave não registrada; telemetria por caminho (sucesso: processing, processed, event-published; retry: processing, failed, retry-scheduled com `queue`/`delayMs`; DLQ: …, dlq; duplicata: duplicate-skipped; JSON inválido: dlq).
- [ ] Implementar; `main` usa `openTelemetry`, declara `orders.events` fanout; rodar; commit.

### Task 3: assinantes, producer e publisher emitem telemetria

**Files:** Modify `src/pubsub/subscriber.ts`, `src/pubsub/inventory-consumer.ts`, `src/pubsub/notification-consumer.ts`, `src/pubsub/publisher.ts`, `src/work-queue/producer.ts`, `tests/pubsub/subscriber.test.ts`, `tests/pubsub/publisher.test.ts`.

**Produces:** `handleEvent(channel, msg, { handler, store, label, emit, doneStage })`; `startSubscriber(queue, handler, component, doneStage)`.

- [ ] Testes: sucesso emite `doneStage` com `detail.eventType`; falha emite `failed`; duplicata emite `duplicate-skipped`; publisher marca `type: 'OrderCreated'`.
- [ ] Implementar; producer/publisher emitem `created` com `detail { total, customerId, outcome }`; rodar; commit.

### Task 4: `order-store`

**Files:** Create `src/dashboard/order-store.ts`, `tests/dashboard/order-store.test.ts`.

**Produces:** `type OrderStatus = 'queued' | 'processing' | 'retrying' | 'processed' | 'completed' | 'dlq'`; `interface OrderView { orderId; total?; outcome?; source?; status; attempt; steps; delivered: { inventory; notification }; updatedAt }`; `interface Totals { created; processed; retrying; dlq; duplicates }`; `class OrderStore { constructor(max = 200); apply(e): OrderView; list(): OrderView[]; totals(): Totals }`.

- [ ] Testes: caminho de sucesso termina `completed`; `fail-once` passa por `retrying` e termina `completed` com `attempt` 2; DLQ; eventos fora de ordem dão o mesmo estado; limite de 200 (mais antigo sai); totais.
- [ ] Implementar; rodar; commit.

### Task 5: criação de pedidos e contadores

**Files:** Create `src/dashboard/order-factory.ts`, `src/dashboard/queue-stats.ts`, `tests/dashboard/order-factory.test.ts`, `tests/dashboard/queue-stats.test.ts`.

**Produces:** `type RequestedOutcome = 'random' | SimulatedOutcome`; `class ValidationError`; `parseCreateOrders(body: unknown): { count; failureRate; outcome }`; `pickOutcome(failureRate, rand?)`; `buildOrder(rand?): OrderMessage`; `summarizeQueues(json): QueueStat[]`; `fetchQueueStats(url, user, pass): Promise<QueueStat[]>`.

- [ ] Testes: validação (faixas, tipos, defaults `failureRate` 0 e `outcome` `random`); sorteio com gerador injetado (0 % nunca falha, 100 % sempre, metade fail-once/fail-always); `summarizeQueues` filtra `orders.*`, ignora `amq.gen-*`, taxa ausente vira 0.
- [ ] Implementar; rodar; commit.

### Task 6: servidor e página

**Files:** Create `src/dashboard/server.ts`, `src/dashboard/public/{index.html,app.js,style.css}`; Modify `docker-compose.yml`, `package.json`.

- [ ] Servidor: rotas da spec, SSE (`snapshot`, `order`, `stats`, `broker`), whitelist de arquivos estáticos, limite de corpo 10 KB, reconexão do consumidor de telemetria a cada 3 s, `HOST`/`PORT`/`RABBITMQ_MGMT_*` por ambiente.
- [ ] Página conforme a seção "Tela" da spec.
- [ ] Serviço `dashboard` no compose (`127.0.0.1:3000:3000`), script `npm run dashboard`; commit.

### Task 7: verificação ponta a ponta e README

- [ ] `docker compose up -d --build`; criar `success`, `fail-once`, `fail-always` via API; conferir etapas no SSE (DLQ após ~35 s); RPC; abrir no navegador.
- [ ] Atualizar README; commit.
