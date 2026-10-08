# Painel em tempo real — desenho

Data: 2026-10-08
Status: aprovado em conversa, aguardando revisão do documento

## Objetivo

Dar ao `rabbitmq-test` uma interface visual para **gerar pedidos e acompanhar o
processamento**, de forma que seja possível perceber tudo o que o projeto
demonstra: entrega pela work queue, retry com atraso, DLQ, deduplicação, fan-out
do pub/sub e RPC.

O painel combina duas visões:

1. **Caminho de cada pedido** — criado → fila → processando → concluído ou
   falhou → retry → … → DLQ, e depois o evento chegando ao estoque e à notificação.
2. **Números agregados** — quantidade de mensagens em cada fila e taxas, em tempo real.

### Fora do escopo

- Autenticação no painel (roda só em `127.0.0.1`).
- Persistência do histórico: o painel guarda os últimos 200 pedidos em memória.
- Status real no RPC: o `rpc-server` continua inventando o status pelo tamanho do
  `orderId`; a tela deixa isso explícito.
- Substituir o painel de management do RabbitMQ.

## Abordagem escolhida

**Telemetria publicada no próprio RabbitMQ.** Cada componente publica um evento
curto a cada etapa num exchange `orders.telemetry`; o servidor do painel consome
esses eventos e os repassa ao navegador por Server-Sent Events (SSE). Os
contadores vêm da API HTTP de management, consultada a cada 2 s.

Alternativas descartadas:

- **Firehose (`amq.rabbitmq.trace`)**: não exige mudar consumidores, mas só vê
  publicação e entrega — não sabe se o worker processou, falhou ou deu `ack`.
- **Só a API de management**: dá números, não o caminho de cada pedido.

## Arquitetura

```
navegador ◀──SSE── dashboard (Node, :3000) ──▶ API de management (contadores, 2 s)
    │                     │  ▲
    │ POST /api/orders    │  │ fila exclusiva ligada a orders.telemetry (#)
    │ POST /api/orders/:id/status
    ▼                     ▼  │
        orders.processing ─▶ worker ─▶ orders.events ─▶ estoque / notificação
        (RPC) ─▶ rpc-server        └──── todos emitem telemetria ────┘
```

### Componentes novos

**`src/lib/telemetry.ts`**

- `emit(channel, event)` publica em `orders.telemetry` (exchange `topic`,
  durável) com routing key `<component>.<stage>`.
- Fire-and-forget: mensagem não persistente, sem esperar confirmação. Qualquer
  erro é apenas logado; `emit` nunca lança.
- Formato do evento:

  ```ts
  interface TelemetryEvent {
    orderId: string;
    messageId?: string;
    component: 'dashboard' | 'producer' | 'publisher' | 'worker'
             | 'inventory-consumer' | 'notification-consumer';
    stage: TelemetryStage;
    attempt?: number;           // 1, 2, 3 — tentativas no worker
    at: string;                 // ISO 8601
    detail?: Record<string, unknown>; // erro, fila de retry, delay, tipo do evento…
  }
  ```

- Etapas (`TelemetryStage`): `created`, `processing`, `processed`, `failed`,
  `retry-scheduled`, `dlq`, `duplicate-skipped`, `event-published`,
  `inventory-done`, `notification-done`.

**`src/dashboard/server.ts`** — `http` nativo do Node, sem framework.

| Rota | Função |
|---|---|
| `GET /` e arquivos estáticos | página em `src/dashboard/public/` |
| `GET /events` | stream SSE: snapshot inicial dos últimos 200 pedidos, depois cada pedido atualizado, contadores e estado da conexão com o broker |
| `POST /api/orders` | corpo `{ count, failureRate, outcome }`; cria os pedidos e responde com os `orderId`s |
| `POST /api/orders/:id/status` | consulta via RPC (reusa `queryOrderStatus`); responde `{ status, updatedAt, latencyMs }` |

- Consome a telemetria com uma **fila exclusiva e temporária** ligada a
  `orders.telemetry` com `#`. Perder telemetria com o painel fechado é aceitável —
  e contrasta, didaticamente, com as filas duráveis dos assinantes.
- **`src/dashboard/order-store.ts`**: aplica cada evento de telemetria ao estado do
  pedido correspondente (lista de etapas, status atual, totais) e mantém só os
  últimos 200 pedidos. É a única cópia da lógica de estado; o navegador recebe o
  **pedido completo** a cada atualização e apenas desenha.
- **Criação de pedidos**: publica em `orders.processing` com confirm channel,
  `messageId` UUID e cabeçalho `x-simulate`, e emite `created`. Para
  `outcome = random`, sorteia `fail-once`/`fail-always`/`success` segundo
  `failureRate` (função de sorteio injetável para teste): dentre os que falham,
  metade `fail-once` e metade `fail-always`.
- **Contadores**: `GET /api/queues/%2F` da API de management a cada 2 s, filtrando
  as filas `orders.*`; envia profundidade de cada fila e as taxas de publicação e
  de `ack` de `orders.processing`.

**`src/dashboard/public/`** — `index.html`, `app.js`, `style.css`, sem build.

### Mudanças em componentes existentes

**Worker (`src/work-queue/worker.ts`)**

- O processador passa a receber `(order, msg)`. O processador padrão lê
  `x-simulate`:
  - `success` (ou ausente): espera 0,5–2 s e conclui;
  - `fail-once`: falha se `x-retry-count` = 0, conclui nas seguintes
    (percurso: falha → `orders.retry.5s` → conclui);
  - `fail-always`: sempre falha (percurso: 5 s → 30 s → DLQ).
- Em caso de sucesso, publica **`OrderProcessed`** em `orders.events` (`type` AMQP
  = `OrderProcessed`, `messageId` = `<messageId original>:processed`), espera a
  confirmação, registra a chave no `ProcessedStore` e só então dá `ack`. Se o
  evento não for confirmado, a original volta para a fila (`nack` com requeue).
  O `messageId` derivado faz os assinantes descartarem o evento repetido caso o
  pedido seja reprocessado.
- Emite: `processing` (com `attempt` = `x-retry-count` + 1), `processed`,
  `failed` (com a mensagem de erro), `retry-scheduled` (fila e delay), `dlq`,
  `duplicate-skipped`, `event-published`.

**Assinantes (`src/pubsub/subscriber.ts`)**

- Emitem `inventory-done` / `notification-done` (com o `type` do evento recebido),
  `failed` e `duplicate-skipped`.
- O log passa a mostrar o tipo do evento (`OrderCreated` vindo do publisher avulso
  ou `OrderProcessed` vindo do worker).

**Producer e publisher de linha de comando**: emitem `created`, para que pedidos
gerados pelo terminal também apareçam no painel. O publisher marca `type` =
`OrderCreated`.

**`rpc-server`**: sem mudanças; a latência é medida pelo painel.

**`src/lib/config.ts`**: `EXCHANGES.ORDERS_TELEMETRY = 'orders.telemetry'`, tipo
`SimulatedOutcome` e tipos da telemetria.

**`docker-compose.yml`**: serviço `dashboard` (`npx tsx src/dashboard/server.ts`),
porta `127.0.0.1:3000:3000`, variáveis `RABBITMQ_URL`, `RABBITMQ_MGMT_URL`
(`http://rabbitmq:15672`), `RABBITMQ_MGMT_USER`, `RABBITMQ_MGMT_PASS`. Script
`npm run dashboard` para rodar fora do Docker.

## Tela

```
┌───────────────────────────────────────────────────────────────────────────┐
│ RabbitMQ — pedidos ao vivo                                 ● conectado     │
├───────────────────────────────────────────────────────────────────────────┤
│ processing   retry.5s   retry.30s   dlq   events.inventory   events.notif │
│     3           1          0         2           0                0       │
│  ↑2,1/s ↓1,8/s                                                            │
│ Criados 48 · Concluídos 41 · Em retry 1 · Na DLQ 2 · Duplicatas 0         │
├───────────────────────────────────────────────────────────────────────────┤
│ Gerar pedidos  Quantidade [10]  Taxa de falha [====|------] 30%           │
│                Forçar resultado ( Sorteado | Sucesso | Falha 1x | Sempre )│
│                [ Enviar ]                                                  │
├───────────────────────────────────────────────────────────────────────────┤
│ ord-048  $212,40  falha 1x   created → processing → failed → retry 5s ⏳   │
│ ord-047  $ 88,10  sucesso    created → processing → processed → evento →  │
│                              estoque ✓ notificação ✓       [status RPC]   │
│ ord-045  $301,00  sempre     … → retry 30s → processing (3) → DLQ ✗        │
│   ▸ clique: log completo com horário e tempo entre etapas                  │
└───────────────────────────────────────────────────────────────────────────┘
```

- **Faixa de contadores**: profundidade de cada fila `orders.*` e, para
  `orders.processing`, taxa de entrada e de `ack`. Abaixo, totais da sessão
  calculados a partir da telemetria.
- **Formulário**: quantidade de 1 a 50; taxa de falha (0–100 %) aplicada aos
  pedidos "Sorteado"; "Forçar resultado" vale para todos os pedidos do envio.
- **Lista**: mais recente no topo; caminho em chips que crescem ao vivo, com cor
  por estado (em andamento, concluído, em retry com ⏳ durante a TTL, DLQ).
  Clique expande o log completo (horário, componente, tempo desde a etapa anterior).
  Botão **status RPC** mostra resposta e latência, com legenda de que o status é
  fictício.
- **Conexão**: indicador do SSE; reconexão automática do `EventSource`, recebendo
  de novo o snapshot ao reconectar. Mensagem "broker indisponível" quando o
  servidor perde o RabbitMQ.
- Tema claro/escuro seguindo o sistema; funciona na largura de celular.

## Tratamento de erros

| Situação | Comportamento |
|---|---|
| Falha ao emitir telemetria | log; o pedido segue normalmente |
| Broker indisponível para o dashboard | reconecta com `connectWithRetry`; SSE envia estado `broker-down` e a tela avisa |
| API de management falha | contadores mostram "—"; lista de pedidos continua |
| `POST /api/orders` inválido | 400 com mensagem: `count` 1–50, `failureRate` 0–100, `outcome` ∈ `random`/`success`/`fail-once`/`fail-always` |
| RPC sem resposta em 5 s | 504 com a mensagem de timeout; a tela mostra o erro no pedido |
| Evento `OrderProcessed` não confirmado | `nack` com requeue do pedido original (reprocessamento; duplicata do evento é absorvida pelos assinantes) |

## Testes

**Unitários (Jest, canais falsos):**

- processador simulado: os três modos de `x-simulate` e o padrão sem cabeçalho;
- worker: telemetria emitida em cada caminho (sucesso, retry, DLQ, duplicata, JSON
  inválido); publicação de `OrderProcessed` com `messageId` derivado; requeue
  quando o evento não é confirmado; ordem publicar → confirmar → registrar → `ack`;
- `telemetry.emit`: routing key correta e não lança quando `publish` lança;
- assinantes: telemetria de sucesso, falha e duplicata;
- `order-store`: montagem do estado a partir de sequências de eventos (sucesso,
  falha uma vez, DLQ, eventos fora de ordem), limite de 200 pedidos, totais;
- criação de pedidos: validação do corpo e sorteio de resultados com gerador
  injetado.

**Ponta a ponta, contra o broker real:**

- subir o compose, criar pedidos `success`, `fail-once` e `fail-always` via
  `POST /api/orders` e conferir no stream SSE as etapas esperadas — inclusive a
  chegada à DLQ após ~35 s;
- consultar o status RPC pela API;
- abrir a página no navegador e verificar a atualização ao vivo.

## Documentação

Atualizar o `README.md`: serviço `dashboard`, como abrir o painel, telemetria
(exchange, etapas), ligação work queue → pub/sub via `OrderProcessed` e o
cabeçalho `x-simulate`.
