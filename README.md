# rabbitmq-test

Projeto de estudo dos três padrões clássicos de mensageria com RabbitMQ, usando um
e-commerce fictício como cenário: **work queue**, **publish/subscribe** e **RPC**.
Inclui um **painel web em tempo real** para gerar pedidos e acompanhar o caminho de
cada um pelas filas.

> **O foco é o transporte das mensagens, não a regra de negócio.** Todo o
> "processamento" é simulado: os pedidos são gerados com dados aleatórios, o worker
> só espera um tempo e escreve no log, os consumidores só escrevem no log e o status
> devolvido pelo RPC é calculado a partir do tamanho do `orderId`. Nada é gravado
> em banco nem enviado para fora.

## Stack

- Node.js 20 + TypeScript, executado com `tsx`
- [`amqplib`](https://github.com/amqp-node/amqplib) como cliente AMQP
- RabbitMQ 3.13 com o plugin de management
- Jest + ts-jest para os testes
- Docker Compose para subir o broker e os consumidores
- Painel: servidor `http` nativo do Node + Server-Sent Events; página em HTML/CSS/JS
  sem build

## Estrutura

```
src/
├── lib/
│   ├── config.ts        # URL, nomes de filas/exchanges, delays de retry, tipos
│   ├── connection.ts    # conexão compartilhada com retry e backoff exponencial
│   ├── idempotency.ts   # registro de mensagens já processadas (deduplicação)
│   └── telemetry.ts     # eventos de telemetria publicados em orders.telemetry
├── work-queue/
│   ├── producer.ts      # publica 10 pedidos em orders.processing
│   └── worker.ts        # consome um por vez, com retry, DLQ e evento OrderProcessed
├── pubsub/
│   ├── publisher.ts             # publica 3 eventos OrderCreated no exchange fanout
│   ├── subscriber.ts            # fila durável, ack/nack e deduplicação dos assinantes
│   ├── inventory-consumer.ts    # "baixa estoque" (só loga)
│   └── notification-consumer.ts # "envia confirmação" (só loga)
├── rpc/
│   ├── server.ts        # responde consultas de status de pedido
│   └── client.ts        # envia a consulta e espera a resposta (timeout de 5 s)
└── dashboard/
    ├── server.ts        # HTTP + SSE; consome a telemetria e cria pedidos
    ├── order-store.ts   # monta o estado de cada pedido a partir da telemetria
    ├── order-factory.ts # validação do POST, sorteio de resultados, pedidos aleatórios
    ├── queue-stats.ts   # contadores via API de management
    └── public/          # index.html, app.js, style.css
tests/                   # testes unitários com amqplib mockado
rabbitmq-ecommerce.postman_collection.json
```

## Como rodar

```bash
npm install
cp .env.example .env          # RABBITMQ_URL=amqp://guest:guest@localhost:5672
docker compose up -d --build  # broker + worker + 2 assinantes + rpc-server + painel
```

O `docker compose` sobe:

| Serviço | O que faz |
|---|---|
| `rabbitmq` | broker; AMQP em `localhost:5672`, painel em http://localhost:15672 (`guest` / `guest`) |
| `worker` | consumidor da work queue (escala com `docker compose up --scale worker=2`) |
| `inventory-consumer` | assinante do exchange `orders.events` (fila `orders.events.inventory`) |
| `notification-consumer` | assinante do exchange `orders.events` (fila `orders.events.notification`) |
| `rpc-server` | atende a fila `orders.status.rpc` |
| `dashboard` | painel em http://localhost:3000 (só em `127.0.0.1`) |

Os serviços esperam o healthcheck do RabbitMQ e tentam conectar com backoff
exponencial (até 10 tentativas); linhas `Attempt 1/10 failed` logo após subir são
normais. **Se o broker cair depois, só o painel reconecta sozinho**: worker,
assinantes e rpc-server param de consumir e precisam de
`docker compose restart worker inventory-consumer notification-consumer rpc-server`.
As mensagens nas filas duráveis não se perdem.

Com os consumidores no ar, dispare os produtores pela máquina local:

```bash
npm run work-queue:producer   # 10 pedidos para o worker
npm run pubsub:publisher      # 3 eventos para os dois assinantes
npm run rpc:client [orderId]  # consulta de status (padrão: ord-001)
docker compose logs -f        # acompanhar os consumidores
```

Também é possível rodar os consumidores fora do Docker (`npm run work-queue:worker`,
`pubsub:inventory`, `pubsub:notification`, `rpc:server`), desde que o RabbitMQ esteja
no ar.

Para parar: `docker compose down`.

## Painel em tempo real — `src/dashboard/`

Abra http://localhost:3000 (ou `npm run dashboard` fora do Docker).

- **Filas**: mensagens em cada fila `orders.*` e, em `orders.processing`, taxas de
  entrada e de `ack`. Vêm da API de management, consultada a cada 2 s — que por sua
  vez atualiza as estatísticas com alguns segundos de atraso.
- **Gerar pedidos**: de 1 a 50 por envio. Em *Sorteado*, a taxa de falha decide
  cada pedido: dos que falham, metade falha uma vez (recupera no retry de 5 s) e
  metade falha sempre (5 s → 30 s → DLQ, uns 35 s). *Sucesso*, *Falha 1x* e
  *Falha sempre* valem para todo o envio.
- **Pedidos**: o caminho de cada um cresce ao vivo (⏳ conta o tempo restante na fila
  de retry). Clique para ver o log com horário, componente e intervalo entre etapas.
  **status RPC** faz a consulta pelo `rpc-server` e mostra resposta e latência.

### Como o painel enxerga os pedidos: telemetria

Cada componente publica um evento curto a cada etapa no exchange `orders.telemetry`
(tipo `topic`, routing key `<componente>.<etapa>`). O painel consome tudo (`#`) por
uma fila exclusiva e temporária — se o painel está fechado, não há o que mostrar —
e repassa ao navegador por SSE.

| Etapa | Quem emite |
|---|---|
| `created` | painel, producer, publisher |
| `processing` (com a tentativa) · `failed` · `retry-scheduled` · `dlq` · `processed` · `event-published` | worker |
| `inventory-done` · `notification-done` | assinantes |
| `duplicate-skipped` | worker e assinantes |

A telemetria é *fire-and-forget*: canal AMQP próprio, mensagens não persistentes,
sem confirmação; uma falha nela é só logada e nunca afeta o pedido.

O servidor monta o estado de cada pedido (`order-store.ts`) ordenando as etapas pelo
horário, porque eventos de componentes diferentes podem chegar fora de ordem, e envia
ao navegador o pedido completo a cada mudança. Guarda os últimos 200 pedidos em
memória.

### Simulação de falha: cabeçalho `x-simulate`

O painel grava em cada pedido o cabeçalho `x-simulate`, lido pelo processador
simulado do worker:

| Valor | Comportamento |
|---|---|
| `success` (ou ausente) | conclui |
| `fail-once` | falha na 1ª tentativa (`x-retry-count` = 0) e conclui no retry |
| `fail-always` | falha sempre e termina na DLQ |

### API do painel

| Rota | Função |
|---|---|
| `GET /events` | stream SSE: `snapshot`, `order`, `stats`, `broker` |
| `POST /api/orders` | corpo `{ "count": 1-50, "failureRate": 0-100, "outcome": "random" \| "success" \| "fail-once" \| "fail-always" }`; 400 se inválido, 503 sem broker |
| `POST /api/orders/:id/status` | consulta RPC; `{ status, updatedAt, latencyMs }`, 504 em timeout |

## Os três padrões

### 1. Work queue — `src/work-queue/`

Distribui tarefas entre workers. Quem produz não espera quem executa.

```
producer ──publica──▶ [orders.processing] ──entrega──▶ worker
 (encerra)                     ▲                        │ processa
                               └──────── ack ───────────┘ (broker apaga a mensagem)
```

- A fila `orders.processing` é `durable` e as mensagens são `persistent`:
  sobrevivem a um restart do broker.
- `prefetch(1)`: o broker só entrega a próxima mensagem depois do `ack` da anterior.
  Com vários workers, quem está livre recebe a próxima.
- O `ack` vai do worker **para o broker**, não para o producer. O producer não
  fica sabendo do resultado do processamento.
- O producer usa *publisher confirms*: antes de encerrar, espera
  (`waitForConfirms`) o broker confirmar que gravou todas as mensagens.
- Cada mensagem leva um `messageId` (UUID), usado pelo worker para deduplicar
  (veja [Idempotência](#idempotência)).

**Retry e dead-letter.** Se o processamento lança erro, o worker publica uma cópia
com o cabeçalho `x-retry-count` incrementado (mantendo o `messageId`), **espera o
broker confirmar a cópia** e só então dá `ack` na original. Se o broker não
confirmar, a original volta para a fila (`nack` com requeue) em vez de ser
descartada. Assim, um crash no meio do caminho pode no máximo duplicar a mensagem
(o que a deduplicação absorve), nunca perdê-la. Para isso o worker usa um
*confirm channel*.

| Tentativa | Vai para | Comportamento |
|---|---|---|
| 1ª falha | `orders.retry.5s` | TTL de 5 s; ao expirar, volta para `orders.processing` |
| 2ª falha | `orders.retry.30s` | TTL de 30 s; ao expirar, volta para `orders.processing` |
| 3ª falha | `orders.dlq` | fica parada, com o erro no cabeçalho `x-error` |
| JSON inválido | `orders.dlq` | direto, sem retry |

As filas de retry não têm consumidor: o atraso vem do `x-message-ttl` combinado
com `x-dead-letter-routing-key` apontando de volta para `orders.processing`.

Por padrão o processador simulado não falha; para ver retry e DLQ, gere pedidos pelo
painel com falha (veja [Simulação de falha](#simulação-de-falha-cabeçalho-x-simulate))
ou publique pelo Postman.

**Ligação com o pub/sub.** Quando conclui um pedido, o worker publica o evento
`OrderProcessed` em `orders.events` (com `messageId` = `<messageId do pedido>:processed`),
espera a confirmação, registra o pedido como processado e só então dá `ack`. Assim,
pedido concluído dispara estoque e notificação. Se o evento não for confirmado, o
pedido volta para a fila; o `messageId` derivado faz os assinantes descartarem o
evento repetido.

### 2. Publish/subscribe — `src/pubsub/`

Um evento, vários interessados.

```
publisher (OrderCreated)  ──┐
                            ├──▶ (orders.events, fanout) ──┬──▶ [orders.events.inventory]    ──▶ inventory-consumer
worker (OrderProcessed)   ──┘                              └──▶ [orders.events.notification] ──▶ notification-consumer
```

- O exchange `orders.events` é do tipo `fanout`: copia cada mensagem para todas as
  filas ligadas a ele, ignorando routing key.
- Cada assinante tem sua **fila durável e nomeada**, ligada ao exchange. Ela
  continua existindo quando o consumidor cai: eventos publicados enquanto ele está
  fora do ar ficam guardados e são entregues quando ele volta.
- Dois produtores de eventos: o publisher avulso (`OrderCreated`) e o worker
  (`OrderProcessed`, a cada pedido concluído). O tipo vai no campo AMQP `type` e
  aparece no log dos assinantes.
- O publisher usa *publisher confirms* e marca cada evento com um `messageId`.
- Os dois assinantes compartilham `subscriber.ts`: `prefetch(1)`, deduplicação e
  `ack`. Em caso de erro, o consumidor faz `nack` sem requeue: a mensagem é
  descartada (não há retry nem DLQ no pub/sub).

### 3. RPC — `src/rpc/`

Pergunta e resposta sobre filas.

```
client ──{orderId}, replyTo, correlationId──▶ [orders.status.rpc] ──▶ server
   ▲                                                                    │
   └──────────── [fila de resposta exclusiva] ◀── resposta, correlationId ┘
```

- O client cria uma fila de resposta temporária, envia a requisição com `replyTo`
  e um `correlationId` (UUID) e espera.
- O server responde na fila `replyTo` repetindo o `correlationId`; o client ignora
  respostas com outro id.
- Sem resposta em 5 s, o client rejeita com `RPC timeout`.
- O status é fictício: `['pending', 'processing', 'shipped', 'delivered']` indexado
  por `orderId.length % 4`. `ord-001` (7 caracteres) sempre devolve `delivered`.

## Idempotência

RabbitMQ garante entrega *pelo menos uma vez*: depois de uma reconexão, de um
retry ou de uma republicação, a mesma mensagem pode chegar de novo. Para não
processar duas vezes, o worker e os assinantes consultam um `ProcessedStore`
(`src/lib/idempotency.ts`):

1. A chave é o `messageId` da mensagem; sem `messageId` (por exemplo, mensagens
   publicadas pelo Postman), usa o `orderId`.
2. Se a chave já foi processada, a mensagem recebe `ack` e é ignorada
   (`Duplicate message ... — skipped` no log).
3. Senão, processa, registra a chave e só então dá `ack`.

A chave só é registrada depois de um processamento bem-sucedido, então uma
mensagem que falhou e voltou pelo retry é processada normalmente.

A implementação incluída, `InMemoryProcessedStore`, guarda até 10 000 chaves na
memória do processo. **Ela é suficiente para a demonstração, não para produção:**
se perde quando o processo reinicia e não é compartilhada entre réplicas
(`--scale worker=2`). Em produção, o `ProcessedStore` seria uma tabela com chave
única ou um `SET NX` no Redis, de preferência na mesma transação do trabalho
realizado. Mesmo assim sobra uma janela: se o processo morrer depois de processar
e antes de registrar a chave, a mensagem é processada de novo.

## Filas e exchanges

| Nome | Tipo | Criado por | Uso |
|---|---|---|---|
| `orders.processing` | fila durável | producer / worker | work queue; dead-letter para `orders.dlq` |
| `orders.retry.5s` | fila durável, TTL 5 s | worker | atraso da 1ª retentativa |
| `orders.retry.30s` | fila durável, TTL 30 s | worker | atraso da 2ª retentativa |
| `orders.dlq` | fila durável | worker | mensagens que esgotaram as tentativas ou são inválidas |
| `orders.events` | exchange fanout | publisher / worker / assinantes | eventos `OrderCreated` e `OrderProcessed` |
| `orders.events.inventory` | fila durável | inventory-consumer | cópia dos eventos para o estoque |
| `orders.events.notification` | fila durável | notification-consumer | cópia dos eventos para notificação |
| `orders.status.rpc` | fila durável | rpc-server | requisições de status |
| `orders.telemetry` | exchange topic | todos | telemetria para o painel |
| `amq.gen-…` | fila exclusiva | painel, client RPC | telemetria do painel; respostas do RPC |

## Formato das mensagens

Pedido (work queue e pub/sub):

```json
{
  "orderId": "ord-001",
  "customerId": "cust-42",
  "items": [{ "productId": "prod-17", "quantity": 3, "price": 54.10 }],
  "total": 397.1,
  "createdAt": "2026-10-08T13:34:37.578Z"
}
```

No producer da work queue, `total` também é sorteado e não corresponde a
quantidade × preço.

RPC: requisição `{ "orderId": "ord-001" }`, resposta
`{ "orderId": "ord-001", "status": "delivered", "updatedAt": "..." }`.

## Configuração

| Variável | Padrão | Observação |
|---|---|---|
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672` | dentro do Docker Compose aponta para `rabbitmq:5672` |
| `RABBITMQ_MGMT_URL` | `http://localhost:15672` | painel: API de management |
| `RABBITMQ_MGMT_USER` / `RABBITMQ_MGMT_PASS` | `guest` / `guest` | painel |
| `HOST` / `PORT` | `127.0.0.1` / `3000` | painel; no Docker, `0.0.0.0` publicado só em `127.0.0.1:3000` |

Os delays de retry (`RETRY_DELAYS = [5000, 30000]`) e o número máximo de
retentativas (`MAX_RETRIES`, igual ao tamanho da lista) ficam em `src/lib/config.ts`.

## Testes

```bash
npm test
```

São 13 suítes e 86 testes, cobrindo conexão com retry, config, idempotência,
telemetria, worker (sucesso, retry, DLQ, JSON inválido, confirmação antes do ack,
duplicatas, `OrderProcessed`, simulação de falha), publisher, assinantes, client/server
RPC e o painel (estado dos pedidos, inclusive fora de ordem; validação do POST;
sorteio de resultados; contadores). Usam
canais e conexões falsos (`jest.fn()` / `jest.mock`), sem precisar de broker.

## Postman

`rabbitmq-ecommerce.postman_collection.json` usa a **API HTTP do painel de
management** (porta 15672), não o protocolo AMQP. Pastas:

- **Monitoramento**: overview, filas, exchanges, conexões e consumidores
- **Work Queue**: enviar pedido, simular mensagem em retry, espiar `orders.dlq` e
  `orders.retry.5s`
- **Pub/Sub**: publicar `OrderCreated` e ver os bindings do exchange
- **RPC**: publicar requisição de status e ver a fila
- **Cenários de teste**: payload inválido (cai na DLQ), mensagem no último retry,
  purgar filas

A coleção já define as variáveis `{{base_url}}` (`http://localhost:15672`) e
`{{vhost}}` (`%2F`, o vhost padrão) e autenticação basic `guest` / `guest`.

## Limitações conhecidas

- Processamento, estoque, notificação e status são simulados (só logs).
- A deduplicação é em memória: não sobrevive a restart nem funciona entre
  várias réplicas (veja [Idempotência](#idempotência)).
- O pub/sub não tem retry nem DLQ: um evento cujo processamento falha é descartado.
- Worker, assinantes e rpc-server não reconectam se o broker cair depois de
  conectados; é preciso reiniciá-los (o painel reconecta).
- O painel não tem autenticação (por isso só escuta em `127.0.0.1`) e guarda o
  histórico só em memória.
- `npm audit` aponta 38 vulnerabilidades, quase todas em dependências de
  desenvolvimento (Jest e afins). Em produção há só uma, moderada, no `uuid` < 11.1.1,
  que afeta v3/v5/v6 com `buf` informado; o projeto usa apenas `v4()` sem `buf`.
  Nenhuma foi corrigida.
