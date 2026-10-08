# rabbitmq-test

Projeto de estudo dos três padrões clássicos de mensageria com RabbitMQ, usando um
e-commerce fictício como cenário: **work queue**, **publish/subscribe** e **RPC**.

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

## Estrutura

```
src/
├── lib/
│   ├── config.ts        # URL, nomes de filas/exchanges, delays de retry, tipos
│   └── connection.ts    # conexão compartilhada com retry e backoff exponencial
├── work-queue/
│   ├── producer.ts      # publica 10 pedidos em orders.processing
│   └── worker.ts        # consome um por vez, com retry e DLQ
├── pubsub/
│   ├── publisher.ts             # publica 3 eventos OrderCreated no exchange fanout
│   ├── inventory-consumer.ts    # "baixa estoque" (só loga)
│   └── notification-consumer.ts # "envia confirmação" (só loga)
└── rpc/
    ├── server.ts        # responde consultas de status de pedido
    └── client.ts        # envia a consulta e espera a resposta (timeout de 5 s)
tests/                   # testes unitários com amqplib mockado
rabbitmq-ecommerce.postman_collection.json
```

## Como rodar

```bash
npm install
cp .env.example .env          # RABBITMQ_URL=amqp://guest:guest@localhost:5672
docker compose up -d --build  # broker + worker + 2 consumidores pub/sub + rpc-server
```

O `docker compose` sobe:

| Serviço | O que faz |
|---|---|
| `rabbitmq` | broker; AMQP em `localhost:5672`, painel em http://localhost:15672 (`guest` / `guest`) |
| `worker` | consumidor da work queue (escala com `docker compose up --scale worker=2`) |
| `inventory-consumer` | assinante do exchange `orders.events` |
| `notification-consumer` | assinante do exchange `orders.events` |
| `rpc-server` | atende a fila `orders.status.rpc` |

Os serviços esperam o healthcheck do RabbitMQ e, mesmo assim, reconectam sozinhos
com backoff exponencial (até 10 tentativas). Linhas `Attempt 1/10 failed` logo
após subir são normais.

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
- O `ack` vai do worker **para o broker**, não para o producer. O producer publica
  e termina; não fica sabendo do resultado.

**Retry e dead-letter.** Se o processamento lança erro, o worker dá `ack` na
original e republica uma cópia com o cabeçalho `x-retry-count` incrementado:

| Tentativa | Vai para | Comportamento |
|---|---|---|
| 1ª falha | `orders.retry.5s` | TTL de 5 s; ao expirar, volta para `orders.processing` |
| 2ª falha | `orders.retry.30s` | TTL de 30 s; ao expirar, volta para `orders.processing` |
| 3ª falha | `orders.dlq` | fica parada, com o erro no cabeçalho `x-error` |
| JSON inválido | `orders.dlq` | direto, sem retry |

As filas de retry não têm consumidor: o atraso vem do `x-message-ttl` combinado
com `x-dead-letter-routing-key` apontando de volta para `orders.processing`.

O processador padrão nunca falha, então esse caminho só aparece nos testes ou
publicando mensagens pelo Postman (veja abaixo).

### 2. Publish/subscribe — `src/pubsub/`

Um evento, vários interessados.

```
publisher ──▶ (orders.events, fanout) ──┬──▶ [fila exclusiva] ──▶ inventory-consumer
                                        └──▶ [fila exclusiva] ──▶ notification-consumer
```

- O exchange `orders.events` é do tipo `fanout`: copia cada mensagem para todas as
  filas ligadas a ele, ignorando routing key.
- Cada consumidor cria uma fila anônima `exclusive` (nome `amq.gen-...`) que some
  quando ele desconecta. Consequência: eventos publicados enquanto um consumidor
  está fora do ar **não** chegam a ele.
- Em caso de erro, o consumidor faz `nack` sem requeue: a mensagem é descartada.

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

## Filas e exchanges

| Nome | Tipo | Criado por | Uso |
|---|---|---|---|
| `orders.processing` | fila durável | producer / worker | work queue; dead-letter para `orders.dlq` |
| `orders.retry.5s` | fila durável, TTL 5 s | worker | atraso da 1ª retentativa |
| `orders.retry.30s` | fila durável, TTL 30 s | worker | atraso da 2ª retentativa |
| `orders.dlq` | fila durável | worker | mensagens que esgotaram as tentativas ou são inválidas |
| `orders.events` | exchange fanout | publisher / consumidores | eventos `OrderCreated` |
| `orders.status.rpc` | fila durável | rpc-server | requisições de status |

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

Os delays de retry (`RETRY_DELAYS = [5000, 30000]`) e o número máximo de
retentativas (`MAX_RETRIES`, igual ao tamanho da lista) ficam em `src/lib/config.ts`.

## Testes

```bash
npm test
```

São 7 suítes e 28 testes, cobrindo conexão com retry, config, worker (sucesso,
retry, DLQ, JSON inválido), publisher, consumidores e client/server RPC. Usam
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
- Pub/sub com filas exclusivas perde eventos publicados enquanto o assinante está
  fora do ar; para entrega garantida, cada assinante precisaria de uma fila durável
  nomeada.
- `npm audit` aponta 38 vulnerabilidades, quase todas em dependências de
  desenvolvimento (Jest e afins). Em produção há só uma, moderada, no `uuid` < 11.1.1,
  que afeta v3/v5/v6 com `buf` informado; o projeto usa apenas `v4()` sem `buf`.
  Nenhuma foi corrigida.
