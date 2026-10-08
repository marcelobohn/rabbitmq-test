'use strict';

// The server owns the order state (src/dashboard/order-store.ts) and sends the full
// order on every change; this page only renders what it receives.

const MAX_ROWS = 200;
const QUEUE_ORDER = [
  'orders.processing',
  'orders.retry.5s',
  'orders.retry.30s',
  'orders.dlq',
  'orders.events.inventory',
  'orders.events.notification',
];

const STATUS_LABEL = {
  queued: 'na fila',
  processing: 'processando',
  retrying: 'aguardando retry',
  processed: 'processado',
  completed: 'concluído',
  dlq: 'DLQ',
};

const OUTCOME_LABEL = {
  success: 'sucesso',
  'fail-once': 'falha 1x',
  'fail-always': 'falha sempre',
};

const COMPONENT_LABEL = {
  dashboard: 'painel',
  producer: 'producer',
  publisher: 'publisher',
  worker: 'worker',
  'inventory-consumer': 'estoque',
  'notification-consumer': 'notificação',
};

const $ = (id) => document.getElementById(id);
const els = {
  streamState: $('stream-state'),
  brokerState: $('broker-state'),
  queues: $('queues'),
  totals: $('totals'),
  form: $('order-form'),
  count: $('count'),
  rate: $('failure-rate'),
  rateValue: $('rate-value'),
  feedback: $('form-feedback'),
  orders: $('orders'),
  empty: $('empty'),
  clear: $('clear-orders'),
};

const orders = new Map();      // orderId -> latest view from the server
const rows = new Map();        // orderId -> <li>
const expanded = new Set();    // orderIds with the log open
const rpcResults = new Map();  // orderId -> { text, error }

// --- helpers -----------------------------------------------------------------

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value === undefined || value === null || value === false) continue;
    if (key === 'class') node.className = value;
    else if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value === true ? '' : value);
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

const money = new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'USD' });
const rateFmt = new Intl.NumberFormat('pt-BR', { maximumFractionDigits: 1 });

function time(iso) {
  const d = new Date(iso);
  return d.toLocaleTimeString('pt-BR', { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0');
}

function setPill(node, text, kind) {
  node.textContent = text;
  node.className = `pill pill-${kind}`;
}

// --- steps -------------------------------------------------------------------

function stepLabel(step) {
  switch (step.stage) {
    case 'created': return 'criado';
    case 'processing': return `processando (${step.attempt ?? 1})`;
    case 'failed': return step.component === 'worker' ? 'falhou' : `falhou (${COMPONENT_LABEL[step.component] ?? step.component})`;
    case 'retry-scheduled': {
      const delay = step.detail?.delayMs;
      return delay ? `retry ${delay / 1000}s` : 'retry';
    }
    case 'dlq': return 'DLQ ✗';
    case 'processed': return 'processado';
    case 'event-published': return 'evento';
    case 'inventory-done': return 'estoque ✓';
    case 'notification-done': return 'notificação ✓';
    case 'duplicate-skipped': return `duplicata (${COMPONENT_LABEL[step.component] ?? step.component})`;
    default: return step.stage;
  }
}

// Seconds left in the retry queue, while the order is waiting there
function retryCountdown(order) {
  if (order.status !== 'retrying') return null;
  const last = [...order.steps].reverse().find((s) => s.stage === 'retry-scheduled');
  if (!last?.detail?.delayMs) return null;
  const left = Math.ceil((new Date(last.at).getTime() + last.detail.delayMs - Date.now()) / 1000);
  return Math.max(left, 0);
}

function stepDetail(step) {
  const d = step.detail ?? {};
  const parts = [];
  if (d.error) parts.push(`erro: ${d.error}`);
  if (d.queue) parts.push(`fila: ${d.queue}`);
  if (d.eventType) parts.push(`evento: ${d.eventType}`);
  if (d.outcome) parts.push(`resultado: ${OUTCOME_LABEL[d.outcome] ?? d.outcome}`);
  if (d.customerId) parts.push(`cliente: ${d.customerId}`);
  if (step.messageId) parts.push(`messageId: ${step.messageId}`);
  return parts.join(' · ');
}

// --- rendering ---------------------------------------------------------------

function renderSteps(order) {
  const container = el('div', { class: 'steps' });
  order.steps.forEach((step, i) => {
    if (i > 0) container.append(el('span', { class: 'arrow', 'aria-hidden': 'true' }, '→'));
    container.append(el('span', { class: `step sg-${step.stage}` }, stepLabel(step)));
  });
  const left = retryCountdown(order);
  if (left !== null) {
    container.append(el('span', { class: 'step sg-retry-scheduled', 'data-countdown': '' }, `⏳ ${left}s`));
  }
  return container;
}

function renderLog(order) {
  const body = order.steps.map((step, i) => {
    const prev = order.steps[i - 1];
    const delta = prev ? `+${((new Date(step.at) - new Date(prev.at)) / 1000).toFixed(2)}s` : '';
    return el('tr', {},
      el('td', {}, time(step.at)),
      el('td', {}, delta),
      el('td', {}, COMPONENT_LABEL[step.component] ?? step.component),
      el('td', {}, stepLabel(step)),
      el('td', { class: 'detail' }, stepDetail(step)),
    );
  });
  return el('table', { class: 'log' },
    el('thead', {}, el('tr', {},
      el('th', {}, 'horário'), el('th', {}, 'Δ'), el('th', {}, 'componente'), el('th', {}, 'etapa'), el('th', {}, 'detalhe'))),
    el('tbody', {}, body),
  );
}

function renderOrder(order) {
  const rpc = rpcResults.get(order.orderId);
  const isOpen = expanded.has(order.orderId);

  const head = el('div', {
    class: 'order-head',
    role: 'button',
    tabindex: '0',
    'aria-expanded': String(isOpen),
    onclick: (e) => { if (!e.target.closest('button')) toggle(order.orderId); },
    onkeydown: (e) => { if ((e.key === 'Enter' || e.key === ' ') && e.target === e.currentTarget) { e.preventDefault(); toggle(order.orderId); } },
  },
    el('span', { class: 'order-id' }, order.orderId),
    order.total !== undefined ? el('span', { class: 'order-total' }, money.format(order.total)) : null,
    order.outcome ? el('span', { class: 'tag' }, OUTCOME_LABEL[order.outcome] ?? order.outcome) : null,
    order.source && order.source !== 'dashboard' ? el('span', { class: 'tag' }, `via ${COMPONENT_LABEL[order.source] ?? order.source}`) : null,
    el('span', { class: 'tag status' }, STATUS_LABEL[order.status] ?? order.status),
    el('span', { class: 'spacer' }),
    rpc ? el('span', { class: `rpc-result${rpc.error ? ' error' : ''}` }, rpc.text) : null,
    el('button', { type: 'button', onclick: () => queryStatus(order.orderId) }, 'status RPC'),
  );

  return el('li', { class: `order st-${order.status}`, 'data-id': order.orderId },
    head,
    renderSteps(order),
    isOpen ? renderLog(order) : null,
  );
}

function upsertOrder(order, { flash = false } = {}) {
  orders.set(order.orderId, order);
  const row = renderOrder(order);
  if (flash) row.classList.add('flash');

  const existing = rows.get(order.orderId);
  if (existing) {
    existing.replaceWith(row);
  } else {
    els.orders.prepend(row);
    while (els.orders.children.length > MAX_ROWS) {
      const last = els.orders.lastElementChild;
      rows.delete(last.dataset.id);
      orders.delete(last.dataset.id);
      last.remove();
    }
  }
  rows.set(order.orderId, row);
  els.empty.hidden = orders.size > 0;
}

function rerender(orderId) {
  const order = orders.get(orderId);
  if (order) upsertOrder(order);
}

function toggle(orderId) {
  if (expanded.has(orderId)) expanded.delete(orderId);
  else expanded.add(orderId);
  rerender(orderId);
}

function renderSnapshot(list) {
  orders.clear();
  rows.clear();
  els.orders.replaceChildren();
  // Drop UI state of orders that are no longer listed (e.g. after "Limpar lista")
  const ids = new Set(list.map((o) => o.orderId));
  for (const id of [...expanded]) if (!ids.has(id)) expanded.delete(id);
  for (const id of [...rpcResults.keys()]) if (!ids.has(id)) rpcResults.delete(id);
  // The server sends newest first; prepend oldest first to keep that order
  [...list].reverse().forEach((order) => upsertOrder(order));
  els.empty.hidden = orders.size > 0;
}

function renderTotals(t) {
  if (!t) return;
  els.totals.replaceChildren(
    'Criados ', el('strong', {}, t.created),
    ' · Processados ', el('strong', {}, t.processed),
    ' · Em retry ', el('strong', {}, t.retrying),
    ' · Na DLQ ', el('strong', {}, t.dlq),
    ' · Duplicatas ', el('strong', {}, t.duplicates),
  );
}

function renderQueues(queues, error) {
  if (!queues) {
    els.queues.replaceChildren(el('p', { class: 'muted' }, `— contadores indisponíveis${error ? ` (${error})` : ''}`));
    return;
  }
  const rank = (name) => { const i = QUEUE_ORDER.indexOf(name); return i === -1 ? QUEUE_ORDER.length : i; };
  const sorted = [...queues].sort((a, b) => rank(a.name) - rank(b.name) || a.name.localeCompare(b.name));

  els.queues.replaceChildren(...sorted.map((q) => {
    const classes = ['queue'];
    if (q.messages > 0) classes.push('has-messages');
    if (q.name.endsWith('.dlq')) classes.push('is-dlq');
    if (q.name.includes('.retry.')) classes.push('is-retry');
    const meta = q.name === 'orders.processing'
      ? `↑ ${rateFmt.format(q.publishRate)}/s · ↓ ${rateFmt.format(q.ackRate)}/s`
      : `${q.consumers} consumidor${q.consumers === 1 ? '' : 'es'}`;
    return el('div', { class: classes.join(' ') },
      el('div', { class: 'queue-name' }, q.name),
      el('div', { class: 'queue-depth' }, q.messages),
      el('div', { class: 'queue-meta' }, meta),
    );
  }));
}

// Keeps the ⏳ countdown of orders waiting in a retry queue ticking
setInterval(() => {
  for (const order of orders.values()) {
    if (order.status === 'retrying') rerender(order.orderId);
  }
}, 1000);

// --- actions -----------------------------------------------------------------

async function queryStatus(orderId) {
  rpcResults.set(orderId, { text: 'consultando…' });
  rerender(orderId);
  try {
    const res = await fetch(`/api/orders/${encodeURIComponent(orderId)}/status`, { method: 'POST' });
    const body = await res.json();
    if (!res.ok) throw new Error(body.error ?? `HTTP ${res.status}`);
    rpcResults.set(orderId, { text: `RPC: ${body.status} em ${body.latencyMs} ms` });
  } catch (err) {
    rpcResults.set(orderId, { text: `RPC: ${err.message}`, error: true });
  }
  rerender(orderId);
}

els.clear.addEventListener('click', async () => {
  els.clear.disabled = true;
  try {
    // The server answers every open tab with an empty snapshot
    const res = await fetch('/api/orders', { method: 'DELETE' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
  } catch (err) {
    els.feedback.className = 'feedback error';
    els.feedback.textContent = `Não foi possível limpar a lista: ${err.message}`;
  } finally {
    els.clear.disabled = false;
  }
});

els.rate.addEventListener('input', () => { els.rateValue.textContent = `${els.rate.value}%`; });

els.form.addEventListener('submit', async (e) => {
  e.preventDefault();
  const button = els.form.querySelector('button[type="submit"]');
  const outcome = new FormData(els.form).get('outcome');
  const payload = {
    count: Number(els.count.value),
    failureRate: Number(els.rate.value),
    outcome,
  };

  button.disabled = true;
  els.feedback.className = 'feedback';
  els.feedback.textContent = 'Enviando…';
  try {
    const res = await fetch('/api/orders', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const body = await res.json();
    if (!res.ok) throw new Error(body.error ?? `HTTP ${res.status}`);

    const byOutcome = body.orders.reduce((acc, o) => { acc[o.outcome] = (acc[o.outcome] ?? 0) + 1; return acc; }, {});
    const summary = Object.entries(byOutcome).map(([k, n]) => `${n} ${OUTCOME_LABEL[k] ?? k}`).join(', ');
    els.feedback.textContent = `${body.orders.length} pedido(s) confirmados pelo broker: ${summary}.`;
  } catch (err) {
    els.feedback.className = 'feedback error';
    els.feedback.textContent = `Não foi possível enviar: ${err.message}`;
  } finally {
    button.disabled = false;
  }
});

// --- live stream -------------------------------------------------------------

function connect() {
  const source = new EventSource('/events');

  source.addEventListener('open', () => setPill(els.streamState, 'ao vivo', 'ok'));
  source.addEventListener('error', () => {
    setPill(els.streamState, 'reconectando…', 'wait');
    setPill(els.brokerState, 'broker ?', 'wait');
  });

  source.addEventListener('snapshot', (e) => {
    const data = JSON.parse(e.data);
    renderSnapshot(data.orders);
    renderTotals(data.totals);
  });
  source.addEventListener('order', (e) => {
    const data = JSON.parse(e.data);
    upsertOrder(data.order, { flash: true });
    renderTotals(data.totals);
  });
  source.addEventListener('stats', (e) => {
    const data = JSON.parse(e.data);
    renderQueues(data.queues, data.error);
  });
  source.addEventListener('broker', (e) => {
    const { up } = JSON.parse(e.data);
    setPill(els.brokerState, up ? 'broker conectado' : 'broker indisponível', up ? 'ok' : 'bad');
  });
}

connect();
