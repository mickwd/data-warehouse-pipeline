// Cliente del dashboard: recibe un snapshot inicial y luego ventanas en vivo por WebSocket.
"use strict";

const MAX_POINTS = 90;
const CHANNELS = ["web", "mobile", "store", "marketplace"];
const COLORS = { web: "#38bdf8", mobile: "#a78bfa", store: "#f59e0b", marketplace: "#22c55e" };

const money = new Intl.NumberFormat("es-AR", { style: "currency", currency: "ARS", maximumFractionDigits: 0 });
const timeLabel = (iso) => new Date(iso).toLocaleTimeString("es-AR", { hour12: false });

Chart.defaults.color = "#94a3b8";
Chart.defaults.borderColor = "#334155";

const totalChart = new Chart(document.getElementById("chart-total"), {
  type: "line",
  data: { labels: [], datasets: [{ label: "Monto", data: [], borderColor: "#38bdf8", tension: 0.3, pointRadius: 0, fill: false }] },
  options: { animation: false, plugins: { legend: { display: false } } },
});

const channelChart = new Chart(document.getElementById("chart-channels"), {
  type: "bar",
  data: {
    labels: [],
    datasets: CHANNELS.map((c) => ({ label: c, data: [], backgroundColor: COLORS[c], stack: "events" })),
  },
  options: { animation: false, scales: { x: { stacked: true }, y: { stacked: true } } },
});

const slidingChart = new Chart(document.getElementById("chart-sliding"), {
  type: "line",
  data: {
    labels: [],
    datasets: [
      { label: "Promedio", data: [], borderColor: "#a78bfa", pointRadius: 0, tension: 0.3 },
      { label: "p95", data: [], borderColor: "#f97316", pointRadius: 0, tension: 0.3 },
    ],
  },
  options: { animation: false },
});

// Ventanas tumbling por canal agrupadas por inicio de ventana
const channelBuckets = new Map();

function pushPoint(chart, label, values) {
  const labels = chart.data.labels;
  let idx = labels.indexOf(label);
  if (idx === -1) {
    labels.push(label);
    chart.data.datasets.forEach((ds, i) => ds.data.push(values[i] ?? null));
    if (labels.length > MAX_POINTS) {
      labels.shift();
      chart.data.datasets.forEach((ds) => ds.data.shift());
    }
  } else {
    chart.data.datasets.forEach((ds, i) => { if (values[i] !== undefined) ds.data[idx] = values[i]; });
  }
}

function handleWindow(w) {
  const label = timeLabel(w.window_end);

  if (w.window_type === "tumbling" && w.group_key === "all") {
    pushPoint(totalChart, label, [w.total]);
    document.getElementById("kpi-count").textContent = w.count.toLocaleString("es-AR");
    document.getElementById("kpi-total").textContent = money.format(w.total);
    document.getElementById("kpi-avg").textContent = money.format(w.avg);
    document.getElementById("kpi-p95").textContent = money.format(w.p95);
  } else if (w.window_type === "tumbling") {
    const bucket = channelBuckets.get(label) ?? {};
    bucket[w.group_key] = w.count;
    channelBuckets.set(label, bucket);
    pushPoint(channelChart, label, CHANNELS.map((c) => bucket[c] ?? 0));
    if (channelBuckets.size > MAX_POINTS) channelBuckets.delete(channelBuckets.keys().next().value);
  } else if (w.window_type === "sliding" && w.group_key === "all") {
    pushPoint(slidingChart, label, [w.avg, w.p95]);
  }
}

function render() {
  totalChart.update("none");
  channelChart.update("none");
  slidingChart.update("none");
}

function setStatus(connected) {
  const el = document.getElementById("status");
  el.textContent = connected ? "en vivo" : "desconectado";
  el.className = `status ${connected ? "status--on" : "status--off"}`;
}

function connect(attempt = 0) {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws/metrics`);

  ws.onopen = () => { setStatus(true); attempt = 0; };
  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    if (msg.type === "snapshot") msg.data.forEach(handleWindow);
    else if (msg.type === "window") handleWindow(msg.data);
    render();
  };
  ws.onclose = () => {
    setStatus(false);
    // Reconexión con backoff exponencial, tope de 30 s
    const delay = Math.min(30000, 1000 * 2 ** attempt);
    setTimeout(() => connect(attempt + 1), delay);
  };
}

connect();
