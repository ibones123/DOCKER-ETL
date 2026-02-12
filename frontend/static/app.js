const API_BASE = "/api";

const statusEl = document.getElementById("status");
const tbody = document.getElementById("tbody");

const limitEl = document.getElementById("limit");
const offsetEl = document.getElementById("offset");
const searchEl = document.getElementById("search");

const kpiTotal = document.getElementById("kpiTotal");
const kpiShown = document.getElementById("kpiShown");
const kpiLast = document.getElementById("kpiLast");

const healthDot = document.getElementById("healthDot");
const healthText = document.getElementById("healthText");

function setStatus(msg) { statusEl.textContent = msg; }

function parseMaybeJson(text) {
  try { return JSON.parse(text); } catch (e) { return null; }
}

async function fetchJson(url, options = {}) {
  const res = await fetch(url, options);
  const text = await res.text();
  const data = parseMaybeJson(text);
  if (!res.ok) {
    const msg = (data && (data.detail || data.message)) || text.slice(0, 120) || `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return data ?? {};
}

function renderRows(items, query) {
  tbody.innerHTML = "";
  const q = (query || "").trim().toLowerCase();

  const filtered = !q ? items : items.filter(it => {
    const s = `${it.cca2||""} ${it.name||""} ${it.region||""} ${it.capital||""}`.toLowerCase();
    return s.includes(q);
  });

  for (const it of filtered) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${it.cca2 ?? ""}</td>
      <td>${it.name ?? ""}</td>
      <td>${it.region ?? ""}</td>
      <td>${it.population ?? ""}</td>
      <td>${it.capital ?? ""}</td>
      <td class="mono">${it.loaded_at ?? ""}</td>
    `;
    tbody.appendChild(tr);
  }

  kpiShown.textContent = String(filtered.length);
  const last = filtered.find(x => x.loaded_at)?.loaded_at || (items.find(x => x.loaded_at)?.loaded_at) || "—";
  kpiLast.textContent = last;
}

async function checkHealth() {
  try {
    await fetchJson(`${API_BASE}/health`);
    healthDot.className = "dot ok";
    healthText.textContent = "API: OK";
  } catch (e) {
    healthDot.className = "dot err";
    healthText.textContent = "API: KO";
  }
}

async function loadData() {
  setStatus("Chargement des données...");
  const limit = Number(limitEl.value || 50);
  const offset = Number(offsetEl.value || 0);

  try {
    const data = await fetchJson(`${API_BASE}/data?limit=${limit}&offset=${offset}`);
    const items = data.items || [];
    kpiTotal.textContent = String(data.total ?? "—");
    renderRows(items, searchEl.value);
    setStatus("OK");
  } catch (e) {
    setStatus(`Erreur: ${e.message}`);
  }
}

async function runEtl() {
  const btn = document.getElementById("btnRun");
  btn.disabled = true;

  setStatus("ETL en cours...");
  try {
    const data = await fetchJson(`${API_BASE}/etl/run`, { method: "POST" });
    setStatus(`ETL OK — rows_processed=${data.rows_processed}, total_db=${data.total_rows_in_db}`);
    await loadData();
  } catch (e) {
    setStatus(`Erreur ETL: ${e.message}`);
  } finally {
    btn.disabled = false;
  }
}

document.getElementById("btnRun").addEventListener("click", runEtl);
document.getElementById("btnRefresh").addEventListener("click", loadData);
searchEl.addEventListener("input", () => loadData());

checkHealth();
loadData();
