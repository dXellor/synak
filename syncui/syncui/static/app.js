/* syncui — vanilla JS, dynamic behaviour only */

// ── State ──────────────────────────────────────────────────────────────────
// SCHEMAS and pairCounter are injected by the template as globals.
let currentMode = "friendly";

// ── Init ───────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll(".mode-btn").forEach(btn =>
    btn.addEventListener("click", () => switchMode(btn.dataset.mode))
  );
  document.getElementById("btn-apply")?.addEventListener("click", applyConfig);
  document.getElementById("btn-add-pair")?.addEventListener("click", addPair);
  document.getElementById("btn-add-peer")?.addEventListener("click", addStaticPeer);
});

// ── Mode toggle ────────────────────────────────────────────────────────────
async function switchMode(mode) {
  if (mode === currentMode) return;

  if (currentMode === "friendly") {
    try {
      const cfg = collectFriendly();
      const { toml } = await apiPost("/api/convert", { json: cfg });
      document.getElementById("raw-textarea").value = toml;
    } catch (e) {
      flash("error", `Cannot convert to TOML: ${e.message}`);
      return;
    }
  } else {
    const toml = document.getElementById("raw-textarea").value.trim();
    if (toml) {
      try {
        const { json: cfg } = await apiPost("/api/convert", { toml });
        reloadForm(cfg);
      } catch (e) {
        flash("error", `Cannot switch: ${e.message}`);
        return;
      }
    }
  }

  currentMode = mode;
  document.querySelectorAll(".mode-btn").forEach(btn =>
    btn.classList.toggle("active", btn.dataset.mode === mode)
  );
  document.getElementById("editor-friendly").style.display = mode === "friendly" ? "" : "none";
  document.getElementById("editor-raw").style.display      = mode === "raw"      ? "" : "none";
}

// Reload the form pane by fetching fresh server-rendered HTML for the config.
async function reloadForm(cfg) {
  const resp = await fetch("/fragment/form", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(cfg),
  });
  if (!resp.ok) throw new Error(await resp.text());
  document.getElementById("editor-friendly").innerHTML = await resp.text();
  pairCounter = document.querySelectorAll(".pair-card").length;
  bindDynamicPairs();
  document.getElementById("btn-add-pair")?.addEventListener("click", addPair);
  document.getElementById("btn-add-peer")?.addEventListener("click", addStaticPeer);
}

// ── Apply ──────────────────────────────────────────────────────────────────
async function applyConfig() {
  const btn = document.getElementById("btn-apply");
  btn.disabled = true;
  try {
    let body;
    if (currentMode === "raw") {
      body = { toml: document.getElementById("raw-textarea").value };
    } else {
      body = { json: collectFriendly() };
    }
    const data = await apiPost("/api/config", body);
    document.getElementById("raw-textarea").value = data.toml || "";
    // Reload form with the canonicalized config the daemon returned
    if (currentMode === "friendly") await reloadForm(data.json);
    flash("success", "Config applied.");
  } catch (e) {
    flash("error", e.message);
  } finally {
    btn.disabled = false;
  }
}

// ── Dynamic pairs ──────────────────────────────────────────────────────────
function addPair() {
  const wrap = document.getElementById("pairs-wrap");
  wrap.insertAdjacentHTML("beforeend", renderPairCardHTML(pairCounter, defaultPair()));
  bindPairCard(wrap.lastElementChild);
  pairCounter++;
}

function bindDynamicPairs() {
  document.querySelectorAll(".pair-card").forEach(bindPairCard);
}

function bindPairCard(card) {
  card.querySelector(".btn-remove-pair")?.addEventListener("click", () => card.remove());
  card.querySelectorAll('input[type="radio"][name$=".mode"]').forEach(radio => {
    radio.addEventListener("change", (e) => onModeChange(e, card));
  });
}

// Called both from Jinja-rendered cards (inline onchange) and JS-created ones.
function onModeChange(e, card) {
  const mode = e.target.value;
  const idx = card.dataset.pairIdx;
  const provSection = card.querySelector(".provider-section");
  provSection.innerHTML = renderProviderSectionHTML(idx, mode, {});
}

function renderPairCardHTML(idx, pair) {
  const modes = ["client-server", "p2p"];
  const dirs  = ["push", "pull", "bidirectional"];
  return `
<div class="section-card pair-card" data-pair-idx="${idx}">
  <div class="section-header">
    <span>Pair</span>
    <button type="button" class="btn-remove-pair">×</button>
  </div>
  <div class="section-body">
    ${fieldRowHTML("id", true, `<input type="text" name="pairs.${idx}.id" value="${esc(pair.id)}">`)}
    ${fieldRowHTML("local", true, `<input type="text" name="pairs.${idx}.local" value="${esc(pair.local)}">`)}
    ${fieldRowHTML("direction", true, radioGroupHTML(`pairs.${idx}.direction`, dirs, pair.direction))}
    ${fieldRowHTML("mode", true, `
      <div class="radio-group" onchange="onModeChange(event, this.closest('.pair-card'))">
        ${modes.map(m => `<label><input type="radio" name="pairs.${idx}.mode" value="${m}"${m === pair.mode ? " checked" : ""}> ${m}</label>`).join("")}
      </div>`)}
    ${fieldRowHTML("interval", false, `<input type="number" name="pairs.${idx}.interval" value="${pair.interval}" min="0" style="width:120px">`, "Seconds between syncs. 0 = watch-based.")}
    ${fieldRowHTML("exclude", false, arrayFieldHTML(`pairs.${idx}.exclude`, pair.exclude))}
  </div>
  <div class="provider-section">
    ${renderProviderSectionHTML(idx, pair.mode, pair.provider || {})}
  </div>
</div>`;
}

function renderProviderSectionHTML(idx, mode, provider) {
  const schema = SCHEMAS[mode];
  if (!schema) return "";
  const required = schema.required || [];
  const rows = Object.entries(schema.properties || {}).map(([key, def]) => {
    const name = `pairs.${idx}.provider.${key}`;
    const cur  = provider[key];
    let widget;
    if (def.enum) {
      widget = selectHTML(name, def.enum, cur ?? def.enum[0]);
    } else if (def.type === "boolean") {
      widget = `<label class="check-group"><input type="checkbox" name="${name}"${cur !== false ? " checked" : ""}> enabled</label>`;
    } else if (def.type === "integer" || def.type === "number") {
      widget = `<input type="number" name="${name}" value="${cur ?? 0}" min="0" style="width:120px">`;
    } else if (def.type === "array") {
      widget = arrayFieldHTML(name, cur || []);
    } else {
      widget = `<input type="text" name="${name}" value="${esc(cur ?? "")}">`;
    }
    return fieldRowHTML(key, required.includes(key), widget, def.description);
  }).join("");
  return `<div class="section-card"><div class="section-header">Provider (${mode})</div><div class="section-body">${rows}</div></div>`;
}

// ── Static peer rows ───────────────────────────────────────────────────────
function addStaticPeer() {
  const wrap = document.getElementById("static-peers-wrap");
  const idx  = wrap.querySelectorAll(".static-peer-row").length;
  wrap.insertAdjacentHTML("beforeend", `
<div class="static-peer-row array-row">
  <input type="text" name="peers.static.${idx}.id" placeholder="peer-id" style="flex:1">
  <input type="text" name="peers.static.${idx}.address" placeholder="192.168.1.42:5000" style="flex:2">
  <button type="button" class="btn-icon" onclick="this.closest('.static-peer-row').remove()">×</button>
</div>`);
}

// ── Collect form → config dict ─────────────────────────────────────────────
function collectFriendly() {
  const cfg = {
    daemon: {
      api_socket: document.querySelector('[name="daemon.api_socket"]')?.value || "",
      log_level:  val("daemon.log_level"),
    },
    pairs: [],
    peers: { discovery: val("peers.discovery") || "static", static: [] },
  };

  document.querySelectorAll(".pair-card").forEach(card => {
    const idx  = card.dataset.pairIdx;
    const p    = k => `pairs.${idx}.${k}`;
    const mode = checkedRadio(p("mode"), card) || "client-server";
    cfg.pairs.push({
      id:        val(p("id"), card),
      mode,
      local:     val(p("local"), card),
      direction: checkedRadio(p("direction"), card) || "bidirectional",
      interval:  intVal(p("interval"), card) ?? 0,
      exclude:   collectArray(p("exclude"), card),
      provider:  collectProvider(card, idx, mode),
    });
  });

  document.querySelectorAll(".static-peer-row").forEach((row, i) => {
    const id      = row.querySelector(`[name="peers.static.${i}.id"]`)?.value.trim();
    const address = row.querySelector(`[name="peers.static.${i}.address"]`)?.value.trim();
    if (id || address) cfg.peers.static.push({ id: id || "", address: address || "" });
  });

  return cfg;
}

function collectProvider(card, idx, mode) {
  const schema = SCHEMAS[mode];
  if (!schema) return {};
  const prov = {};
  Object.entries(schema.properties || {}).forEach(([key, def]) => {
    const name = `pairs.${idx}.provider.${key}`;
    if (def.type === "boolean") {
      const el = card.querySelector(`[name="${name}"]`);
      if (el) prov[key] = el.checked;
    } else if (def.type === "integer" || def.type === "number") {
      const v = intVal(name, card);
      if (v !== null) prov[key] = v;
    } else if (def.type === "array") {
      const arr = collectArray(name, card);
      if (arr.length) prov[key] = arr;
    } else {
      const v = val(name, card);
      if (v) prov[key] = v;
    }
  });
  return prov;
}

// ── HTML helpers (used only for JS-created elements) ──────────────────────
function fieldRowHTML(label, required, widgetHTML, desc) {
  return `
<div class="field-row">
  <div class="field-label${required ? " required" : ""}">${label}</div>
  <div class="field-input">${widgetHTML}${desc ? `<div class="field-desc">${desc}</div>` : ""}</div>
</div>`;
}

function radioGroupHTML(name, options, selected) {
  return `<div class="radio-group">` +
    options.map(o => `<label><input type="radio" name="${name}" value="${o}"${o === selected ? " checked" : ""}> ${o}</label>`).join("") +
    `</div>`;
}

function selectHTML(name, options, selected) {
  return `<select name="${name}">` +
    options.map(o => `<option value="${o}"${o === selected ? " selected" : ""}>${o}</option>`).join("") +
    `</select>`;
}

function arrayFieldHTML(name, values) {
  const rows = values.map(v =>
    `<div class="array-row"><input type="text" name="${name}[]" value="${esc(v)}"><button type="button" class="btn-icon" onclick="this.closest('.array-row').remove()">×</button></div>`
  ).join("");
  return `<div class="array-field">${rows}<button type="button" class="btn-add-row" onclick="addArrayRow(this,'${name}')">+ Add</button></div>`;
}

function addArrayRow(btn, name) {
  btn.insertAdjacentHTML("beforebegin",
    `<div class="array-row"><input type="text" name="${name}[]" value=""><button type="button" class="btn-icon" onclick="this.closest('.array-row').remove()">×</button></div>`
  );
}

function defaultPair() {
  return { id: "", mode: "client-server", local: "", direction: "bidirectional", interval: 0, exclude: [], provider: {} };
}

function esc(s) {
  return String(s ?? "").replace(/&/g,"&amp;").replace(/"/g,"&quot;").replace(/</g,"&lt;");
}

// ── Form value helpers ─────────────────────────────────────────────────────
function val(name, root = document)         { return root.querySelector(`[name="${name}"]`)?.value.trim() ?? ""; }
function checkedRadio(name, root = document){ return root.querySelector(`[name="${name}"]:checked`)?.value ?? null; }
function intVal(name, root = document)      { const v = root.querySelector(`[name="${name}"]`)?.value; return (v === "" || v == null) ? null : (parseInt(v,10) || null); }
function collectArray(name, root = document){ return Array.from(root.querySelectorAll(`[name="${name}[]"]`)).map(e => e.value.trim()).filter(Boolean); }

// ── API helpers ────────────────────────────────────────────────────────────
async function apiPost(path, body) {
  const resp = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await resp.json();
  if (!resp.ok) {
    const err = new Error(data.error || `HTTP ${resp.status}`);
    err.daemonDown = !!data.daemon_down;
    throw err;
  }
  return data;
}

// ── Flash ──────────────────────────────────────────────────────────────────
function flash(type, message) {
  const el = document.getElementById("flash");
  el.className = `flash flash-${type}`;
  el.textContent = message;
  el.style.display = "";
  clearTimeout(el._timer);
  if (type === "success") el._timer = setTimeout(() => { el.style.display = "none"; }, 4000);
}
