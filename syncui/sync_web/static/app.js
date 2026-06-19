/* sync-web — vanilla JS, no framework */

// ── State ──────────────────────────────────────────────────────────────────
let currentMode = "raw";       // "raw" | "friendly"
let currentConfig = null;      // last successfully loaded config dict
let schemas = {};              // provider schemas from /api/schemas

// ── Init ───────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  setupModeToggle();
  setupApplyButton();
  loadSchemas().then(loadConfig);
});

// ── Mode toggle ────────────────────────────────────────────────────────────
function setupModeToggle() {
  document.querySelectorAll(".mode-btn").forEach(btn => {
    btn.addEventListener("click", () => switchMode(btn.dataset.mode));
  });
}

function switchMode(mode) {
  if (mode === currentMode) return;

  // If leaving raw mode, try to parse the textarea so edits carry over.
  if (currentMode === "raw" && mode === "friendly") {
    const toml = document.getElementById("raw-textarea").value;
    if (toml.trim()) {
      applyTomlToState(toml).catch(() => {
        // Parse failed — stay in raw and show error (applyTomlToState already flashed it)
        return;
      }).then(ok => {
        if (ok !== false) activateMode(mode);
      });
      return;
    }
  }

  activateMode(mode);
}

function activateMode(mode) {
  currentMode = mode;
  document.querySelectorAll(".mode-btn").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.mode === mode);
  });
  document.getElementById("editor-raw").style.display = mode === "raw" ? "" : "none";
  document.getElementById("editor-friendly").style.display = mode === "friendly" ? "" : "none";

  if (mode === "friendly" && currentConfig) renderFriendly(currentConfig);
  if (mode === "raw" && currentConfig) {
    // Re-sync textarea from current in-memory config (might have been updated via friendly)
  }
}

// ── Load ───────────────────────────────────────────────────────────────────
async function loadSchemas() {
  try {
    schemas = await apiGet("/api/schemas");
  } catch (_) {
    schemas = {};
  }
}

async function loadConfig() {
  let data;
  try {
    data = await apiGet("/api/config");
  } catch (err) {
    if (err.daemonDown) {
      showDaemonDown(err.message);
    } else {
      flash("error", `Failed to load config: ${err.message}`);
    }
    return;
  }

  showDaemonUp();
  currentConfig = data.json;
  document.getElementById("raw-textarea").value = data.toml;
  if (currentMode === "friendly") renderFriendly(currentConfig);
}

// ── Apply ──────────────────────────────────────────────────────────────────
function setupApplyButton() {
  document.getElementById("btn-apply").addEventListener("click", applyConfig);
}

async function applyConfig() {
  const btn = document.getElementById("btn-apply");
  btn.disabled = true;

  let body;
  if (currentMode === "raw") {
    body = { toml: document.getElementById("raw-textarea").value };
  } else {
    body = { json: collectFriendly() };
  }

  try {
    const data = await apiPost("/api/config", body);
    currentConfig = data.json;
    document.getElementById("raw-textarea").value = data.toml;
    if (currentMode === "friendly") renderFriendly(currentConfig);
    flash("success", "Config applied.");
  } catch (err) {
    flash("error", err.message);
  } finally {
    btn.disabled = false;
  }
}

async function applyTomlToState(toml) {
  try {
    const data = await apiPost("/api/config", { toml });
    currentConfig = data.json;
    document.getElementById("raw-textarea").value = data.toml;
    return true;
  } catch (err) {
    flash("error", err.message);
    return false;
  }
}

// ── Friendly form — render ─────────────────────────────────────────────────
function renderFriendly(cfg) {
  const root = document.getElementById("friendly-form");
  root.innerHTML = "";

  // Daemon section
  root.appendChild(renderDaemonSection(cfg.daemon || {}));

  // Pairs section
  const pairsWrap = document.createElement("div");
  pairsWrap.id = "pairs-wrap";
  (cfg.pairs || []).forEach((pair, i) => pairsWrap.appendChild(renderPairCard(pair, i)));
  root.appendChild(pairsWrap);

  const addPairBtn = document.createElement("button");
  addPairBtn.className = "btn-add-pair";
  addPairBtn.textContent = "+ Add pair";
  addPairBtn.addEventListener("click", () => {
    const wrap = document.getElementById("pairs-wrap");
    const idx = wrap.children.length;
    wrap.appendChild(renderPairCard(defaultPair(), idx));
  });
  root.appendChild(addPairBtn);

  // Peers section
  root.appendChild(renderPeersSection(cfg.peers || {}));
}

function renderDaemonSection(daemon) {
  const card = sectionCard("Daemon");

  // api_socket — read-only
  card.appendChild(fieldRow("api_socket", false,
    readonlyInput(daemon.api_socket || ""),
    "Cannot change at runtime."
  ));

  // log_level — select
  const levels = ["debug", "info", "warning", "error", "critical"];
  card.appendChild(fieldRow("log_level", false,
    selectField("daemon.log_level", levels, daemon.log_level || "info")
  ));

  return card;
}

function renderPairCard(pair, idx) {
  const card = document.createElement("div");
  card.className = "section-card pair-card";
  card.dataset.pairIdx = idx;

  const header = document.createElement("div");
  header.className = "section-header";
  header.innerHTML = `<span>Pair</span>`;
  const removeBtn = document.createElement("button");
  removeBtn.className = "btn-remove-pair";
  removeBtn.title = "Remove pair";
  removeBtn.textContent = "×";
  removeBtn.addEventListener("click", () => card.remove());
  header.appendChild(removeBtn);
  card.appendChild(header);

  const body = document.createElement("div");
  body.className = "section-body";

  const p = (k) => `pairs.${idx}.${k}`;

  body.appendChild(fieldRow("id", true, textInput(p("id"), pair.id || "")));
  body.appendChild(fieldRow("local", true, textInput(p("local"), pair.local || "")));
  body.appendChild(fieldRow("direction", true,
    radioGroup(p("direction"), ["push", "pull", "bidirectional"], pair.direction || "bidirectional")
  ));
  body.appendChild(fieldRow("mode", true,
    radioGroup(p("mode"), ["client-server", "p2p"], pair.mode || "client-server",
      () => refreshProviderSection(card, pair.mode || "client-server", pair.provider || {})
    )
  ));
  body.appendChild(fieldRow("interval", false,
    numberInput(p("interval"), pair.interval ?? 0, 0),
    "Seconds between syncs. 0 = watch-based."
  ));
  body.appendChild(fieldRow("exclude", false,
    arrayField(p("exclude"), pair.exclude || []),
    "fnmatch patterns to skip."
  ));

  card.appendChild(body);

  // Provider sub-section (appended to card, after body)
  const provSection = document.createElement("div");
  provSection.className = "provider-section";
  card.appendChild(provSection);
  refreshProviderSection(card, pair.mode || "client-server", pair.provider || {});

  // Re-render provider when mode changes
  card.querySelector(`[name="${p("mode")}"]`)?.closest(".radio-group")
    ?.addEventListener("change", (e) => {
      refreshProviderSection(card, e.target.value, {});
    });

  return card;
}

function refreshProviderSection(card, mode, providerCfg) {
  const provSection = card.querySelector(".provider-section");
  provSection.innerHTML = "";

  const schema = schemas[mode];
  if (!schema) return;

  const idx = card.dataset.pairIdx;
  const innerCard = sectionCard(`Provider (${mode})`);
  const body = innerCard.querySelector(".section-body");

  Object.entries(schema.properties || {}).forEach(([key, def]) => {
    const required = (schema.required || []).includes(key);
    const name = `pairs.${idx}.provider.${key}`;
    const val = providerCfg[key];
    let widget;

    if (def.enum) {
      widget = selectField(name, def.enum, val ?? def.enum[0]);
    } else if (def.type === "boolean") {
      widget = checkboxField(name, val ?? true);
    } else if (def.type === "integer" || def.type === "number") {
      widget = numberInput(name, val ?? 0, def.type === "integer" ? 0 : undefined);
    } else if (def.type === "array") {
      widget = arrayField(name, val || []);
    } else {
      widget = textInput(name, val ?? "");
    }

    body.appendChild(fieldRow(key, required, widget, def.description));
  });

  provSection.appendChild(innerCard);
}

function renderPeersSection(peers) {
  const card = sectionCard("Peers");

  card.appendChild(fieldRow("discovery", false,
    selectField("peers.discovery", ["static"], peers.discovery || "static")
  ));

  const staticList = peers.static || [];
  const staticWrap = document.createElement("div");
  staticWrap.className = "section-body";
  staticWrap.style.borderTop = "1px solid var(--border)";
  staticWrap.id = "peers-static-wrap";

  const staticHeader = document.createElement("div");
  staticHeader.className = "section-header";
  staticHeader.textContent = "Static peers";
  card.querySelector(".section-card") || card;

  // Append static header + rows directly inside the card's body
  const cardBody = card.querySelector(".section-body");
  const staticLabel = document.createElement("div");
  staticLabel.className = "field-label";
  staticLabel.style.gridColumn = "1 / -1";
  staticLabel.style.fontWeight = "600";
  staticLabel.style.paddingTop = "4px";
  staticLabel.textContent = "static peers";
  cardBody.appendChild(staticLabel);

  staticList.forEach((peer, i) => cardBody.appendChild(renderStaticPeer(peer, i)));

  const addBtn = document.createElement("button");
  addBtn.className = "btn-add-row";
  addBtn.textContent = "+ Add static peer";
  addBtn.style.marginTop = "4px";
  addBtn.addEventListener("click", () => {
    const count = cardBody.querySelectorAll(".static-peer-row").length;
    addBtn.before(renderStaticPeer({}, count));
  });
  cardBody.appendChild(addBtn);

  return card;
}

function renderStaticPeer(peer, idx) {
  const row = document.createElement("div");
  row.className = "static-peer-row array-row";
  row.style.gridColumn = "1 / -1";

  const idInput = document.createElement("input");
  idInput.type = "text";
  idInput.name = `peers.static.${idx}.id`;
  idInput.value = peer.id || "";
  idInput.placeholder = "peer-id";
  idInput.style.flex = "1";

  const addrInput = document.createElement("input");
  addrInput.type = "text";
  addrInput.name = `peers.static.${idx}.address`;
  addrInput.value = peer.address || "";
  addrInput.placeholder = "192.168.1.42:5000";
  addrInput.style.flex = "2";

  const removeBtn = document.createElement("button");
  removeBtn.className = "btn-icon";
  removeBtn.title = "Remove";
  removeBtn.textContent = "×";
  removeBtn.addEventListener("click", () => row.remove());

  row.appendChild(idInput);
  row.appendChild(addrInput);
  row.appendChild(removeBtn);
  return row;
}

function defaultPair() {
  return { id: "", mode: "client-server", local: "", direction: "bidirectional", interval: 0, exclude: [], provider: {} };
}

// ── Friendly form — collect ────────────────────────────────────────────────
function collectFriendly() {
  const cfg = { daemon: {}, pairs: [], peers: { discovery: "static", static: [] } };

  // Daemon
  cfg.daemon.api_socket = currentConfig?.daemon?.api_socket || "";
  cfg.daemon.log_level = val("daemon.log_level");

  // Pairs
  document.querySelectorAll(".pair-card").forEach((card, idx) => {
    const p = (k) => `pairs.${idx}.${k}`;
    const mode = checkedRadio(p("mode")) || "client-server";
    const pair = {
      id: val(p("id")),
      mode,
      local: val(p("local")),
      direction: checkedRadio(p("direction")) || "bidirectional",
      interval: intVal(p("interval")),
      exclude: collectArray(p("exclude")),
      provider: collectProvider(card, idx, mode),
    };
    cfg.pairs.push(pair);
  });

  // Peers
  cfg.peers.discovery = val("peers.discovery") || "static";
  cfg.peers.static = [];
  document.querySelectorAll(".static-peer-row").forEach((row, i) => {
    const id = row.querySelector(`[name="peers.static.${i}.id"]`)?.value.trim();
    const address = row.querySelector(`[name="peers.static.${i}.address"]`)?.value.trim();
    if (id || address) cfg.peers.static.push({ id: id || "", address: address || "" });
  });

  return cfg;
}

function collectProvider(card, idx, mode) {
  const schema = schemas[mode];
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
      prov[key] = collectArray(name, card);
    } else {
      const v = val(name, card);
      if (v !== "") prov[key] = v;
    }
  });
  return prov;
}

// ── Widget helpers ─────────────────────────────────────────────────────────
function sectionCard(title) {
  const card = document.createElement("div");
  card.className = "section-card";
  const header = document.createElement("div");
  header.className = "section-header";
  header.textContent = title;
  card.appendChild(header);
  const body = document.createElement("div");
  body.className = "section-body";
  card.appendChild(body);
  return card;
}

function fieldRow(label, required, widget, desc) {
  const row = document.createElement("div");
  row.className = "field-row";

  const lbl = document.createElement("div");
  lbl.className = "field-label" + (required ? " required" : "");
  lbl.textContent = label;
  row.appendChild(lbl);

  const wrap = document.createElement("div");
  wrap.className = "field-input";
  wrap.appendChild(widget);
  if (desc) {
    const d = document.createElement("div");
    d.className = "field-desc";
    d.textContent = desc;
    wrap.appendChild(d);
  }
  row.appendChild(wrap);
  return row;
}

function textInput(name, value) {
  const el = document.createElement("input");
  el.type = "text";
  el.name = name;
  el.value = value;
  return el;
}

function readonlyInput(value) {
  const el = textInput("", value);
  el.readOnly = true;
  return el;
}

function numberInput(name, value, min) {
  const el = document.createElement("input");
  el.type = "number";
  el.name = name;
  el.value = value ?? "";
  if (min !== undefined) el.min = min;
  el.style.width = "120px";
  return el;
}

function selectField(name, options, selected) {
  const el = document.createElement("select");
  el.name = name;
  options.forEach(opt => {
    const o = document.createElement("option");
    o.value = opt;
    o.textContent = opt;
    if (opt === selected) o.selected = true;
    el.appendChild(o);
  });
  return el;
}

function radioGroup(name, options, selected, onChange) {
  const wrap = document.createElement("div");
  wrap.className = "radio-group";
  options.forEach(opt => {
    const lbl = document.createElement("label");
    const input = document.createElement("input");
    input.type = "radio";
    input.name = name;
    input.value = opt;
    if (opt === selected) input.checked = true;
    if (onChange) input.addEventListener("change", () => onChange(opt));
    lbl.appendChild(input);
    lbl.appendChild(document.createTextNode(opt));
    wrap.appendChild(lbl);
  });
  return wrap;
}

function checkboxField(name, checked) {
  const lbl = document.createElement("label");
  lbl.className = "check-group";
  const el = document.createElement("input");
  el.type = "checkbox";
  el.name = name;
  el.checked = !!checked;
  lbl.appendChild(el);
  lbl.appendChild(document.createTextNode("enabled"));
  return lbl;
}

function arrayField(name, values) {
  const wrap = document.createElement("div");
  wrap.className = "array-field";
  wrap.dataset.arrayName = name;

  values.forEach(v => wrap.appendChild(arrayRow(name, v)));

  const addBtn = document.createElement("button");
  addBtn.className = "btn-add-row";
  addBtn.textContent = "+ Add";
  addBtn.addEventListener("click", () => addBtn.before(arrayRow(name, "")));
  wrap.appendChild(addBtn);
  return wrap;
}

function arrayRow(name, value) {
  const row = document.createElement("div");
  row.className = "array-row";
  const input = document.createElement("input");
  input.type = "text";
  input.name = name + "[]";
  input.value = value;
  const btn = document.createElement("button");
  btn.className = "btn-icon";
  btn.textContent = "×";
  btn.title = "Remove";
  btn.addEventListener("click", () => row.remove());
  row.appendChild(input);
  row.appendChild(btn);
  return row;
}

// ── Form value helpers ─────────────────────────────────────────────────────
function val(name, root = document) {
  return root.querySelector(`[name="${name}"]`)?.value.trim() ?? "";
}

function checkedRadio(name, root = document) {
  return root.querySelector(`[name="${name}"]:checked`)?.value ?? null;
}

function intVal(name, root = document) {
  const v = root.querySelector(`[name="${name}"]`)?.value;
  if (v === "" || v === undefined || v === null) return null;
  const n = parseInt(v, 10);
  return isNaN(n) ? null : n;
}

function collectArray(name, root = document) {
  return Array.from(root.querySelectorAll(`[name="${name}[]"]`))
    .map(el => el.value.trim())
    .filter(Boolean);
}

// ── API helpers ────────────────────────────────────────────────────────────
async function apiGet(path) {
  const resp = await fetch(path);
  const data = await resp.json();
  if (!resp.ok) {
    const err = new Error(data.error || `HTTP ${resp.status}`);
    err.daemonDown = !!data.daemon_down;
    throw err;
  }
  return data;
}

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

// ── Status / banner helpers ────────────────────────────────────────────────
function showDaemonUp() {
  const badge = document.getElementById("daemon-badge");
  badge.className = "badge badge-running";
  badge.textContent = "running";
  document.getElementById("daemon-banner").style.display = "none";
}

function showDaemonDown(message) {
  const badge = document.getElementById("daemon-badge");
  badge.className = "badge badge-down";
  badge.textContent = "down";
  const banner = document.getElementById("daemon-banner");
  banner.textContent = message;
  banner.style.display = "";
}

function flash(type, message) {
  const el = document.getElementById("flash");
  el.className = `flash flash-${type}`;
  el.textContent = message;
  el.style.display = "";
  clearTimeout(el._timer);
  if (type === "success") {
    el._timer = setTimeout(() => { el.style.display = "none"; }, 4000);
  }
}
