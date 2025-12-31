// public/script.js (FULL REWRITE v3)
// Changes in this rewrite:
// ✅ Stars display: ALWAYS shows 5 ✪ icons when starred, plus master-star number (if any)
// ✅ Removed Live Signature (Perfect Match) support entirely
// ✅ Added Pet Item (optional) local autocomplete using PET_ITEM_LIST
// ✅ Keeps PARTIAL tier tag as data-tier="PARTIAL" for purple styling
// ✅ Autocomplete dedupe + abort handling kept
// ✅ WI toggle only shows for Wither Blades


function $(id) { return document.getElementById(id); }


/* =========================
   Constants
========================= */
const PET_ITEM_LIST = [
  "All Skills Exp Boost",
  "All Skills Exp Super-Boost",
  "Antique Remedies",
  "Bejeweled Collar",
  "Big Teeth",
  "Bigger Teeth",
  "Bingo Booster",
  "Brown Bandana",
  "Bubblegum",
  "Burnt Texts",
  "Combat Exp Boost",
  "Cretan Urn",
  "Crochet Tiger Plushie",
  "Dead Cat Food",
  "Dwarf Turtle Shelmet",
  "Edible Seaweed",
  "Eerie Toy",
  "Eerie Treat",
  "Exp Share",
  "Exp Share Core",
  "Fake Neuroscience Degree",
  "Farming Exp Boost",
  "Fishing Exp Boost",
  "Flying Pig",
  "Foraging Exp Boost",
  "Four-Eyed Fish",
  "Frog Treat",
  "Gold Claws",
  "Grandma's Knitting Needle",
  "Green Bandana",
  "Guardian Lucky Claw",
  "Hardened Scales",
  "Hephaestus Plushie",
  "Hephaestus Relic",
  "Hephaestus Remedies",
  "Hephaestus Shelmet",
  "Hephaestus Souvenir",
  "Hephaestus Urn",
  "Iron Claws",
  "Jerry 3D Glasses",
  "Lucky Clover",
  "Mining Exp Boost",
  "Minos Relic",
  "Party Hat",
  "Quick Claw",
  "Radioactive Vial",
  "Reaper Gem",
  "Reinforced Scales",
  "Saddle",
  "Serrated Claws",
  "Sharpened Claws",
  "Simple Carrot Candy",
  "Spooky Cupcake",
  "Textbook",
  "Tier Boost",
  "Tier Boost Core",
  "Titanium Minecart",
  "Vampire Fang",
  "Washed-up Souvenir",
  "Yellow Bandana",
];


/* =========================
   Utils
========================= */
function parseCoins(input) {
  if (!input) return NaN;
  let s = String(input).trim().toLowerCase().replace(/[, ]+/g, "");
  const m = s.match(/^(-?\d+(\.\d+)?)([kmbt])?$/i);
  if (!m) return NaN;
  let value = parseFloat(m[1]);
  const suffix = (m[3] || "").toLowerCase();
  const multipliers = { k: 1e3, m: 1e6, b: 1e9, t: 1e12 };
  if (suffix && multipliers[suffix]) value *= multipliers[suffix];
  return Math.round(value);
}


function formatCoins(n) {
  if (!isFinite(n)) return "—";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(n) + " coins";
}


function formatShort(n) {
  const x = Number(n);
  if (!isFinite(x)) return "—";
  const abs = Math.abs(x);
  if (abs >= 1e12) return (x / 1e12).toFixed(2).replace(/\.00$/, "") + "t";
  if (abs >= 1e9)  return (x / 1e9).toFixed(2).replace(/\.00$/, "") + "b";
  if (abs >= 1e6)  return (x / 1e6).toFixed(2).replace(/\.00$/, "") + "m";
  if (abs >= 1e3)  return (x / 1e3).toFixed(2).replace(/\.00$/, "") + "k";
  return String(Math.round(x));
}


function escapeHtml(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}


function normalizeTextForDedupe(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, "")
    .replace(/\s+/g, " ")
    .trim();
}


function toKeyFromLabel(label) {
  // used for local lists; server expects snake-ish keys in some places
  return String(label || "").trim().toLowerCase().replace(/\s+/g, "_");
}


function prettyFromKey(k) {
  const x = String(k || "").trim();
  if (!x || x === "none" || x === "null") return "None";
  return x
    .replace(/_/g, " ")
    .split(" ")
    .filter(Boolean)
    .map((w) => (w ? w[0].toUpperCase() + w.slice(1).toLowerCase() : w))
    .join(" ");
}


/* =========================
   Wither Impact visibility
========================= */
function isWitherBladeKey(itemKeyOrLabel) {
  const k = String(itemKeyOrLabel || "").trim().toLowerCase();
  return k === "hyperion" || k === "scylla" || k === "valkyrie" || k === "astraea";
}


function updateWIVisibility() {
  const itemEl = $("advItem");
  const wiEl = $("advWI");
  if (!itemEl || !wiEl) return;


  const row = wiEl.closest(".toggle-row") || wiEl.closest(".field") || wiEl.parentElement;
  const key = (itemEl.dataset.key || itemEl.value || "").trim();
  const show = isWitherBladeKey(key);


  if (row) row.style.display = show ? "" : "none";
  else wiEl.style.display = show ? "" : "none";


  if (!show) wiEl.checked = false;
}


/* =========================
   Stars rendering
   - ALWAYS show 5 icons when starred
   - show master-star number if >0
========================= */
function clampInt(n, lo, hi) {
  const x = Math.trunc(Number(n) || 0);
  if (!Number.isFinite(x)) return lo;
  return Math.max(lo, Math.min(hi, x));
}


function renderStarsHtml(dungeonStars, masterStars) {
  const ds = clampInt(dungeonStars, 0, 5);
  const ms = clampInt(masterStars, 0, 5);
  if (ds <= 0 && ms <= 0) return "";

  // Requested display: item name + 5 ✪ + master digit glyph (➊➋➌➍➎)
  const icons = ds > 0 ? "✪".repeat(ds) : "";
  const dingbat = ["", "➊", "➋", "➌", "➍", "➎"][ms] || "";
  return `<span class="sb-stars">${escapeHtml(icons)}</span>${dingbat ? ` <span class="mstar-glyph">${escapeHtml(dingbat)}</span>` : ""}`;
}


/* =========================
   Enchant rendering
========================= */
function normalizeTier(t) {
  const u = String(t || "").toUpperCase().trim();
  if (["AAA", "AA", "A", "B", "BB", "PARTIAL"].includes(u)) return u;
  return "MISC-A";
}


function tierLabel(tier) {
  const t = normalizeTier(tier);
  return t === "PARTIAL" ? "PARTIAL" : t;
}


function parseEnchantAny(raw) {
  if (raw == null) return { tier: "MISC-A", label: "—" };
  if (typeof raw === "object") {
    const tier = normalizeTier(raw.tier);
    const label = String(raw.label ?? "").trim();
    return { tier, label: label || "—" };
  }
  const s = String(raw).trim();
  if (!s) return { tier: "MISC-A", label: "—" };
  return { tier: "MISC-A", label: s };
}


function enchantTagHtml(tier) {
  const t = normalizeTier(tier);
  return `<span class="ench-tag" data-tier="${escapeHtml(t)}">${escapeHtml(tierLabel(t))}</span>`;
}


function enchantLineHtml(raw) {
  const { tier, label } = parseEnchantAny(raw);
  return `
    <div class="ench-line">
      ${enchantTagHtml(tier)}
      <span class="ench-text">• ${escapeHtml(label)}</span>
    </div>
  `.trim();
}


function enchantInlineHtml(raw) {
  const { tier, label } = parseEnchantAny(raw);
  return `${enchantTagHtml(tier)} <span class="ench-text">${escapeHtml(label)}</span>`;
}


/* =========================
   Clipboard UUID
========================= */
async function copyTextToClipboard(text) {
  const t = String(text || "");
  if (!t) return false;


  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(t);
      return true;
    }
  } catch {}


  try {
    const ta = document.createElement("textarea");
    ta.value = t;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.top = "-9999px";
    ta.style.left = "-9999px";
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return !!ok;
  } catch {
    return false;
  }
}


function uuidButtonHtml(uuid) {
  const u = String(uuid || "").trim();
  if (!u) return "";
  return `<button type="button" class="uuid-copy-btn" data-uuid="${escapeHtml(u)}" title="Copy UUID">UUID</button>`;
}


function setUuidBtnState(btn, label, ok) {
  btn.textContent = label;
  btn.classList.toggle("uuid-ok", !!ok);
  btn.classList.toggle("uuid-bad", !ok);
  setTimeout(() => {
    btn.textContent = "UUID";
    btn.classList.remove("uuid-ok", "uuid-bad");
  }, 1100);
}


/* =========================
   Tabs / Views
========================= */
function setView(view) {
  const basics = $("view-basics");
  const advanced = $("view-advanced");
  const activeLabel = $("activeViewLabel");
  if (!basics || !advanced) return;


  const isBasics = view === "basics";
  basics.classList.toggle("active", isBasics);
  advanced.classList.toggle("active", !isBasics);


  document.querySelectorAll(".browse-tabs .tab").forEach((t) => {
    const on = t.dataset.view === view;
    t.classList.toggle("active", on);
    t.setAttribute("aria-selected", on ? "true" : "false");
  });


  if (activeLabel) activeLabel.textContent = isBasics ? "Basics" : "Advanced";
}


/* =========================
   Basics calculators
========================= */
function calculateTaxAndProfit(sellPrice) {
  let taxRate = 0.01;
  if (sellPrice < 10_000_000) taxRate = 0.01;
  else if (sellPrice < 100_000_000) taxRate = 0.02;
  else taxRate = 0.025;


  const auctionTax = Math.round(sellPrice * taxRate);
  const afterTax = sellPrice - auctionTax;
  const collectionFee = Math.round(afterTax * 0.01);
  const finalProfit = afterTax - collectionFee;
  return { taxRate, auctionTax, afterTax, collectionFee, finalProfit };
}


function calculateProfit(purchasePrice, sellPrice) {
  const { finalProfit } = calculateTaxAndProfit(sellPrice);
  return finalProfit - purchasePrice;
}


function calculateBreakEven(purchasePrice) {
  let low = purchasePrice;
  let high = purchasePrice * 2;
  let mid = purchasePrice;


  for (let i = 0; i < 80; i++) {
    mid = Math.floor((low + high) / 2);
    const profit = calculateProfit(purchasePrice, mid);
    if (Math.abs(profit) <= 1) break;
    if (profit < 0) low = mid + 1;
    else high = mid - 1;
  }
  return mid;
}


function runCalculator() {
  const sellEl = $("sellPrice");
  const outTax = $("taxResult");
  const outProfit = $("profitResult");
  if (!sellEl || !outTax || !outProfit) return;


  const coinAmount = parseCoins(sellEl.value);
  if (!isFinite(coinAmount) || coinAmount <= 0) {
    outTax.innerText = "Enter a valid amount (e.g. 1m, 1.5m, 1200000).";
    outProfit.innerText = "";
    return;
  }
  const { taxRate, auctionTax, finalProfit } = calculateTaxAndProfit(coinAmount);
  outTax.innerText = `Auction Tax (${(taxRate * 100).toFixed(2)}%): ${formatCoins(auctionTax)}`;
  outProfit.innerText = `Take-home (after 1% collection fee): ${formatCoins(finalProfit)}`;
}


function runLowballCalculator() {
  const purchaseEl = $("purchasePrice");
  const sellEl = $("sellPriceLow");
  const output = $("lowballProfitResult");
  const breakEvenOutput = $("breakEvenResult");
  if (!purchaseEl || !sellEl || !output || !breakEvenOutput) return;


  const purchasePrice = parseCoins(purchaseEl.value);
  const sellPrice = parseCoins(sellEl.value);


  if (!isFinite(purchasePrice) || purchasePrice <= 0) {
    output.innerText = "Enter a valid purchase price.";
    breakEvenOutput.innerText = "—";
    return;
  }
  if (!isFinite(sellPrice) || sellPrice <= 0) {
    output.innerText = "Enter a valid sell price.";
    breakEvenOutput.innerText = "—";
    return;
  }


  const totalProfit = calculateProfit(purchasePrice, sellPrice);
  const breakEvenPrice = calculateBreakEven(purchasePrice);


  output.innerText = `Final Profit: ${formatCoins(totalProfit)}`;
  breakEvenOutput.innerText = `${formatCoins(breakEvenPrice)}`;
}


/* =========================
   Stars slider
========================= */
function setupStars10Slider() {
  const s = $("advStars10");
  const v = $("advStars10Value");
  const clear = $("advStars10Clear");
  if (!s || !v || !clear) return;


  const sync = () => (v.textContent = String(s.value));
  sync();
  s.addEventListener("input", sync);
  clear.addEventListener("click", () => { s.value = "0"; sync(); });
}


/* =========================
   Server-backed autocomplete
========================= */
function setupAutocomplete({ inputId, boxId, endpoint, limit = 30, onPick }) {
  const input = $(inputId);
  const box = $(boxId);
  if (!input || !box) return;


  let timer = null;
  let controller = null;
  let reqSeq = 0;


  const toLabel = (x) => (typeof x === "string" ? x : (x?.label || x?.key || ""));
  const toKey = (x) => (typeof x === "string" ? x : (x?.key || x?.label || ""));


  function hide() { box.style.display = "none"; box.innerHTML = ""; }


  function render(items) {
    box.innerHTML = "";
    if (!Array.isArray(items) || items.length === 0) return hide();


    const seen = new Set();
    const cleaned = [];


    for (const it of items) {
      const label = String(toLabel(it) || "").trim();
      const key = String(toKey(it) || "").trim();
      if (!label && !key) continue;


      const dedupeKey = key ? key.toLowerCase() : normalizeTextForDedupe(label);
      if (!dedupeKey) continue;


      if (seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);


      cleaned.push(it);
      if (cleaned.length >= limit) break;
    }


    if (!cleaned.length) return hide();


    box.style.display = "block";
    for (const it of cleaned) {
      const div = document.createElement("div");
      div.className = "item";
      div.dataset.label = toLabel(it);
      div.dataset.key = toKey(it);
      div.textContent = toLabel(it);
      box.appendChild(div);
    }
  }


  input.addEventListener("keydown", () => { input.dataset.key = ""; });


  input.addEventListener("input", () => {
    clearTimeout(timer);
    const q = input.value.trim();
    if (!q) return hide();


    timer = setTimeout(async () => {
      if (controller) controller.abort();
      controller = new AbortController();
      const mySeq = ++reqSeq;


      try {
        const res = await fetch(`${endpoint}?q=${encodeURIComponent(q)}&limit=${limit}`, {
          signal: controller.signal,
          cache: "no-store",
        });
        const data = await res.json().catch(() => ({}));
        if (mySeq !== reqSeq) return;
        render(data.items || []);
      } catch {
        if (mySeq === reqSeq) hide();
      }
    }, 90);
  });


  box.addEventListener("mousedown", (e) => {
    const el = e.target.closest(".item");
    if (!el) return;
    e.preventDefault();
    input.value = el.dataset.label || "";
    input.dataset.key = el.dataset.key || "";
    hide();
    input.focus();
    onPick?.(input.dataset.key || "");
  });


  document.addEventListener("click", (e) => {
    if (e.target === input || box.contains(e.target)) return;
    hide();
  });
}


function setupItemAutocomplete() {
  setupAutocomplete({
    inputId: "advItem",
    boxId: "itemSuggest",
    endpoint: "/api/items",
    limit: 40,
    onPick: () => updateWIVisibility(),
  });
}


/* =========================
   Local autocomplete (Pet Item)
========================= */
function setupLocalAutocomplete({ inputId, boxId, list, limit = 30, onPick }) {
  const input = $(inputId);
  const box = $(boxId);
  if (!input || !box) return;


  let timer = null;


  function hide() { box.style.display = "none"; box.innerHTML = ""; }


  function render(items) {
    box.innerHTML = "";
    if (!items.length) return hide();
    box.style.display = "block";
    for (const label of items.slice(0, limit)) {
      const div = document.createElement("div");
      div.className = "item";
      div.dataset.label = label;
      div.dataset.key = toKeyFromLabel(label);
      div.textContent = label;
      box.appendChild(div);
    }
  }


  input.addEventListener("keydown", () => { input.dataset.key = ""; });


  input.addEventListener("input", () => {
    clearTimeout(timer);
    const q = input.value.trim().toLowerCase();
    if (!q) return hide();


    timer = setTimeout(() => {
      const hits = [];
      for (const it of list) {
        if (it.toLowerCase().includes(q)) hits.push(it);
        if (hits.length >= limit) break;
      }
      render(hits);
    }, 60);
  });


  box.addEventListener("mousedown", (e) => {
    const el = e.target.closest(".item");
    if (!el) return;
    e.preventDefault();
    input.value = el.dataset.label || "";
    input.dataset.key = el.dataset.key || "";
    hide();
    input.focus();
    onPick?.(input.dataset.key || "");
  });


  document.addEventListener("click", (e) => {
    if (e.target === input || box.contains(e.target)) return;
    hide();
  });
}


/* =========================
   Enchant autocomplete (comma segments)
========================= */
function setupEnchantAutocomplete() {
  const input = $("advEnchants");
  const box = $("enchSuggest");
  if (!input || !box) return;


  let timer = null;


  function currentSegmentInfo() {
    const raw = input.value || "";
    const idx = raw.lastIndexOf(",");
    if (idx === -1) return { prefix: "", seg: raw.trim() };
    return { prefix: raw.slice(0, idx + 1), seg: raw.slice(idx + 1).trim() };
  }


  function hide() { box.style.display = "none"; box.innerHTML = ""; }


  function render(items) {
    box.innerHTML = "";
    if (!Array.isArray(items) || items.length === 0) return hide();
    box.style.display = "block";
    for (const name of items.slice(0, 30)) {
      const div = document.createElement("div");
      div.className = "item";
      div.dataset.name = name;
      div.textContent = name;
      box.appendChild(div);
    }
  }


  input.addEventListener("input", () => {
    clearTimeout(timer);
    const { seg } = currentSegmentInfo();
    const q = seg.trim();
    if (!q) return hide();


    timer = setTimeout(async () => {
      try {
        const res = await fetch(`/api/enchants?q=${encodeURIComponent(q)}&limit=30`, { cache: "no-store" });
        const data = await res.json().catch(() => ({}));
        render(data.items || []);
      } catch { hide(); }
    }, 70);
  });


  box.addEventListener("mousedown", (e) => {
    const el = e.target.closest(".item");
    if (!el) return;
    e.preventDefault();
    const pick = el.dataset.name;
    const { prefix } = currentSegmentInfo();
    input.value = (prefix ? prefix.trimEnd() + " " : "") + pick + ", ";
    hide();
    input.focus();
  });


  document.addEventListener("click", (e) => {
    if (e.target === input || box.contains(e.target)) return;
    hide();
  });
}


/* =========================
   Recommend API call (NO liveSignature)
========================= */
async function fetchRecommended({
  item,
  stars10,
  enchants,
  wi,
  rarity,
  dye,
  skin,
  petlvl,
  petskin,
  petitem,
}) {
  const params = new URLSearchParams();
  params.set("item", item);
  params.set("stars10", String(stars10 || 0));
  params.set("enchants", enchants || "");


  if (wi) params.set("wi", "1");
  if (rarity) params.set("rarity", rarity);
  if (dye) params.set("dye", dye);
  if (skin) params.set("skin", skin);
  if (petlvl) params.set("petlvl", String(petlvl));
  if (petskin) params.set("petskin", petskin);
  if (petitem) params.set("petitem", petitem);


  const res = await fetch(`/api/recommend?${params.toString()}`, { cache: "no-store" });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.error || `API error (${res.status})`);
  return data;
}


/* =========================
   Render Top 3 rail
========================= */
function renderTop3Rail(top3) {
  const rail = $("top3Rail");
  if (!rail) return;


  if (!Array.isArray(top3) || top3.length === 0) {
    rail.innerHTML = `
      <div class="mini-card">
        <div class="mini-name">No echoes yet</div>
        <div class="mini-meta">Run a recommendation to reveal your closest matches.</div>
      </div>
    `;
    return;
  }


  rail.innerHTML = top3.slice(0, 3).map((m, idx) => {
    const name = escapeHtml(m.item_name ?? "—");


    const ds = Number(m.dstars ?? m.dungeonStars ?? 0);
    const ms = Number(m.mstars ?? m.masterStars ?? 0);
    const starsHtml = renderStarsHtml(ds, ms);


    const price = formatCoins(Number(m.final_price));
    const score = escapeHtml(String(Math.round(m.score ?? 0)));
    const uuid = String(m.uuid || "").trim();


    const dye = prettyFromKey(m.dye || "none");
    const skin = prettyFromKey(m.skin || "none");
    const petLevel = Number(m.petLevel || 0);
    const petSkin = prettyFromKey(m.petskin || "none");


    const keyFactors = Array.isArray(m.matched) ? m.matched.slice(0, 6) : [];
    const enchLines = Array.isArray(m.allEnchants) ? m.allEnchants.slice(0, 10) : [];


    const keyHtml = keyFactors.length ? `
      <div class="mini-meta">
        <b>Key</b>
        <div class="mini-stack">
          ${keyFactors.map((f) => {
            const ench = enchantInlineHtml(f?.enchant ?? "—");
            const add = escapeHtml((Number(f.add) || 0).toFixed(2));
            return `
              <div class="mini-row">
                <span class="mini-row-left">${ench}</span>
                <b class="mini-row-right">+${add}</b>
              </div>
            `;
          }).join("")}
        </div>
      </div>
    ` : "";


    const enchHtml = enchLines.length ? `
      <div class="mini-meta mini-scroll">
        <b>Enchants</b>
        <div class="ench-list">
          ${enchLines.map(enchantLineHtml).join("")}
        </div>
      </div>
    ` : "";


    return `
      <div class="mini-card">
        <div class="mini-head">
          <div class="mini-title">
            <div class="mini-name">${idx + 1}) ${name} ${starsHtml}</div>
            <div class="mini-tags">
              <span class="chip">Dye: <b>${escapeHtml(dye)}</b></span>
              <span class="chip">Skin: <b>${escapeHtml(skin)}</b></span>
              <span class="chip">Pet: <b>${escapeHtml(petLevel ? String(petLevel) : "—")}</b></span>
              <span class="chip">Pet Skin: <b>${escapeHtml(petSkin)}</b></span>
              <span class="chip">Score: <b>${score}</b></span>
            </div>
          </div>
          <div class="mini-price">${price}</div>
        </div>


        <div class="mini-actions">${uuid ? uuidButtonHtml(uuid) : ""}</div>
        ${keyHtml}
        ${enchHtml}
        ${uuid ? `<div class="mini-uuid">UUID: ${escapeHtml(uuid)}</div>` : ""}
      </div>
    `;
  }).join("");
}


/* =========================
   Advanced output
========================= */
function renderAdvanced(outEl, data) {
  const recRaw = data?.recommended;
  const rec = recRaw == null ? null : Number(recRaw);
  const rl = Number(data?.range_low);
  const rh = Number(data?.range_high);
  const rc = Number(data?.range_count || 0);


  const live = data?.live || null;


  const rangeText =
    rc > 0 && isFinite(rl) && isFinite(rh)
      ? `${formatShort(rl)} ~ ${formatShort(rh)}`
      : "—";


  const liveText = live
    ? `${formatCoins(Number(live.price))} ${live.bin ? "(BIN)" : "(BID)"}`
    : "—";


  const liveEnds = live?.end_ts
    ? `Ends: ${new Date(Number(live.end_ts)).toLocaleString()}`
    : "";


  outEl.innerHTML = `
    <div class="out-head">Recommended Price</div>
    <div class="out-big">${rec != null && isFinite(rec) ? escapeHtml(formatCoins(rec)) : "—"}</div>


    <div class="out-grid">
      <div class="out-box">
        <div class="out-box-k">Range</div>
        <div class="out-box-v">${escapeHtml(rangeText)} <span class="out-box-s">(Top ${rc || 0})</span></div>
      </div>
      <div class="out-box">
        <div class="out-box-k">Current Lowest Live Match</div>
        <div class="out-box-v">${escapeHtml(liveText)}</div>
        ${liveEnds ? `<div class="out-box-s">${escapeHtml(liveEnds)}</div>` : ""}
      </div>
    </div>


    <div class="out-sub">${data?.note ? escapeHtml(data.note) : "Closest matches shown in Market Echoes."}</div>
  `;


  renderTop3Rail(Array.isArray(data.top3) ? data.top3 : []);
}


/* =========================
   Advanced run
========================= */
async function runAdvancedMode() {
  const out = $("advOut");
  const btn = $("advBtn");


  const itemEl = $("advItem");
  const starsEl = $("advStars10");
  const rarityEl = $("advRarity");
  const enchEl = $("advEnchants");
  const wiEl = $("advWI");


  const dyeEl = $("advDye");
  const skinEl = $("advSkin");
  const petItemEl = $("advPetItem");
  const petLevelEl = $("advPetLevel");
  const petSkinEl = $("advPetSkin");


  if (!out || !btn || !itemEl || !starsEl || !enchEl) return;


  const item = (itemEl.dataset.key || itemEl.value || "").trim();
  const stars10 = Number(starsEl.value || 0);
  const enchants = (enchEl.value || "").trim();


  const rarity = String(rarityEl?.value || "").trim().toLowerCase();
  const dye = ((dyeEl?.dataset.key || dyeEl?.value) || "").trim();
  const skin = ((skinEl?.dataset.key || skinEl?.value) || "").trim();


  const petitem = ((petItemEl?.dataset.key || petItemEl?.value) || "").trim();
  const petlvl = petLevelEl?.value ? Number(petLevelEl.value.trim()) : 0;
  const petskin = ((petSkinEl?.dataset.key || petSkinEl?.value) || "").trim();


  const wi = !!wiEl?.checked;


  if (!item) {
    out.innerHTML = `<div class="out-head">Pick an item from suggestions.</div>`;
    renderTop3Rail([]);
    return;
  }


  btn.disabled = true;
  out.innerHTML = `<div class="out-head">Scanning the market…</div><div class="out-sub">Scoring sales + scanning live auctions.</div>`;


  try {
    const data = await fetchRecommended({
      item,
      stars10,
      enchants,
      wi,
      rarity,
      dye,
      skin,
      petlvl: petlvl > 0 ? petlvl : 0,
      petskin,
      petitem,
    });


    renderAdvanced(out, data || {});
  } catch (err) {
    out.innerHTML = `<div class="out-err">Error</div><div class="out-sub">${escapeHtml(err?.message || "Unknown error")}</div>`;
    renderTop3Rail([]);
    console.error(err);
  } finally {
    btn.disabled = false;
  }
}


/* =========================
   Wire once
========================= */
document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll(".browse-tabs .tab").forEach((t) => {
    t.addEventListener("click", () => setView(t.dataset.view));
  });


  setView("basics");


  $("calcBtn")?.addEventListener("click", runCalculator);
  $("lowballBtn")?.addEventListener("click", runLowballCalculator);


  $("advItem")?.addEventListener("input", updateWIVisibility);
  updateWIVisibility();


  $("advBtn")?.addEventListener("click", () => { setView("advanced"); runAdvancedMode(); });


  document.addEventListener("click", async (e) => {
    const btn = e.target?.closest?.(".uuid-copy-btn");
    if (!btn) return;
    e.preventDefault();
    e.stopPropagation();
    const uuid = btn.getAttribute("data-uuid") || "";
    const ok = await copyTextToClipboard(uuid);
    setUuidBtnState(btn, ok ? "Copied!" : "Copy failed", ok);
  });


  setupStars10Slider();


  setupItemAutocomplete();
  setupEnchantAutocomplete();


  setupAutocomplete({ inputId: "advDye", boxId: "dyeSuggest", endpoint: "/api/dyes", limit: 30 });
  setupAutocomplete({ inputId: "advSkin", boxId: "skinSuggest", endpoint: "/api/skins", limit: 30 });
  setupAutocomplete({ inputId: "advPetSkin", boxId: "petSkinSuggest", endpoint: "/api/petskins", limit: 30 });


  // ✅ Pet Item local autocomplete (requires: advPetItem + petItemSuggest in HTML)
  setupLocalAutocomplete({
    inputId: "advPetItem",
    boxId: "petItemSuggest",
    list: PET_ITEM_LIST,
    limit: 30,
  });


  renderTop3Rail([]);
});
