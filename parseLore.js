// parseLore.js (v11 - ALIGNED WITH INGEST v8: safe keys, safe stars10, stable signatures)
// - canonicalItemKey NO LONGER strips normal trailing digits (only strips pet "Lvl X" prefix)
// - coflnetStars10FromText only reads master-star digit/roman if it appears immediately after the 5 stars
// - stars10 always authoritative in signature if any stars exist
// - WI robust: scroll arrays + lore fallback
// - circle-star + weird digit glyphs supported

import { gunzipSync } from "node:zlib";
import { parse as parseNbt } from "prismarine-nbt";

/* =========================
   Text normalize
========================= */
function stripMcFormatting(s) {
  return String(s ?? "").replace(/§./g, "");
}

export function cleanText(s) {
  let x = stripMcFormatting(String(s ?? "")).normalize("NFKC");
  x = x.replace(/[’]/g, "'");
  x = x.replace(/[^\p{L}\p{N}\s']/gu, " ");
  x = x.replace(/\s+/g, " ").trim();
  return x;
}

export function normKey(s) {
  return cleanText(s)
    .toLowerCase()
    .replace(/['’]/g, "")
    .replace(/[-_]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/* =========================
   Digit normalization (dingbats/circled digits => ascii)
========================= */
const DIGIT_CHAR_MAP = (() => {
  const map = new Map();
  const addRange = (startDigit, chars) => {
    for (let i = 0; i < chars.length; i++) map.set(chars[i], String(startDigit + i));
  };
  addRange(0, "⓪①②③④⑤⑥⑦⑧⑨");
  addRange(0, "０１２３４５６７８９");
  addRange(1, "➊➋➌➍➎➏➐➑➒➓");
  addRange(1, "❶❷❸❹❺❻❼❽❾❿");
  addRange(1, "⓵⓶⓷⓸⓹⓺⓻⓼⓽⓾");
  addRange(0, "⁰¹²³⁴⁵⁶⁷⁸⁹");
  addRange(0, "₀₁₂₃₄₅₆₇₈₉");
  return map;
})();

function normalizeWeirdDigits(s) {
  const x = stripMcFormatting(String(s ?? "")).normalize("NFKC");
  let out = "";
  for (const ch of x) out += DIGIT_CHAR_MAP.get(ch) ?? ch;
  return out;
}

/* =========================
   Stars from display text (COFLNET style)
   - supports circles + stars
   - supports master-digit 1..5 OR roman I..V immediately after stars
========================= */
const STAR_CHARS = new Set(["✪", "★", "☆", "✯", "✰", "●", "⬤", "○", "◉", "◎", "◍"]);
function isStarChar(ch) {
  return STAR_CHARS.has(ch);
}

// returns 0..10
export function coflnetStars10FromText(text) {
  const s = normalizeWeirdDigits(text);
  if (!s) return 0;

  // only look near the end
  const SEARCH_WINDOW = 80;
  const start = Math.max(0, s.length - SEARCH_WINDOW);
  const tail = s.slice(start);

  // find last star-like char
  let lastStarLocal = -1;
  for (let i = tail.length - 1; i >= 0; i--) {
    if (isStarChar(tail[i])) { lastStarLocal = i; break; }
  }
  if (lastStarLocal < 0) return 0;

  const lastStarIdx = start + lastStarLocal;

  // count backwards up to 5 stars with tiny separator tolerance
  let starCount = 0;
  let i = lastStarIdx;
  let sepBudget = 12;

  while (i >= 0 && starCount < 5) {
    const ch = s[i];
    if (isStarChar(ch)) { starCount++; sepBudget = 12; i--; continue; }
    if (sepBudget > 0 && /[\s·|:()\[\]{}<>~\-_=+.,]/.test(ch)) { sepBudget--; i--; continue; }
    break;
  }

  if (starCount <= 0) return 0;
  if (starCount < 5) return starCount;

  // ✅ master digit / roman must be "immediately after" the last star cluster
  const afterRaw = s.slice(lastStarIdx + 1, lastStarIdx + 24);
  const after = afterRaw.replace(/^[\s·|:()\[\]{}<>~\-_=+.,]+/, ""); // trim only separators

  // First token only
  const token = (after.match(/^([0-9]{1,2}|i{1,3}|iv|v)\b/i) || [])[1] || "";

  if (/^[1-5]$/.test(token)) return 5 + Number(token);

  const rom = token.toLowerCase();
  const ROM = { i: 1, ii: 2, iii: 3, iv: 4, v: 5 };
  if (ROM[rom]) return 5 + ROM[rom];

  return 5;
}

/* =========================
   Reforge stripping
========================= */
const REFORGE_PREFIXES = new Set([
  "hasty","precise","rapid","spiritual","fine","neat","grand","awkward","rich","headstrong","unreal",
  "fabled","withered","heroic","spicy","sharp","legendary","dirty","fanged","suspicious","bulky",
  "gilded","warped","coldfused","fair","gentle","odd","fast","jerry's",
  "ancient","giant","perfect","renowned","jaded","loving","necrotic","empowered","spiked","cubic",
  "hyper","submerged","pure","smart","clean","fierce","heavy","light","wise","titanic","mythic","waxed",
  "fortified","strengthened","glistening","blooming","rooted","royal","blood-soaked",
  "auspicious","fleet","refined","heated","ambered","magnetic","mithraic","lustrous","glacial","blazing",
  "blessed","bountiful","moil","toil","earthy","moonglade",
  "salty","treacherous","sturdy","pitchin'","lucky","aquadynamic","chilly","stiff",
]);

function tokenize(s) {
  return normKey(s).split(" ").filter(Boolean);
}

function stripReforgePrefixTokens(tokens) {
  const t = Array.isArray(tokens) ? tokens.slice() : tokenize(tokens);
  for (let i = 0; i < 2; i++) {
    if (t.length > 1 && REFORGE_PREFIXES.has(t[0])) t.shift();
    else break;
  }
  return t;
}

/* =========================
   Canonical item key (ALIGNED WITH INGEST v8)
   - strips stars/circles
   - strips reforges
   - strips “Lvl X” prefix for pets
   - DOES NOT strip normal trailing digits anymore
========================= */
export function canonicalItemKey(name) {
  let s = String(name ?? "");
  s = normalizeWeirdDigits(s);
  s = stripMcFormatting(s);

  // strip star/circle glyphs
  s = s.replace(/[✪★☆✯✰●⬤•○◉◎◍]+/g, " ");

  // remove bracketed junk
  s = s.replace(/\(([^)]*)\)/g, " ");
  s = s.replace(/\[([^\]]*)\]/g, " ");

  // separate letters+digits boundaries (rare but safe)
  s = s.replace(/(\p{L})(\d+)/gu, "$1 $2");

  let t = tokenize(s);
  if (!t.length) return "";

  // Pet “Lvl 100 Sheep” style
  if ((t[0] === "lvl" || t[0] === "lv" || t[0] === "level") && /^\d+$/.test(t[1] || "")) {
    t = t.slice(2);
    if (!t.length) return "";
  }

  // strip reforges (up to 2 tokens)
  t = stripReforgePrefixTokens(t);

  return t.join(" ");
}

export function canonicalItemDisplay(nameOrKey) {
  const k = canonicalItemKey(nameOrKey);
  if (!k) return "";
  return k
    .split(" ")
    .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : w))
    .join(" ");
}

/* =========================
   Enchants normalize + parse
========================= */
export function normalizeEnchantKey(nameKeyRaw) {
  let k = normKey(nameKeyRaw).replace(/_/g, " ");
  if (k.startsWith("ultimate ")) k = k.replace(/^ultimate\s+/, "");
  if (k.startsWith("ultimate_")) k = k.replace(/^ultimate_/, "");
  return k.trim();
}

const ROMAN = new Map([
  ["i", 1], ["ii", 2], ["iii", 3], ["iv", 4], ["v", 5],
  ["vi", 6], ["vii", 7], ["viii", 8], ["ix", 9], ["x", 10],
  ["xi", 11], ["xii", 12], ["xiii", 13], ["xiv", 14], ["xv", 15],
  ["xvi", 16], ["xvii", 17], ["xviii", 18], ["xix", 19], ["xx", 20],
]);

function romanToInt(s) {
  const k = String(s ?? "").toLowerCase().trim();
  return ROMAN.get(k) ?? NaN;
}

export function parseEnchantList(text) {
  const out = new Map();
  const items = String(text ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  for (const it of items) {
    const raw = cleanText(it);
    const parts = raw.split(/\s+/).filter(Boolean);
    if (parts.length < 2) continue;

    const lvlTok = parts[parts.length - 1];
    let lvl = Number(lvlTok);
    if (!Number.isFinite(lvl)) lvl = romanToInt(lvlTok);
    if (!Number.isFinite(lvl) || lvl <= 0) continue;

    const nameKey = normalizeEnchantKey(parts.slice(0, -1).join(" "));
    if (!nameKey) continue;

    out.set(nameKey, Math.max(out.get(nameKey) || 0, lvl));
  }

  return out;
}

export function displayEnchant(nameKeyRaw, lvl) {
  const title = normalizeEnchantKey(nameKeyRaw)
    .split(" ")
    .filter(Boolean)
    .map((w) => w[0].toUpperCase() + w.slice(1))
    .join(" ");
  return `${title} ${Number(lvl)}`;
}

/* =========================
   Enchant tiering (unchanged from your map)
========================= */
const ENCHANT_TIER_MAP = new Map();

function addTier(name, tier, levels) {
  const k = normalizeEnchantKey(name);
  if (!k) return;
  if (!ENCHANT_TIER_MAP.has(k)) ENCHANT_TIER_MAP.set(k, new Map());
  const m = ENCHANT_TIER_MAP.get(k);
  for (const lv of levels) m.set(Number(lv), String(tier).toUpperCase());
}
function addRange(name, tier, lo, hi) {
  const arr = [];
  for (let i = lo; i <= hi; i++) arr.push(i);
  addTier(name, tier, arr);
}

/* === YOUR MAP (paste unchanged below this line) === */
// (keep exactly your existing addTier/addRange calls)
addRange("Chimera", "AAA", 3, 5);
addRange("Fatal Tempo", "AAA", 3, 5);
addTier("Prosecute", "AAA", [6]);
addTier("Smoldering", "AAA", [5]);
addTier("Looting", "AAA", [5]);
addTier("First Strike", "AAA", [5]);
addTier("Critical", "AAA", [7]);
addTier("Giant Killer", "AAA", [7]);
addTier("Vicious", "AAA", [5]);
addTier("Sharpness", "AAA", [7]);
addTier("Ender Slayer", "AAA", [7]);
addTier("Power", "AAA", [7]);
addTier("Habanero Tactics", "AAA", [5]);
addTier("Growth", "AAA", [7]);
addTier("Protection", "AAA", [7]);
addTier("Expertise", "AAA", [9, 10]);
addTier("Compact", "AAA", [9, 10]);
addTier("Efficiency", "AAA", [9, 10]);
addTier("Champion", "AAA", [9, 10]);
addTier("Divine Gift", "AAA", [3]);

addRange("Chimera", "AA", 1, 2);
addTier("Fatal Tempo", "AA", [1]);
addTier("Soul Eater", "AA", [5]);
addRange("Duplex", "AA", 1, 5);
addTier("Dragon Hunter", "AA", [5]);
addTier("Vicious", "AA", [3, 4]);
addTier("Snipe", "AA", [4]);
addTier("Overload", "AA", [5]);
addTier("Tabasco", "AA", [2]);
addTier("Legion", "AA", [5]);
addTier("Refrigerate", "AA", [5]);
addTier("Counter-Strike", "AA", [5]);
addTier("Expertise", "AA", [7, 8]);
addTier("Flash", "AA", [5]);
addTier("Champion", "AA", [6, 7, 8]);
addTier("Divine Gift", "AA", [1, 2]);
addTier("Cubism", "AA", [6]);

addTier("One For All", "A", [1]);
addTier("Execute", "A", [6]);
addTier("Smite", "A", [7]);
addTier("Giant Killer", "A", [6]);
addTier("Syphon", "A", [4, 5]);
addTier("Mana Vampire", "A", [4, 5]);
addTier("Smoldering", "A", [4]);
addTier("Tabasco", "A", [3]);
addTier("Thunderlord", "A", [7]);
addTier("Thunderbolt", "A", [6, 7]);
addTier("Titan Killer", "A", [7]);
addTier("Dragon Hunter", "A", [3, 4]);
addTier("Ultimate Wise", "A", [5]);
addTier("Wisdom", "A", [5]);
addTier("Legion", "A", [1, 2, 3, 4]);
addTier("Growth", "A", [6]);
addTier("Rejuvenate", "A", [5]);
addTier("Sugar Rush", "A", [3]);
addTier("True Protection", "A", [1]);
addRange("Champion", "A", 1, 5);
addTier("Efficiency", "A", [6, 7, 8]);
addTier("Compact", "A", [5, 6, 7, 8]);
addTier("Strong Mana", "A", [5]);
addTier("Ferocious Mana", "A", [5]);
addTier("Divine Gift", "A", [1]);

addTier("Protection", "B", [6]);
addTier("Sharpness", "B", [6]);
addTier("Toxophilite", "B", [1]);
addTier("Ultimate Wise", "B", [1, 2, 3, 4]);
addTier("Bank", "B", [5]);
addTier("Rejuvenate", "B", [1, 2, 3, 4]);
addTier("Feather Falling", "B", [6, 7, 8, 9, 10]);
addTier("Infinite Quiver", "B", [10]);
addTier("Turbo-Crops", "B", [5]);
addTier("Vampirism", "B", [6]);
addTier("First Strike", "B", [4]);
addTier("Looting", "B", [4]);
addTier("Life Steal", "B", [4, 5]);
addTier("Luck", "B", [7]);
addTier("Bane of Arthropods", "B", [7]);
addTier("Pristine", "B", [1, 2, 3, 4]);
addTier("Sunder", "B", [6]);
addTier("Harvesting", "B", [6]);
addTier("Smoldering", "B", [1, 2, 3]);
addTier("Dragon Hunter", "B", [1, 2]);
addTier("Experience", "B", [4]);
addTier("Fire Aspect", "B", [3]);
addTier("Compact", "B", [1, 2, 3, 4]);
addTier("Expertise", "B", [1, 2, 3, 4]);

addTier("Bank", "BB", [1, 2, 3, 4]);
addRange("No Pain No Gain", "BB", 1, 5);
addRange("Ultimate Jerry", "BB", 1, 5);
addRange("Combo", "BB", 1, 5);
addTier("Bane of Arthropods", "BB", [6]);
addTier("Smite", "BB", [6]);
addTier("Luck", "BB", [6]);
addTier("Scavenger", "BB", [4, 5]);
addTier("Dragon Tracer", "BB", [6]);
addTier("Punch", "BB", [2]);
addTier("Rainbow", "BB", [1]);
addTier("Replenish", "BB", [1]);
addTier("Charm", "BB", [5]);
addTier("Corruption", "BB", [5]);
addTier("Sugar Rush", "BB", [1, 2]);
addTier("Fortune", "BB", [4]);
addTier("Critical", "BB", [6]);
addTier("Ender Slayer", "BB", [6]);
addTier("Power", "BB", [6]);

export function tierFor(nameKey, lvl) {
  const k = normalizeEnchantKey(nameKey);
  const n = Number(lvl);
  if (!k || !Number.isFinite(n) || n <= 0) return "MISC";
  const levels = ENCHANT_TIER_MAP.get(k);
  if (!levels) return "MISC";
  return levels.get(n) ?? "MISC";
}

/* =========================
   Signature helpers
========================= */
function toSigKey(k) {
  return normKey(k).replace(/\s+/g, "_");
}

function mapToEnchantTokens(map) {
  return Array.from(map.entries())
    .sort((a, b) => a[0].localeCompare(b[0]) || b[1] - a[1])
    .map(([k, v]) => `${toSigKey(k)}:${Number(v)}`);
}

/* =========================
   Pet held item extraction
========================= */
function canonicalPetItemKey(label) {
  const k = normKey(String(label || "")).replace(/\s+/g, "_");
  return k || "";
}

function parsePetHeldItemFromLore(loreRaw) {
  const lore = stripMcFormatting(String(loreRaw || ""));
  const lines = lore.split("\n");

  for (const rawLine of lines) {
    const line = String(rawLine).normalize("NFKC").trim();
    let m = line.match(/^(held item|pet item)\s*:\s*(.+)$/i);
    if (!m) m = line.match(/^(held item|pet item)\s+(.+)$/i);
    if (m) return cleanText(m[2]).trim();
  }
  return "";
}

function extractPetHeldItem(extra, loreRaw) {
  const candidates = [
    extra?.petItem,
    extra?.pet_item,
    extra?.heldItem,
    extra?.held_item,
    extra?.petHeldItem,
    extra?.pet_held_item,
  ];

  for (const c of candidates) {
    if (typeof c === "string" && c.trim()) {
      const label = c.replace(/_/g, " ").trim();
      const key = canonicalPetItemKey(label);
      if (key) return { label, key };
    }
  }

  const loreLabel = parsePetHeldItemFromLore(loreRaw);
  if (loreLabel) {
    const key = canonicalPetItemKey(loreLabel);
    if (key) return { label: loreLabel, key };
  }

  return { label: "", key: "" };
}

/* =========================
   NBT parsing (safe)
========================= */
async function parseItemBytes(itemBytes) {
  const b64 = String(itemBytes ?? "").trim();
  if (!b64) return null;

  let buf;
  try { buf = Buffer.from(b64, "base64"); } catch { return null; }

  let nbtBuf;
  try { nbtBuf = gunzipSync(buf); } catch { nbtBuf = buf; }

  try {
    const parsed = await new Promise((resolve) => {
      parseNbt(nbtBuf, (err, out) => resolve(err ? null : out));
    });
    return parsed?.parsed ?? parsed ?? null;
  } catch {
    return null;
  }
}

function unwrap(node) {
  if (node == null || typeof node !== "object") return node;
  if ("type" in node && "value" in node) return unwrap(node.value);
  if (Array.isArray(node)) return node.map(unwrap);
  const out = {};
  for (const [k, v] of Object.entries(node)) out[k] = unwrap(v);
  return out;
}

function findExtraAttributes(rootParsed) {
  const root = unwrap(rootParsed);
  if (!root || typeof root !== "object") return null;

  const stack = [root];
  const seen = new Set();

  while (stack.length) {
    const cur = stack.pop();
    if (!cur || typeof cur !== "object" || seen.has(cur)) continue;
    seen.add(cur);

    if (cur.ExtraAttributes && typeof cur.ExtraAttributes === "object") return cur.ExtraAttributes;
    if (cur.tag?.ExtraAttributes && typeof cur.tag.ExtraAttributes === "object") return cur.tag.ExtraAttributes;

    for (const v of Object.values(cur)) {
      if (v && typeof v === "object") stack.push(v);
    }
  }
  return null;
}

/* =========================
   Extract features
========================= */
function extractEnchants(extra) {
  const ench = new Map();

  if (extra?.enchantments && typeof extra.enchantments === "object") {
    for (const [k, v] of Object.entries(extra.enchantments)) {
      const nk = normalizeEnchantKey(String(k).replace(/_/g, " "));
      const lv = Number(v);
      if (nk && Number.isFinite(lv) && lv > 0) ench.set(nk, Math.max(ench.get(nk) || 0, lv));
    }
  }

  const ue = extra?.ultimate_enchant;
  if (ue) {
    let name = "";
    let lv = 0;

    if (typeof ue === "string") {
      const m = ue.match(/^([A-Z_]+)_(\d{1,2})$/);
      if (m) { name = m[1]; lv = Number(m[2]); }
    } else if (typeof ue === "object") {
      name = String(ue.enchant ?? ue.enchantment ?? ue.id ?? "");
      lv = Number(ue.level ?? ue.lvl ?? ue.tier ?? 0);
    }

    const nk = normalizeEnchantKey(String(name).replace(/_/g, " "));
    if (nk && Number.isFinite(lv) && lv > 0) ench.set(nk, Math.max(ench.get(nk) || 0, lv));
  }

  return ench;
}

function extractStars(extra, itemName, loreRaw) {
  const dRaw = Number(extra?.dungeon_item_level ?? 0);
  const uRaw = Number(extra?.upgrade_level ?? 0);

  const d10 = Number.isFinite(dRaw) ? Math.max(0, Math.min(10, Math.trunc(dRaw))) : 0;
  const u10 = Number.isFinite(uRaw) ? Math.max(0, Math.min(10, Math.trunc(uRaw))) : 0;

  // TOTAL 6..10
  if (d10 > 5) return { dstars: 5, mstars: Math.max(0, Math.min(5, d10 - 5)) };
  if (u10 > 5) return { dstars: 5, mstars: Math.max(0, Math.min(5, u10 - 5)) };

  // both present 1..5 -> dungeon + master
  if (d10 > 0 && u10 > 0) {
    const dstars = Math.max(0, Math.min(5, d10));
    const mstars = Math.max(0, Math.min(5, u10));
    return mstars > 0 && dstars === 0 ? { dstars: 5, mstars } : { dstars, mstars };
  }

  if (d10 > 0) return { dstars: Math.max(0, Math.min(5, d10)), mstars: 0 };

  // upgrade only: could be master stars if text shows >=5
  if (u10 > 0) {
    const total = Math.max(coflnetStars10FromText(itemName), coflnetStars10FromText(loreRaw));
    if (total >= 5) return { dstars: 5, mstars: Math.max(0, Math.min(5, u10)) };
    return { dstars: Math.max(0, Math.min(5, u10)), mstars: 0 };
  }

  // fallback from text
  const total = Math.max(coflnetStars10FromText(itemName), coflnetStars10FromText(loreRaw));
  if (total <= 0) return { dstars: 0, mstars: 0 };
  if (total <= 5) return { dstars: total, mstars: 0 };
  return { dstars: 5, mstars: total - 5 };
}

function extractPetLevel(extra, itemName) {
  const petInfo = extra?.petInfo;
  if (typeof petInfo === "string") {
    try {
      const obj = JSON.parse(petInfo);
      const lv = Number(obj?.level);
      if (Number.isFinite(lv) && lv >= 1 && lv <= 200) return Math.trunc(lv);
    } catch {}
  }

  const t = tokenize(itemName);
  if ((t[0] === "lvl" || t[0] === "lv" || t[0] === "level") && /^\d+$/.test(t[1] || "")) {
    const lv = Number(t[1]);
    if (Number.isFinite(lv) && lv >= 1 && lv <= 200) return Math.trunc(lv);
  }

  return 0;
}

function extractCosmetics(extra) {
  const dye = typeof extra?.dye_item === "string" ? toSigKey(extra.dye_item.replace(/_/g, " ")) : "";
  const skin = typeof extra?.skin === "string" ? toSigKey(extra.skin.replace(/_/g, " ")) : "";
  const petSkinRaw = extra?.petSkin ?? extra?.pet_skin ?? "";
  const petskin =
    typeof petSkinRaw === "string" && petSkinRaw ? toSigKey(String(petSkinRaw).replace(/_/g, " ")) : "";
  return { dye: dye || "none", skin: skin || "none", petskin: petskin || "none" };
}

function extractWitherImpactFlag(itemName, loreRaw, extra) {
  const k = normKey(canonicalItemKey(itemName));
  const isBlade =
    k.includes("hyperion") || k.includes("astraea") || k.includes("scylla") || k.includes("valkyrie");
  if (!isBlade) return false;

  const lore = stripMcFormatting(String(loreRaw || "")).toLowerCase();
  if (lore.includes("wither impact")) return true;

  const vals = [];
  const pushAny = (v) => {
    if (!v) return;
    if (Array.isArray(v)) for (const x of v) vals.push(String(x));
    else vals.push(String(v));
  };

  pushAny(extra?.ability_scroll);
  pushAny(extra?.ability_scrolls);
  pushAny(extra?.scrolls);
  pushAny(extra?.wither_scrolls);

  for (const [kk, vv] of Object.entries(extra || {})) {
    if (String(kk).toLowerCase().includes("scroll")) pushAny(vv);
  }

  const joined = vals.join("|").toLowerCase();
  return (
    joined.includes("implosion_scroll") &&
    joined.includes("shadow_warp_scroll") &&
    joined.includes("wither_shield_scroll")
  );
}

/* =========================
   BUILD SIGNATURE
========================= */
export async function buildSignature({ itemName = "", lore = "", tier = "", itemBytes = "" } = {}) {
  const rootParsed = await parseItemBytes(itemBytes);
  const extra = findExtraAttributes(rootParsed) || {};

  const enchMap = extractEnchants(extra);
  const enchTokens = mapToEnchantTokens(enchMap);

  let { dstars, mstars } = extractStars(extra, itemName, lore);

  // guarantee: mstars implies dstars=5
  if (mstars > 0 && dstars === 0) dstars = 5;

  const stars10 = Math.max(0, Math.min(10, (dstars || 0) + (mstars || 0)));

  const hasWI = extractWitherImpactFlag(itemName, lore, extra);
  const petLevel = extractPetLevel(extra, itemName);

  const { dye, skin, petskin } = extractCosmetics(extra);
  const petHeld = extractPetHeldItem(extra, lore);

  const tierKey = normKey(tier).replace(/\s+/g, "_");

  const parts = [];
  if (tierKey) parts.push(`tier:${tierKey}`);
  if (dstars) parts.push(`dstars:${dstars}`);
  if (mstars) parts.push(`mstars:${mstars}`);
  if (stars10) parts.push(`stars10:${stars10}`);
  if (hasWI) parts.push("wither_impact:1");
  if (petLevel) parts.push(`pet_level:${petLevel}`);
  if (dye && dye !== "none") parts.push(`dye:${dye}`);
  if (skin && skin !== "none") parts.push(`skin:${skin}`);
  if (petskin && petskin !== "none") parts.push(`petskin:${petskin}`);
  if (petHeld?.key) parts.push(`pet_item:${petHeld.key}`);

  return [...parts, ...enchTokens].join("|");
}
