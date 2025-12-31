// parseLore.js (FULL WORKING v5 - adds PET HELD ITEM + keeps everything else stable)
// Exports used by server.js/ingest.js:
// cleanText, normKey, normalizeEnchantKey,
// canonicalItemKey, canonicalItemDisplay,
// parseEnchantList, displayEnchant,
// tierFor,
// buildSignature({ itemName, lore, tier, itemBytes })
//
// NEW in v5:
// - Signature now includes pet_item:<key> when detectable (so Legendary Ender Dragon Tier Boost can be filtered)
// - Pet item is extracted from:
//   1) ExtraAttributes (if present)
//   2) Lore lines: "Held Item: ..." or "Pet Item: ..."
//   3) (Fallback) nothing => not included (server should treat missing as "unverifiable")


import { gunzipSync } from "node:zlib";
import { parse as parseNbt } from "prismarine-nbt";


/* =========================
   Text normalize
========================= */
export function cleanText(s) {
  let x = String(s ?? "").normalize("NFKC");
  x = x.replace(/§./g, ""); // MC color codes
  x = x.replace(/[’]/g, "'"); // normalize apostrophes
  x = x.replace(/[^\p{L}\p{N}\s']/gu, " "); // letters/numbers/spaces/apostrophes
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
   Unicode variant stripping
   (Fixes "Dark Claymore ➊" etc)
========================= */
const CIRCLED_DIGITS = "⓪①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳";
const DINGBAT_DIGITS = "➊➋➌➍➎➏➐➑➒➓";
const OTHER_VARIANT_CHARS_RE =
  /[\u24EA\u2460-\u2473\u24F4-\u24FF\u2776-\u277F\u2780-\u2793\u278A-\u2793]/gu;


function stripVariantDigits(s) {
  const str = String(s ?? "").normalize("NFKC");
  const removed = str
    .replace(OTHER_VARIANT_CHARS_RE, " ")
    .split("")
    .filter((ch) => !CIRCLED_DIGITS.includes(ch) && !DINGBAT_DIGITS.includes(ch))
    .join("");
  return removed;
}


/* =========================
   Reforge stripping
========================= */
const REFORGE_PREFIXES = new Set([
  // bows
  "hasty","precise","rapid","spiritual","fine","neat","grand","awkward","rich","headstrong","unreal",
  // weapons
  "fabled","withered","heroic","spicy","sharp","legendary","dirty","fanged","suspicious","shiny","bulky",
  "gilded","warped","coldfused","fair","gentle","odd","fast","jerry's",
  // armor
  "ancient","giant","perfect","renowned","jaded","loving","necrotic","empowered","spiked","cubic",
  "hyper","submerged","pure","smart","clean","fierce","heavy","light","wise","titanic","mythic","waxed",
  "fortified","strengthened","glistening","blooming","rooted","royal","blood-soaked",
  // tools
  "auspicious","fleet","refined","heated","ambered","magnetic","mithraic","lustrous","glacial","blazing",
  "blessed","bountiful","moil","toil","earthy","moonglade",
  // fishing
  "salty","treacherous","sturdy","pitchin'","lucky","aquadynamic","chilly","stiff",
]);


function tokenize(s) {
  return normKey(s).split(" ").filter(Boolean);
}


function stripReforgePrefixTokens(tokens) {
  const t = Array.isArray(tokens) ? tokens.slice() : tokenize(tokens);
  for (let i = 0; i < 4; i++) {
    if (t.length > 1 && REFORGE_PREFIXES.has(t[0])) t.shift();
    else break;
  }
  return t;
}


/* =========================
   Canonical item key
========================= */
export function canonicalItemKey(name) {
  let s = String(name ?? "");


  s = stripVariantDigits(s);
  s = s.replace(/§./g, "");
  s = s.replace(/[✪★☆✯✰]+/g, " ");


  s = s.replace(/\(([^)]*)\)/g, " ");
  s = s.replace(/\[([^\]]*)\]/g, " ");


  s = s.replace(/\b\d+\s*[*★✪]\b/g, " ");


  s = s.replace(/(\p{L})(\d+)/gu, "$1 $2");


  let t = tokenize(s);
  if (!t.length) return "";


  if ((t[0] === "lvl" || t[0] === "lv" || t[0] === "level") && /^\d+$/.test(t[1] || "")) {
    t = t.slice(2);
    if (!t.length) return "";
  }


  t = stripReforgePrefixTokens(t);


  while (t.length > 1 && /^\d+$/.test(t[t.length - 1])) t.pop();


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
   Enchant tiering (YOUR LIST)
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


/* AAA */
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


/* AA */
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


/* A */
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


/* B */
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


/* BB */
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


/**
 * Build an enchant autocomplete catalog directly from ENCHANT_TIER_MAP.
 * Keeps tiers aligned with strict matching logic.
 */
export function getEnchantCatalog() {
  const out = [];
  for (const [key, levels] of ENCHANT_TIER_MAP.entries()) {
    const lvls = Array.from(levels.keys()).map(Number).filter((n)=>Number.isFinite(n)).sort((a,b)=>a-b);
    if (!lvls.length) continue;
    out.push({ name: displayEnchantName(key), key, min: lvls[0], max: lvls[lvls.length-1] });
  }
  out.sort((a,b)=>a.name.localeCompare(b.name));
  return out;
}


function displayEnchantName(nameKey) {
  // Turn normalized key back into a readable label (best-effort)
  const s = String(nameKey || "").replace(/_/g, " ");
  return s.replace(/\w/g, (c) => c.toUpperCase());
}


/* =========================
   Signature build helpers
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
   Pet held-item extraction (NEW)
========================= */
function canonicalPetItemKey(label) {
  const k = normKey(String(label || "")).replace(/\s+/g, "_");
  return k || "";
}


function stripMcColors(s) {
  return String(s ?? "").replace(/§./g, "");
}


function parsePetHeldItemFromLore(loreRaw) {
  const lore = String(loreRaw || "");
  const lines = lore.split("\n").map(stripMcColors);


  for (const rawLine of lines) {
    const line = rawLine.normalize("NFKC").trim();


    // Match both:
    // "Held Item: Hephaestus Relic"
    // "Held Item Hephaestus Relic" (in case formatting differs)
    let m = line.match(/^(held item|pet item)\s*:\s*(.+)$/i);
    if (!m) m = line.match(/^(held item|pet item)\s+(.+)$/i);


    if (m) return cleanText(m[2]).trim();
  }
  return "";
}




function extractPetHeldItem(extra, loreRaw) {
  // Try ExtraAttributes first (names vary across versions)
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
      // some store "TIER_BOOST" -> normalize nicely
      const label = c.replace(/_/g, " ").trim();
      const key = canonicalPetItemKey(label);
      if (key) return { label, key };
    }
  }


  // Fallback: parse from lore if present
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
  try {
    buf = Buffer.from(b64, "base64");
  } catch {
    return null;
  }


  let nbtBuf;
  try {
    nbtBuf = gunzipSync(buf);
  } catch {
    nbtBuf = buf;
  }


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
   Extract features from ExtraAttributes
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
      if (m) {
        name = m[1];
        lv = Number(m[2]);
      }
    } else if (typeof ue === "object") {
      name = String(ue.enchant ?? ue.enchantment ?? ue.id ?? "");
      lv = Number(ue.level ?? ue.lvl ?? ue.tier ?? 0);
    }


    const nk = normalizeEnchantKey(String(name).replace(/_/g, " "));
    if (nk && Number.isFinite(lv) && lv > 0) ench.set(nk, Math.max(ench.get(nk) || 0, lv));
  }


  return ench;
}


function extractStars(extra) {
  // Hypixel data is inconsistent:
  // - sometimes dungeon_item_level is 0..5 (dungeon stars)
  // - sometimes it is 0..10 (total stars incl. master)
  // - sometimes upgrade_level is 0..10 (total)
  const dRaw = Number(extra?.dungeon_item_level ?? 0);
  const uRaw = Number(extra?.upgrade_level ?? 0);

  const d = Number.isFinite(dRaw) ? Math.max(0, Math.min(10, Math.trunc(dRaw))) : 0;
  const u = Number.isFinite(uRaw) ? Math.max(0, Math.min(10, Math.trunc(uRaw))) : 0;

  const total = Math.max(d, u); // best guess total stars (0..10)

  if (total <= 0) return { dstars: 0, mstars: 0 };
  if (total <= 5) return { dstars: total, mstars: 0 };

  return { dstars: 5, mstars: Math.max(0, Math.min(5, total - 5)) };
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
  const dye =
    typeof extra?.dye_item === "string" ? toSigKey(extra.dye_item.replace(/_/g, " ")) : "";


  const skin =
    typeof extra?.skin === "string" ? toSigKey(extra.skin.replace(/_/g, " ")) : "";


  const petSkinRaw = extra?.petSkin ?? extra?.pet_skin ?? "";
  const petskin =
    typeof petSkinRaw === "string" && petSkinRaw ? toSigKey(String(petSkinRaw).replace(/_/g, " ")) : "";


  return {
    dye: dye || "none",
    skin: skin || "none",
    petskin: petskin || "none",
  };
}


function extractWitherImpactFlag(itemName, rootParsed) {
  const key = canonicalItemKey(itemName);
  const isBlade = ["hyperion", "astraea", "scylla", "valkyrie"].some((w) => key.includes(w));
  if (!isBlade) return false;


  const s = JSON.stringify(unwrap(rootParsed) || {});
  return s.includes("IMPLOSION_SCROLL") && s.includes("SHADOW_WARP_SCROLL") && s.includes("WITHER_SHIELD_SCROLL");
}


/* =========================
   BUILD SIGNATURE
========================= */
export async function buildSignature({ itemName = "", lore = "", tier = "", itemBytes = "" } = {}) {
  const rootParsed = await parseItemBytes(itemBytes);
  const extra = findExtraAttributes(rootParsed) || {};


  const enchMap = extractEnchants(extra);
  const enchTokens = mapToEnchantTokens(enchMap);


  const { dstars, mstars } = extractStars(extra);
  const hasWI = extractWitherImpactFlag(itemName, rootParsed);
  const petLevel = extractPetLevel(extra, itemName);


  const { dye, skin, petskin } = extractCosmetics(extra);


  // NEW: pet held item
  const petHeld = extractPetHeldItem(extra, lore);


  const tierKey = normKey(tier).replace(/\s+/g, "_");


  const parts = [];
  if (tierKey) parts.push(`tier:${tierKey}`);
  if (dstars) parts.push(`dstars:${dstars}`);
  if (mstars) parts.push(`mstars:${mstars}`);
  if (hasWI) parts.push("wither_impact:1");
  if (petLevel) parts.push(`pet_level:${petLevel}`);
  if (dye && dye !== "none") parts.push(`dye:${dye}`);
  if (skin && skin !== "none") parts.push(`skin:${skin}`);
  if (petskin && petskin !== "none") parts.push(`petskin:${petskin}`);


  // Only include if detected (server can treat missing as unverifiable)
  if (petHeld?.key) parts.push(`pet_item:${petHeld.key}`);


  return [...parts, ...enchTokens].join("|");
}
