// server.js (v16 - Coflnet stars + strict partial + hard clean autocomplete + Cloud Run safe)
//
// ✅ /api/items UNIQUE item_key + UNIQUE base label (kills Terminator ➋➌➍, ②③④, etc)
// ✅ Converts circled/dingbat digits to ASCII digits BEFORE dedupe
// ✅ STRICT candidate filtering:
//    - Stars diff: 0 exact, 1 => PARTIAL, >=2 => NONE (excluded)
//    - Enchant diff: level diff rules (primary), tier diff fallback
// ✅ PARTIAL is returned as tier:"PARTIAL" (frontend renders purple)
// ✅ Stars computed Coflnet-style: "✪✪✪✪✪➌" => 8 stars10
// ✅ WI detection accepts 1/true/yes and parseLore detects scrolls case-safe
// ✅ item_key canonicalized (Hyperion vs hyperion works)

import path from "path";
import { fileURLToPath } from "url";
import express from "express";
import pg from "pg";
import dotenv from "dotenv";

import {
  normKey,
  canonicalItemKey,
  parseEnchantList,
  displayEnchant,
  normalizeEnchantKey,
  buildSignature,
  tierFor,
  // NEW export from parseLore.js v6:
  coflnetStars10FromText,
} from "./parseLore.js";

dotenv.config();

const app = express();
app.use(express.json());

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use(express.static(path.join(__dirname, "public")));

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

/* =========================
   Config
========================= */
const ITEMS_LIMIT_DEFAULT = 40;
const ITEMS_LIMIT_MAX = 60;

// ingest every 2 min -> alive window small
const LIVE_ALIVE_MS = 3 * 60 * 1000;
const LIVE_SCAN_LIMIT = 4000;

/* =========================
   Display cleaning + unicode digit normalization
========================= */
function stripStarGlyphs(s) {
  return String(s || "")
    .replace(/[✪★☆✯✰●]+/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function unicodeNormalizeBasic(s) {
  return String(s || "")
    .normalize("NFKD")
    .replace(/[’‘]/g, "'")
    .replace(/[–—]/g, "-");
}

// Map lots of “circled/dingbat” digits to ASCII digits.
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
  // (Covers what you showed: ➋➌➍ etc)
  return map;
})();

function normalizeWeirdDigits(s) {
  const x = unicodeNormalizeBasic(s);
  let out = "";
  for (const ch of x) out += DIGIT_CHAR_MAP.get(ch) ?? ch;
  return out;
}

const REFORGE_PREFIXES = [
  "Shiny","Heroic","Suspicious","Fabled","Dirty","Withered","Spicy","Sharp","Gentle","Odd","Fast","Fair","Epic",
  "Spiritual","Precise","Hasty","Neat","Grand","Rapid","Unreal","Awkward","Rich","Clean","Fierce","Heavy","Light",
  "Mythic","Titanic","Smart","Wise","Perfect","Renowned","Ancient","Giant","Necrotic","Loving","Empowered",
  "Blood Soaked","Mossy","Toil","Bustling","Bountiful","Fleet","Heated","Ambered","Fruitful","Stellar","Auspicious","Refined",
];

const REFORGE_RE = new RegExp(
  `^(${REFORGE_PREFIXES
    .sort((a, b) => b.length - a.length)
    .map((x) => x.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
    .join("|")})\\s+`,
  "i"
);

// Keep letters/numbers/spaces/'/-
function stripNonWordButKeepNice(s) {
  const x = normalizeWeirdDigits(s);
  return x
    .replace(/[✪★☆✯✰●]+/g, "")
    .replace(/[^\p{L}\p{N}\s'\-]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanDisplayName(name) {
  let s = stripNonWordButKeepNice(name);
  for (let i = 0; i < 6; i++) {
    const next = s.replace(REFORGE_RE, "").trim();
    if (next === s) break;
    s = next;
  }
  return s;
}

// Label derived from key (stable). Also normalizes weird digits in key.
function labelFromKey(itemKey) {
  let k = String(itemKey || "").trim();
  if (!k) return "";
  k = normalizeWeirdDigits(k);
  const spaced = k.replace(/_/g, " ").replace(/\s+/g, " ").trim();
  const titled = spaced.replace(/\w\S*/g, (w) => w[0].toUpperCase() + w.slice(1));
  return cleanDisplayName(titled) || titled;
}

function normLabel(label) {
  return normKey(stripNonWordButKeepNice(label)).replace(/\s+/g, " ").trim();
}

// Remove junk suffix variants:
// - trailing digits 1..10 (with OR without space)
// - roman numerals I..X
function baseLabelForDedupe(label) {
  const n = normLabel(label);
  if (!n) return "";

  let m = n.match(/^(.*?)(?:\s*)(\d{1,2})$/);
  if (m) {
    const base = (m[1] || "").trim();
    const num = Number(m[2]);
    if (base && Number.isFinite(num) && num >= 1 && num <= 10) return base;
  }

  m = n.match(/^(.*)\s+(i|ii|iii|iv|v|vi|vii|viii|ix|x)$/i);
  if (m) {
    const base = (m[1] || "").trim();
    if (base) return base;
  }

  return n;
}

/* =========================
   Signature parsing helpers
========================= */
const RESERVED_SIG_KEYS = new Set([
  "tier","dstars","mstars","stars10","stars",
  "wither_impact","witherimpact",
  "pet_level","pet_item","dye","skin","petskin",
]);

function sigGet(sig, key) {
  const parts = String(sig || "").split("|");
  for (const p of parts) {
    const i = p.indexOf(":");
    if (i <= 0) continue;
    const k = p.slice(0, i);
    if (k !== key) continue;
    return p.slice(i + 1) ?? "";
  }
  return "";
}

function sigDungeonStars(sig) {
  return Math.max(0, Math.min(5, Number(sigGet(sig, "dstars")) || 0));
}
function sigMasterStars(sig) {
  return Math.max(0, Math.min(5, Number(sigGet(sig, "mstars")) || 0));
}

// ✅ Coflnet: use stars10 if present; else dstars+mstars; else attempt to parse from signature text if any.
function sigStars10(sig) {
  const s10 = Number(sigGet(sig, "stars10")) || 0;
  if (s10 > 0) return Math.max(0, Math.min(10, Math.trunc(s10)));

  const s = Number(sigGet(sig, "stars")) || 0;
  if (s > 0) return Math.max(0, Math.min(10, Math.trunc(s)));

  const fromSplit = sigDungeonStars(sig) + sigMasterStars(sig);
  if (fromSplit > 0) return Math.max(0, Math.min(10, fromSplit));

  // last resort: parse "stars-like" stuff embedded in sig string (rare)
  return Math.max(0, Math.min(10, coflnetStars10FromText(sig) || 0));
}

function sigWI(sig) {
  const v = sigGet(sig, "wither_impact") || sigGet(sig, "witherimpact") || "";
  const t = String(v).trim().toLowerCase();
  return t === "1" || t === "true" || t === "yes";
}
function sigTier(sig) {
  return sigGet(sig, "tier") || "";
}
function sigDye(sig) {
  return sigGet(sig, "dye") || "none";
}
function sigSkin(sig) {
  return sigGet(sig, "skin") || "none";
}
function sigPetSkin(sig) {
  return sigGet(sig, "petskin") || "none";
}
function sigPetLevel(sig) {
  const n = Number(sigGet(sig, "pet_level"));
  return Number.isFinite(n) ? Math.max(0, Math.min(200, Math.trunc(n))) : 0;
}
function sigPetItem(sig) {
  return sigGet(sig, "pet_item") || "none";
}

function sigEnchantMap(sig) {
  const out = new Map();
  const parts = String(sig || "").split("|");
  for (const p of parts) {
    const i = p.indexOf(":");
    if (i <= 0) continue;
    const kRaw = p.slice(0, i);
    const vRaw = p.slice(i + 1);
    if (RESERVED_SIG_KEYS.has(kRaw)) continue;

    const lv = Number(vRaw);
    if (!Number.isFinite(lv) || lv <= 0) continue;

    const nameKey = normalizeEnchantKey(String(kRaw).replace(/_/g, " "));
    if (!nameKey) continue;

    const prev = out.get(nameKey);
    if (!prev || lv > prev) out.set(nameKey, lv);
  }
  return out;
}

/* =========================
   Stats helpers
========================= */
function median(nums) {
  const a = nums.slice().sort((x, y) => x - y);
  const n = a.length;
  if (!n) return null;
  const mid = Math.floor(n / 2);
  return n % 2 ? a[mid] : Math.round((a[mid - 1] + a[mid]) / 2);
}

/* =========================
   Tier rank (fallback only)
========================= */
const TIER_RANK = { BB: 0, B: 1, A: 2, AA: 3, AAA: 4, MISC: -1 };
function tierRank(t) {
  const k = String(t || "").toUpperCase();
  return TIER_RANK[k] ?? -1;
}

/* =========================
   Input normalize
========================= */
function normUserKey(raw) {
  const k = normKey(String(raw || "").replace(/_/g, " ")).replace(/\s+/g, "_");
  if (!k) return "";
  if (k === "none" || k === "any") return "";
  return k;
}
function parseUserPetLevel(raw) {
  const v = String(raw ?? "").trim();
  if (!v) return 0;
  const n = Number(v);
  if (!Number.isFinite(n)) return 0;
  const level = Math.trunc(n);
  return level >= 1 && level <= 200 ? level : 0;
}
function listToOptions(labels) {
  return labels.map((l) => ({
    label: l,
    key: normKey(l.replace(/_/g, " ")).replace(/\s+/g, "_"),
  }));
}
function normalizeFromOptions(raw, options) {
  const k = normUserKey(raw);
  if (!k) return "";
  const byKey = options.find((o) => o.key === k);
  if (byKey) return byKey.key;

  const nk = normKey(raw);
  const byLabel = options.find((o) => normKey(o.label) === nk);
  return byLabel ? byLabel.key : k;
}

/* =========================
   PET ITEM OPTIONS + endpoint
========================= */
const PET_ITEM_LABELS = [
  "All Skills Exp Boost","All Skills Exp Super-Boost","Antique Remedies","Bejeweled Collar","Big Teeth","Bigger Teeth",
  "Bingo Booster","Brown Bandana","Bubblegum","Burnt Texts","Combat Exp Boost","Cretan Urn","Crochet Tiger Plushie",
  "Dead Cat Food","Dwarf Turtle Shelmet","Edible Seaweed","Eerie Toy","Eerie Treat","Exp Share","Exp Share Core",
  "Fake Neuroscience Degree","Farming Exp Boost","Fishing Exp Boost","Flying Pig","Foraging Exp Boost","Four-Eyed Fish",
  "Frog Treat","Gold Claws","Grandma's Knitting Needle","Green Bandana","Guardian Lucky Claw","Hardened Scales",
  "Hephaestus Plushie","Hephaestus Relic","Hephaestus Remedies","Hephaestus Shelmet","Hephaestus Souvenir",
  "Hephaestus Urn","Iron Claws","Jerry 3D Glasses","Lucky Clover","Mining Exp Boost","Minos Relic","Party Hat",
  "Quick Claw","Radioactive Vial","Reaper Gem","Reinforced Scales","Saddle","Serrated Claws","Sharpened Claws",
  "Simple Carrot Candy","Spooky Cupcake","Textbook","Tier Boost","Tier Boost Core","Titanium Minecart","Vampire Fang",
  "Washed-up Souvenir","Yellow Bandana",
];
const PETITEM_OPTIONS = listToOptions(PET_ITEM_LABELS);

app.get("/api/petitems", (req, res) => {
  const q = normKey(req.query.q || "");
  const limit = Math.max(5, Math.min(60, Number(req.query.limit || 30)));
  const items = !q ? PETITEM_OPTIONS : PETITEM_OPTIONS.filter((x) => normKey(x.label).includes(q));
  res.json({ items: items.slice(0, limit) });
});

/* =========================
   Cosmetics options (same as yours)
========================= */
const DYE_LABELS = [
  "Aquamarine Dye","Archfiend Dye","Aurora Dye","Bingo Blue Dye","Black Ice Dye","Bone Dye","Brick Red Dye",
  "Byzantium Dye","Carmine Dye","Celadon Dye","Celeste Dye","Chocolate Dye","Copper Dye","Cyclamen Dye",
  "Dark Purple Dye","Dung Dye","Emerald Dye","Flame Dye","Fossil Dye","Frog Dye","Frostbitten Dye",
  "Hellebore Dye","Holly Dye","Iceberg Dye","Jade Dye","Kingfisher Dye","Lava Dye","Livid Dye","Lucky Dye",
  "Mango Dye","Marine Dye","Matcha Dye","Midnight Dye","Mocha Dye","Mythological Dye","Nadeshiko Dye","Necron Dye",
  "Nyanza Dye","Oasis Dye","Ocean Dye","Pastel Sky Dye","Pearlescent Dye","Pelt Dye","Periwinkle Dye","Portal Dye",
  "Pure Black Dye","Pure Blue Dye","Pure White Dye","Pure Yellow Dye","Red Tulip Dye","Rose Dye","Snowflake Dye",
  "Sunflower Dye","Sunset Dye","Tentacle Dye","Warden Dye","Wild Strawberry Dye"
];
const SKIN_LABELS = [
  "Ablaze Skin","Admiral Skin","Baby Hydra Skin","Baby Skin","Black Widow Skin","Bloom Skin",
  "Blue Oni Reaper Mask Skin","Caduceus Mender Skin","Celestial Goldor's Helmet Skin",
  "Celestial Maxor's Helmet Skin","Celestial Necron's Helmet Skin","Celestial Storm's Helmet Skin",
  "Celestial Wither Goggles Skin","Corrupt Wither Goggles Helmet Skin","Crimson Skin",
  "Cyberpunk Wither Goggles Skin","Deep Sea Skin","Diamond Skin","Ender Knight Skin","Frost Knight Skin",
  "Frozen Diver Skin","Gemstone Divan Helmet Skin","Genie Skin","Golden Skin","Great Shark Magma Lord Skin",
  "Harvester Helmet Skin","Hero Skin","Ice Hydra Skin","Iceberg Skin","Icicle Skin","Jester Bonzo's Mask Skin",
  "Leaf Skin","Lunar Rabbit Hat Skin","Mauve Skin","Meteor Magma Lord Helmet Skin","Oni Reaper Mask Skin",
  "Paladin Skin","Panda Spirit Skin","Puffer Fish Skin","Puppy Skin","Rabbit Onesie Jerry Skin",
  "Red Oni Reaper Mask Skin","Redback Skin","Reinforced Skin","Sandstorm Cat Skin","Sentinel Warden Skin",
  "Shimmer Skin","Sly Fox Skin","Smoldering Ember Skin","Snowglobe Skin","Spirit Skin","Starknight Skin",
  "Thief Skin","True Warden Skin"
];
const PET_SKIN_LABELS = [
  "Anubis Golden Dragon Skin","Ancient Golden Dragon Skin","Super Plushie Ender Dragon Skin",
  "Pastel Ender Dragon Skin","Undead Ender Dragon Skin","Neon Blue Ender Dragon Skin",
  "Neon Red Ender Dragon Skin","Neon Green Ender Dragon Skin","Neon Purple Ender Dragon Skin",
  "Neon Yellow Ender Dragon Skin","Neon Orange Ender Dragon Skin","Baby Blue Ender Dragon Skin",
  "Baby Red Ender Dragon Skin","Baby Green Ender Dragon Skin","Baby Purple Ender Dragon Skin",
  "Baby Yellow Ender Dragon Skin","Baby Orange Ender Dragon Skin",
  "Blue Whale Plushie Skin","Tiger Plushie Skin","Elephant Plushie Skin","Pig Plushie Skin","Bee Plushie Skin",
  "Cow Plushie Skin","Chicken Plushie Skin","Dog Plushie Skin","Cat Plushie Skin","Sheep Plushie Skin",
  "Baby Wither Skeleton"
];

const DYE_OPTIONS = listToOptions(DYE_LABELS);
const SKIN_OPTIONS = listToOptions(SKIN_LABELS);
const PETSKIN_OPTIONS = listToOptions(PET_SKIN_LABELS);

/* =========================
   Filter verification
========================= */
function applyVerifiedFiltersOrNull(sig, filters) {
  if (!sig) return { ok: true, unverifiable: true };

  const { userWI, userRarity, userDye, userSkin, userPetSkin, userPetLevel, userPetItem } = filters;

  if (userWI && !sigWI(sig)) return { ok: false, unverifiable: false };
  if (userRarity && sigTier(sig) !== userRarity) return { ok: false, unverifiable: false };
  if (userDye && userDye !== "none" && sigDye(sig) !== userDye) return { ok: false, unverifiable: false };
  if (userSkin && userSkin !== "none" && sigSkin(sig) !== userSkin) return { ok: false, unverifiable: false };
  if (userPetSkin && userPetSkin !== "none" && sigPetSkin(sig) !== userPetSkin) return { ok: false, unverifiable: false };
  if (userPetLevel > 0 && sigPetLevel(sig) < userPetLevel) return { ok: false, unverifiable: false };
  if (userPetItem && userPetItem !== "none" && sigPetItem(sig) !== userPetItem) return { ok: false, unverifiable: false };

  return { ok: true, unverifiable: false };
}

/* =========================
   STRICT MATCH QUALITY
   Stars: diff 0 exact, diff 1 partial, diff>=2 reject
   Enchants: LEVEL diff rules (primary), tier diff fallback
========================= */
function enchantDiff(nameKey, inputLvl, saleLvl) {
  const inL = Number(inputLvl) || 0;
  const saL = Number(saleLvl) || 0;

  // PRIMARY: level difference (your rule)
  const levelDiff = Math.abs(saL - inL);

  // FALLBACK: tier bucket diff (if you still want it to matter)
  const inTier = tierFor(nameKey, inL);
  const saTier = tierFor(nameKey, saL);
  const tierDiff =
    (inTier && saTier && inTier !== "MISC" && saTier !== "MISC")
      ? Math.abs(tierRank(saTier) - tierRank(inTier))
      : 0;

  // Use the stricter diff
  return Math.max(levelDiff, tierDiff);
}

function strictMatchQuality({ userEnchantsMap, inputStars10, sig, filters }) {
  if (!sig) return "NONE";

  const vf = applyVerifiedFiltersOrNull(sig, filters);
  if (!vf.ok) return "NONE";

  let anyPartial = false;

  const inStars = Number(inputStars10) || 0;
  if (inStars > 0) {
    const saStars = sigStars10(sig);
    const diff = Math.abs(saStars - inStars);
    if (diff === 1) anyPartial = true;
    else if (diff >= 2) return "NONE";
  }

  const saleEnchants = sigEnchantMap(sig);

  for (const [nameKey, inputLvlRaw] of userEnchantsMap.entries()) {
    const inL = Number(inputLvlRaw);
    if (!Number.isFinite(inL) || inL <= 0) continue;

    const saleLvl = Number(saleEnchants.get(nameKey) || 0);
    if (!saleLvl) return "NONE";

    const diff = enchantDiff(nameKey, inL, saleLvl);
    if (diff === 1) anyPartial = true;
    else if (diff >= 2) return "NONE";
  }

  return anyPartial ? "PARTIAL" : "PERFECT";
}

/* =========================
   Scoring (display SALE level)
========================= */
function tierBonusForTier(tier) {
  const t = String(tier || "").toUpperCase();
  return t === "AAA" ? 8 : t === "AA" ? 5 : t === "A" ? 3 : t === "B" ? 2 : t === "BB" ? 1 : 0;
}
const W_EXACT_STARS = tierBonusForTier("AAA");
const W_PARTIAL_STARS = tierBonusForTier("A");

function starsScore(inputStars10, saleStars10) {
  const inS = Math.max(0, Math.min(10, Number(inputStars10) || 0));
  const saS = Math.max(0, Math.min(10, Number(saleStars10) || 0));
  if (inS <= 0) return { add: 0, tier: null, label: null };

  const diff = Math.abs(saS - inS);
  if (diff === 0) return { add: W_EXACT_STARS, tier: "AAA", label: `Stars ${inS} → ${saS}` };
  if (diff === 1) return { add: W_PARTIAL_STARS, tier: "PARTIAL", label: `Stars ${inS} → ${saS}` };
  return { add: 0, tier: "MISC", label: `Stars ${inS} → ${saS}` };
}

function scorePartial({ userEnchantsMap, inputStars10, sig, filters }) {
  const matched = [];
  let score = 0;

  const vf = applyVerifiedFiltersOrNull(sig, filters);
  if (!vf.ok) return null;

  score += vf.unverifiable ? -2 : 2;

  if (Number(inputStars10) > 0) {
    const st = starsScore(inputStars10, sigStars10(sig));
    score += st.add;
    if (st.label) matched.push({ enchant: { tier: st.tier, label: st.label }, add: st.add });
  }

  const saleEnchants = sig ? sigEnchantMap(sig) : new Map();

  for (const [nameKey, inputLvlRaw] of userEnchantsMap.entries()) {
    const inL = Number(inputLvlRaw);
    if (!Number.isFinite(inL) || inL <= 0) continue;

    const saleLvl = Number(saleEnchants.get(nameKey) || 0);
    if (!saleLvl) continue;

    const diff = enchantDiff(nameKey, inL, saleLvl);

    let tierLabel = "MISC";
    let add = 0;

    if (diff === 0) {
      // exact -> show sale tier (gold)
      tierLabel = tierFor(nameKey, saleLvl);
      add = tierBonusForTier(tierLabel) + 1.2;
    } else if (diff === 1) {
      // partial -> purple in frontend
      tierLabel = "PARTIAL";
      add = 1.0;
    } else {
      tierLabel = "MISC";
      add = 0;
    }

    add *= 1 + Math.min(10, Math.max(0, saleLvl - 1)) * 0.08;

    score += add;
    matched.push({
      enchant: { tier: tierLabel, label: displayEnchant(nameKey, saleLvl) }, // ✅ SALE level displayed
      add,
    });
  }

  matched.sort((a, b) => (b.add ?? 0) - (a.add ?? 0));
  return { score, matched, saleEnchants, unverifiable: vf.unverifiable };
}

/* =========================
   ✅ /api/items (hard clean dedupe)
========================= */
app.get("/api/items", async (req, res) => {
  try {
    const qRaw = String(req.query.q || "").trim();
    const limit = Math.min(ITEMS_LIMIT_MAX, Math.max(1, Number(req.query.limit || ITEMS_LIMIT_DEFAULT)));
    if (!qRaw) return res.json({ items: [] });

    const q = qRaw.toLowerCase();

    const { rows } = await pool.query(
      `
      WITH c AS (
        SELECT item_key, MAX(ended_ts) AS ts
        FROM sales
        WHERE item_key IS NOT NULL AND item_key <> ''
          AND (item_key ILIKE '%' || $1 || '%' OR item_name ILIKE '%' || $1 || '%')
        GROUP BY item_key
        UNION ALL
        SELECT item_key, MAX(last_seen_ts) AS ts
        FROM auctions
        WHERE item_key IS NOT NULL AND item_key <> ''
          AND (item_key ILIKE '%' || $1 || '%' OR item_name ILIKE '%' || $1 || '%')
        GROUP BY item_key
      )
      SELECT item_key, MAX(ts) AS ts
      FROM c
      GROUP BY item_key
      ORDER BY MAX(ts) DESC
      LIMIT 1200
      `,
      [q]
    );

    const out = [];
    const seenKey = new Set();
    const seenBaseLabel = new Set();

    for (const r of rows) {
      let key = String(r.item_key || "").trim();
      if (!key) continue;

      key = normalizeWeirdDigits(key);
      if (seenKey.has(key)) continue;

      const label = labelFromKey(key);
      const base = baseLabelForDedupe(label);
      if (!base) continue;
      if (seenBaseLabel.has(base)) continue;

      seenKey.add(key);
      seenBaseLabel.add(base);
      out.push({ key, label });

      if (out.length >= limit) break;
    }

    res.json({ items: out });
  } catch (e) {
    res.status(500).json({ error: e?.message || String(e) });
  }
});

/* =========================
   /api/recommend (STRICT candidates + LBIN)
========================= */
app.get("/api/recommend", async (req, res) => {
  try {
    const now = Date.now();

    const itemKeyFromClient = String(req.query.item_key || "").trim();
    const itemRaw = itemKeyFromClient || String(req.query.item || "");
    const itemKey = canonicalItemKey(normalizeWeirdDigits(itemRaw));

    if (!itemKey) {
      return res.json({ recommended: null, top3: [], count: 0, note: "Pick an item from suggestions.", live: null });
    }

    const inputStars10 = Math.max(0, Math.min(10, Number(req.query.stars10 ?? req.query.stars ?? 0)));
    const userRarity = normUserKey(req.query.rarity || "");
    const userWI = String(req.query.wi ?? "").trim().toLowerCase() === "1"
      || String(req.query.wi ?? "").trim().toLowerCase() === "true";

    const userDye = normalizeFromOptions(req.query.dye, DYE_OPTIONS);
    const userSkin = normalizeFromOptions(req.query.skin, SKIN_OPTIONS);
    const userPetSkin = normalizeFromOptions(req.query.petskin ?? req.query.petSkin, PETSKIN_OPTIONS);
    const userPetLevel = parseUserPetLevel(req.query.petlvl ?? req.query.petLevel);
    const userPetItem = normalizeFromOptions(req.query.petitem ?? req.query.petItem, PETITEM_OPTIONS);

    const userEnchantsMap = parseEnchantList(req.query.enchants || "");
    const filters = { userWI, userRarity, userDye, userSkin, userPetSkin, userPetLevel, userPetItem };

    const since = now - 120 * 24 * 60 * 60 * 1000;

    const { rows } = await pool.query(
      `
      SELECT uuid, item_name, item_key, final_price, ended_ts, signature
      FROM sales
      WHERE item_key = $1
        AND ended_ts >= $2
      ORDER BY ended_ts DESC
      LIMIT 50000
      `,
      [itemKey, since]
    );

    const candidates = [];
    const perfectPrices = [];
    const partialPrices = [];

    for (const r of rows) {
      const price = Number(r.final_price || 0);
      if (!Number.isFinite(price) || price <= 0) continue;

      const sig = String(r.signature || "").trim();
      const quality = strictMatchQuality({ userEnchantsMap, inputStars10, sig, filters });
      if (quality === "NONE") continue;

      if (quality === "PERFECT") perfectPrices.push(price);
      else partialPrices.push(price);

      const sc = scorePartial({ userEnchantsMap, inputStars10, sig, filters });
      if (!sc) continue;

      candidates.push({
        uuid: r.uuid,
        item_name: stripStarGlyphs(r.item_name),
        final_price: price,
        ended_ts: Number(r.ended_ts) || 0,
        signature: sig,

        dstars: sig ? sigDungeonStars(sig) : 0,
        mstars: sig ? sigMasterStars(sig) : 0,
        stars10: sig ? sigStars10(sig) : 0,

        wi: sig ? sigWI(sig) : false,
        dye: sig ? sigDye(sig) : "none",
        skin: sig ? sigSkin(sig) : "none",
        petskin: sig ? sigPetSkin(sig) : "none",
        petLevel: sig ? sigPetLevel(sig) : 0,
        petItem: sig ? sigPetItem(sig) : "none",

        score: sc.score,
        matched: sc.matched,
        allEnchants: sig
          ? Array.from(sc.saleEnchants.entries()).map(([k, v]) => ({
              tier: tierFor(k, v),
              label: displayEnchant(k, v),
            }))
          : [],
        unverifiable: sc.unverifiable,
      });
    }

    candidates.sort((a, b) => (b.score - a.score) || (a.final_price - b.final_price));
    const top3 = candidates.slice(0, 3);

    const pricePool = perfectPrices.length ? perfectPrices : partialPrices;
    const med = pricePool.length ? median(pricePool) : null;
    const rangeLow = pricePool.length ? Math.min(...pricePool) : null;
    const rangeHigh = pricePool.length ? Math.max(...pricePool) : null;

    // LBIN
    const { rows: liveRows } = await pool.query(
      `
      SELECT uuid, item_name, item_key, bin, start_ts, end_ts, starting_bid,
             tier, signature, item_lore, item_bytes, last_seen_ts
      FROM auctions
      WHERE is_ended = false
        AND bin = true
        AND item_key = $1
        AND last_seen_ts >= $2
      ORDER BY starting_bid ASC
      LIMIT $3
      `,
      [itemKey, now - LIVE_ALIVE_MS, LIVE_SCAN_LIMIT]
    );

    let bestPerfect = null;
    let bestPartial = null;

    for (const a of liveRows) {
      const price = Number(a.starting_bid || 0);
      if (!Number.isFinite(price) || price <= 0) continue;

      let sig = String(a.signature || "").trim();
      if (!sig) {
        sig = String(
          (await buildSignature({
            itemName: a.item_name || "",
            lore: a.item_lore || "",
            tier: a.tier || "",
            itemBytes: a.item_bytes || "",
          })) || ""
        ).trim();
      }

      const quality = strictMatchQuality({ userEnchantsMap, inputStars10, sig, filters });
      if (quality === "NONE") continue;

      const sc = scorePartial({ userEnchantsMap, inputStars10, sig, filters });
      if (!sc) continue;

      const cand = {
        uuid: a.uuid,
        item_name: stripStarGlyphs(a.item_name),
        price,
        bin: true,
        start_ts: Number(a.start_ts || 0),
        end_ts: Number(a.end_ts || 0),
        signature: sig,
        stars10: sigStars10(sig),
        wi: sigWI(sig),
        score: sc.score,
        matched: sc.matched,
        quality,
      };

      if (quality === "PERFECT") {
        if (!bestPerfect || cand.price < bestPerfect.price) bestPerfect = cand;
      } else {
        if (!bestPartial || cand.price < bestPartial.price) bestPartial = cand;
      }
    }

    const liveBest = bestPerfect || bestPartial || null;
    const note = candidates.length ? null : "No sales found that pass strict matching in the selected history window.";

    return res.json({
      recommended: med,
      median: med,
      range_low: rangeLow,
      range_high: rangeHigh,
      range_count: pricePool.length,
      count: candidates.length,
      note,
      top3,
      live: liveBest,
    });
  } catch (err) {
    return res.status(500).json({ error: err?.message || String(err) });
  }
});

/* =========================
   Enchant autocomplete (unchanged)
========================= */
const ENCHANT_CATALOG = [
  { name: "Chimera", min: 1, max: 5 },
  { name: "Duplex", min: 1, max: 5 },
  { name: "Overload", min: 1, max: 5 },
  { name: "Dragon Hunter", min: 1, max: 5 },
  { name: "Soul Eater", min: 1, max: 5 },
  { name: "Ultimate Wise", min: 1, max: 5 },
  { name: "One For All", min: 1, max: 1 },
  { name: "Fatal Tempo", min: 1, max: 5 },
  { name: "Legion", min: 1, max: 5 },
  { name: "Last Stand", min: 1, max: 5 },
  { name: "Power", min: 6, max: 7 },
  { name: "Sharpness", min: 6, max: 7 },
  { name: "Critical", min: 6, max: 7 },
  { name: "Ender Slayer", min: 6, max: 7 },
];

const ENCHANT_CATALOG_NORM = ENCHANT_CATALOG.map((e) => ({
  name: e.name,
  key: normKey(e.name),
  min: e.min,
  max: e.max,
}));

app.get("/api/enchants", (req, res) => {
  const q = normKey(req.query.q || "");
  const LIMIT = Math.max(5, Math.min(60, Number(req.query.limit || 30)));
  if (!q) return res.json({ items: [] });

  const items = [];
  for (const e of ENCHANT_CATALOG_NORM) {
    if (!e.key.includes(q)) continue;
    for (let lv = e.max; lv >= e.min; lv--) {
      items.push(`${e.name} ${lv}`);
      if (items.length >= LIMIT) break;
    }
    if (items.length >= LIMIT) break;
  }
  return res.json({ items });
});

/* =========================
   Cosmetics endpoints + health
========================= */
app.get("/api/dyes", (req, res) => {
  const q = normKey(req.query.q || "");
  const limit = Math.max(5, Math.min(60, Number(req.query.limit || 30)));
  const items = !q ? DYE_OPTIONS : DYE_OPTIONS.filter((x) => normKey(x.label).includes(q));
  res.json({ items: items.slice(0, limit) });
});
app.get("/api/skins", (req, res) => {
  const q = normKey(req.query.q || "");
  const limit = Math.max(5, Math.min(60, Number(req.query.limit || 30)));
  const items = !q ? SKIN_OPTIONS : SKIN_OPTIONS.filter((x) => normKey(x.label).includes(q));
  res.json({ items: items.slice(0, limit) });
});
app.get("/api/petskins", (req, res) => {
  const q = normKey(req.query.q || "");
  const limit = Math.max(5, Math.min(60, Number(req.query.limit || 30)));
  const items = !q ? PETSKIN_OPTIONS : PETSKIN_OPTIONS.filter((x) => normKey(x.label).includes(q));
  res.json({ items: items.slice(0, limit) });
});
app.get("/api/health", (_req, res) => res.json({ ok: true }));

/* =========================
   Boot
========================= */
const PORT = Number(process.env.PORT || 8080);
app.listen(PORT, "0.0.0.0", () => {
  console.log(`✅ Server running on port ${PORT}`);
});
