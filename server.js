// server.js (clean rewrite - fixes lvlDiff redeclare + strict matching rules)
//
// Key rules enforced everywhere:
// - enchant level diff 0 => PERFECT (gold)
// - enchant level diff 1 => PARTIAL (purple)
// - enchant level diff >=2 => NOT A MATCH (skip auction entirely)
// Same for stars10 (0/1/>=2).
//
// Live BIN (LBIN):
// - cheapest PERFECT else cheapest PARTIAL
// Recommended price:
// - median(PERFECT) else median(PARTIAL) else null

import path from "path";
import { fileURLToPath } from "url";
import express from "express";
import pg from "pg";
import dotenv from "dotenv";

import {
  normKey,
  canonicalItemKey,
  canonicalItemDisplay,
  parseEnchantList,
  displayEnchant,
  normalizeEnchantKey,
  buildSignature,
  tierFor,
  getEnchantCatalog,
} from "./parseLore.js";

dotenv.config();

const app = express();
app.use(express.json());

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use(express.static(path.join(__dirname, "public")));

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

/* =========================
   Display helper (prevents double star icons)
========================= */
function stripStarGlyphs(s) {
  return String(s || "")
    .replace(/[✪★☆✯✰⭐●•]+/g, "")
    .replace(/[\u2460-\u2473\u2776-\u277F\u2780-\u2793]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

/* =========================
   Signature helpers
========================= */
const RESERVED_SIG_KEYS = new Set([
  "tier",
  "dstars",
  "mstars",
  "wither_impact",
  "pet_level",
  "pet_item",
  "dye",
  "skin",
  "petskin",
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
function sigStars10(sig) {
  return Math.max(0, Math.min(10, sigDungeonStars(sig) + sigMasterStars(sig)));
}

function sigWI(sig) {
  return sigGet(sig, "wither_impact") === "1";
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

/* =========================
   Options helpers
========================= */
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
const PETITEM_OPTIONS = listToOptions(PET_ITEM_LABELS);

app.get("/api/petitems", (req, res) => {
  const q = normKey(req.query.q || "");
  const limit = Math.max(5, Math.min(60, Number(req.query.limit || 30)));
  const items = !q
    ? PETITEM_OPTIONS
    : PETITEM_OPTIONS.filter((x) => normKey(x.label).includes(q));
  res.json({ items: items.slice(0, limit) });
});

/* =========================
   Cosmetics options (your lists kept)
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
const PET_SKIN_LABELS = ["Anubis Golden Dragon Skin","Ancient Golden Dragon Skin","Super Plushie Ender Dragon Skin",
  "Pastel Ender Dragon Skin","Undead Ender Dragon Skin","Neon Blue Ender Dragon Skin",
  "Neon Red Ender Dragon Skin","Neon Green Ender Dragon Skin","Neon Purple Ender Dragon Skin",
  "Neon Yellow Ender Dragon Skin","Neon Orange Ender Dragon Skin","Baby Blue Ender Dragon Skin",
  "Baby Red Ender Dragon Skin","Baby Green Ender Dragon Skin","Baby Purple Ender Dragon Skin",
  "Baby Yellow Ender Dragon Skin","Baby Orange Ender Dragon Skin",
  "Blue Whale Plushie Skin","Tiger Plushie Skin","Elephant Plushie Skin","Pig Plushie Skin","Bee Plushie Skin",
  "Cow Plushie Skin","Chicken Plushie Skin","Dog Plushie Skin","Cat Plushie Skin","Sheep Plushie Skin",
  "Pink Plushie Megalodon Skin","Plushie Tyrannosaurus Skin","Dark Blue Plushie Elephant Skin",
  "Purple Plushie Elephant Skin","Teal Plushie Elephant Skin","White Plushie Elephant Skin",
  "Onyx Plush Baby Yeti Skin",
  "Golden Dragon Level 1-200 Variants","Safari Elephant Skin","Catgirl Black Cat Skin","Monster Sheep Skin",
  "Gummy Worm Scatha Skin","Cute Jellyfish Skin","Shell Shulked Turtle Skin","Spirit Orca Blue Whale Skin",
  "Snow Crow Skin","Seasonal Griffin Skin",
  "Molten Glacite Golem Skin","Cavern Glacite Golem Skin","Seagull Phoenix Skin","Flamingo Phoenix Skin",
  "Gateway Endermite Skin","Cloud Mammoth Skin","Void Mammoth Skin","Genie Baby Goblin Skin","Jinn Goblin Skin",
  "Red Panda Ocelot Skin","Panther Ocelot Skin","Bamboo Giraffe Skin","Cherry Giraffe Skin","Warped Giraffe Skin",
  "Galaxy Parrot Skin","Buccaneer Parrot Skin","Toucan Parrot Skin","Gold Macaw Parrot Skin",
  "Blue Chick Chicken Skin","Black Chick Chicken Skin","Pink Chick Chicken Skin","Turkey Chicken Skin",
  "Rubber Chicken Chicken Skin","Banana Slug Skin","Cake Snail Skin","Lion Tamarin Monkey Skin","Lemur Monkey Skin",
  "Golden Monkey Skin","Melting Snowman Skin","Ice Golem Snowman Skin","Silbrrrfish Silverfish Skin",
  "Fossil T-Rex Skin","Baby Blue T-Rex Skin","Magma T-Rex Skin","Toxic T-Rex Skin","Jungle T-Rex Skin",
  "Glacial Hedgehog Skin","Baby Emperor Penguin Skin","Field Mouse Rat Skin","Ninja Rat Skin","PiRate Rat Skin",
  "Rat-stronaut Rat Skin","SecRat Service Rat Skin","SecuRaty Guard Rat Skin","Squeakheart Rat Skin",
  "Despair Enderman Skin","Xenon Enderman Skin","Neon Enderman Skin","Nebula Enderman Skin","Dark Star Enderman Skin",
  "Despair Wither Skeleton Skin","Dark Wither Skeleton Skin","Candy Slime Spirit Skin","Fairy Slime Spirit Skin",
  "Elemental Water Spirit Skin","Elemental Fire Spirit Skin","Elemental Earth Spirit Skin","Elemental Air Spirit Skin",
  "Mummy Jerry Skin","Handsome Jerry Skin","Leprechaun Jerry Skin","Red Elf Jerry Skin","Green Elf Jerry Skin",
  "Fenrir Wolf Skin","Husky Wolf Skin","Dark Wolf Skin","Hellhound Wolf Skin","Skeleton Dog Wolf Skin",
  "Loafed Tiger Skin","Golden Tiger Skin","Neon Tiger Skin","Saber-Tooth Tiger Skin",
  "Loafed Black Cat Skin","Cardboard Box Black Cat Skin","Armaron Armadillo Skin","Enchanted Armadillo Skin",
  "Seafoam Armadillo Skin","Glacial Armadillo Skin","Blizzard Bal Skin","Inferno Bal Skin",
  "Black Lion Skin","White Lion Skin","Moonbloom Mooshroom Cow Skin","Moocelium Mooshroom Cow Skin",
  "Hermit Baked Beans Ammonite Skin","Hermit Paua Shell Ammonite Skin","Hermit Sand Castle Ammonite Skin",
  "Hermit Beach Ball Ammonite Skin","Hermit Graphite Ammonite Skin",
  "Midnight Dolphin Skin","Snubfin Dolphin Skin","Green Snubfin Dolphin Skin","Red Snubfin Dolphin Skin",
  "Purple Snubfin Dolphin Skin",
  "Harlequin Flying Fish Skin","Chromari Squid Skin","Glow Squid Skin","Real Grandma Wolf Skin","End Golem Skin",
  "Miner Mole Skin","Choco Magma Cube Skin","Pot O' Gold Rock Skin","Candy Cane Rock Skin","Ice Rock Skin",
  "Black Widow Spider Skin","Peacock Spider Skin","Pink Tarantula Skin","Greenbottle Tarantula Skin",
  "Cosmic Blue Whale Skin","Megalodon Shark Skin","Tiger Shark Skin","Great White Shark Skin","Whale Shark Skin",
  "Neon Blue Megalodon Skin","Baby Megalodon Skin","Chroma Sheep Skin","White Wooly Sheep Skin","Black Wooly Sheep Skin",
  "Chromatic Crush Sheep Skin","Purple Crushed Sheep Skin","Blue Crush Sheep Skin",
  "Luminescent Jellyfish Skin","RGBee Bee Skin","Loyalty Kuudra Skin",
  "Reindrake Griffin Skin","Aurora Reindeer Skin","Red Nose Reindeer Skin","Rudolph Reindeer Skin",
  "Krampus Reindeer Skin","Jingle Bell Reindeer Skin","Peafowl Griffin Skin",
  "Baby Ghast","Baby Magma Cube","Baby Slime","Baby Silverfish","Baby Spider","Baby Cave Spider","Baby Zombie",
  "Baby Skeleton","Baby Creeper","Baby Enderman","Baby Bee","Baby Chicken","Baby Cow","Baby Pig","Baby Sheep",
  "Baby Rabbit","Baby Ocelot","Baby Wolf","Baby Parrot","Baby Horse","Baby Squid","Baby Bat","Baby Jerry",
  "Baby Wither Skeleton"
];

const DYE_OPTIONS = listToOptions(DYE_LABELS);
const SKIN_OPTIONS = listToOptions(SKIN_LABELS);
const PETSKIN_OPTIONS = listToOptions(PET_SKIN_LABELS);

/* =========================
   Filter verification (strict)
========================= */
function applyVerifiedFiltersOrNull(sig, filters) {
  if (!sig) return { ok: true, unverifiable: true };

  const {
    userWI,
    userRarity,
    userDye,
    userSkin,
    userPetSkin,
    userPetLevel,
    userPetItem,
  } = filters;

  if (userWI && !sigWI(sig)) return { ok: false, unverifiable: false };
  if (userRarity && sigTier(sig) !== userRarity) return { ok: false, unverifiable: false };
  if (userDye && userDye !== "none" && sigDye(sig) !== userDye) return { ok: false, unverifiable: false };
  if (userSkin && userSkin !== "none" && sigSkin(sig) !== userSkin) return { ok: false, unverifiable: false };
  if (userPetSkin && userPetSkin !== "none" && sigPetSkin(sig) !== userPetSkin) return { ok: false, unverifiable: false };
  if (userPetLevel > 0 && sigPetLevel(sig) < userPetLevel) return { ok: false, unverifiable: false };

  // Pet Item filter must match exactly
  if (userPetItem && userPetItem !== "none" && sigPetItem(sig) !== userPetItem) {
    return { ok: false, unverifiable: false };
  }

  return { ok: true, unverifiable: false };
}

/* =========================
   Strict match quality (PERFECT / PARTIAL / NONE)
========================= */
function strictMatchQuality({ userEnchantsMap, inputStars10, sig, filters }) {
  const wantsStarFiltering = (Number(inputStars10) || 0) > 0;
  const wantsEnchantFiltering = !!userEnchantsMap && userEnchantsMap.size > 0;

  const wantsCosmeticFiltering =
    (!!filters?.userDye && filters.userDye !== "none") ||
    (!!filters?.userSkin && filters.userSkin !== "none") ||
    (!!filters?.userPetSkin && filters.userPetSkin !== "none") ||
    (Number(filters?.userPetLevel || 0) > 0) ||
    (!!filters?.userPetItem && filters.userPetItem !== "none") ||
    !!filters?.userWI ||
    !!filters?.userRarity;

  const needsSignature = wantsStarFiltering || wantsEnchantFiltering || wantsCosmeticFiltering;

  // ✅ If user isn't filtering by anything that requires signature,
  // let signature-less auctions pass as "PERFECT" (so LBIN can work).
  if (!sig) return needsSignature ? "NONE" : "PERFECT";

  const vf = applyVerifiedFiltersOrNull(sig, filters);
  if (!vf.ok) return "NONE";

  let anyPartial = false;

  // stars: diff 0 exact, diff 1 partial, diff >=2 none
  const inStars = Number(inputStars10) || 0;
  if (inStars > 0) {
    const saStars = sigStars10(sig);
    const diff = Math.abs(saStars - inStars);
    if (diff === 1) anyPartial = true;
    else if (diff >= 2) return "NONE";
  }

  // enchants: diff 0 exact, diff 1 partial, diff >=2 none
  const saleEnchants = sigEnchantMap(sig);

  for (const [nameKey, inputLvlRaw] of userEnchantsMap.entries()) {
    const inL = Number(inputLvlRaw);
    if (!Number.isFinite(inL) || inL <= 0) continue;

    const saleLvl = Number(saleEnchants.get(nameKey) || 0);
    if (!saleLvl) return "NONE";

    const lvlDiff = Math.abs(saleLvl - inL);
    if (lvlDiff === 1) anyPartial = true;
    else if (lvlDiff >= 2) return "NONE";
  }

  return anyPartial ? "PARTIAL" : "PERFECT";
}


/* =========================
   Scoring (only called after strictMatchQuality != NONE)
   Produces matched list with proper tiers:
   - EXACT: gold (uses real tier)
   - PARTIAL: purple ("PARTIAL")
========================= */
const TIER_BONUS = { BB: 1, B: 2, A: 3, AA: 5, AAA: 8, PARTIAL: 0.6, MISC: 0 };
function tierBonusForTier(t) {
  const k = String(t || "").toUpperCase();
  return TIER_BONUS[k] ?? 0;
}

function starsScore(inputStars10, saleStars10) {
  const inS = Math.max(0, Math.min(10, Number(inputStars10) || 0));
  const saS = Math.max(0, Math.min(10, Number(saleStars10) || 0));
  if (inS <= 0) return null;

  const diff = Math.abs(saS - inS);
  if (diff === 0) return { tier: "AAA", add: 8, label: `Stars ${inS} → ${saS}` };
  if (diff === 1) return { tier: "PARTIAL", add: 1.2, label: `Stars ${inS} → ${saS}` };
  return null; // diff>=2 should never be scored (strict already dropped)
}

function scoreAfterStrict({ userEnchantsMap, inputStars10, sig, filters }) {
  const vf = applyVerifiedFiltersOrNull(sig, filters);
  if (!vf.ok) return null;

  const matched = [];
  let score = 0;

  // Slight penalty if unverifiable (should be rare now, but kept)
  score += vf.unverifiable ? -2 : 2;

  // Stars scoring
  const st = starsScore(inputStars10, sigStars10(sig));
  if (st) {
    score += st.add;
    matched.push({ enchant: { tier: st.tier, label: st.label }, add: st.add });
  }

  const saleEnchants = sigEnchantMap(sig);

  for (const [nameKey, inputLvlRaw] of userEnchantsMap.entries()) {
    const inL = Number(inputLvlRaw);
    if (!Number.isFinite(inL) || inL <= 0) continue;

    const saleLvl = Number(saleEnchants.get(nameKey) || 0);
    if (!saleLvl) continue; // strict already ensures if requested it exists; safety.

    const diff = Math.abs(saleLvl - inL);
    if (diff >= 2) continue; // safety; strict should have dropped

    const inTier = tierFor(nameKey, inL);
    if (!inTier || inTier === "MISC") continue;

    let tierLabel, add;

    if (diff === 0) {
      tierLabel = inTier;                     // gold uses real tier (AAA/AA/A/B/BB)
      add = tierBonusForTier(inTier) + 1.2;
    } else {
      tierLabel = "PARTIAL";                  // purple
      add = tierBonusForTier("PARTIAL");
    }

    // bounded bonus for higher requested levels (kept mild)
    add *= 1 + Math.min(10, Math.max(0, inL - 1)) * 0.08;

    score += add;
    matched.push({ enchant: { tier: tierLabel, label: displayEnchant(nameKey, inL) }, add });
  }

  matched.sort((a, b) => (b.add ?? 0) - (a.add ?? 0));
  return { score, matched, saleEnchants, unverifiable: vf.unverifiable };
}

/* =========================
   /api/recommend
========================= */
app.get("/api/recommend", async (req, res) => {
  try {
    const now = Date.now();

    const itemInput = String(req.query.item || "");
    const itemKey = canonicalItemKey(itemInput);
    if (!itemKey) {
      return res.json({
        recommended: null,
        top3: [],
        count: 0,
        note: "Pick an item from suggestions.",
        live: null,
      });
    }

    const inputStars10 = Math.max(0, Math.min(10, Number(req.query.stars10 ?? req.query.stars ?? 0)));
    const userRarity = normUserKey(req.query.rarity || "");
    const userWI = String(req.query.wi ?? "") === "1" || String(req.query.wi ?? "") === "true";

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
      if (!sig) continue; // sales should have sig; if not, skip for correctness

      const q = strictMatchQuality({ userEnchantsMap, inputStars10, sig, filters });
      if (q === "NONE") continue;

      if (q === "PERFECT") perfectPrices.push(price);
      else partialPrices.push(price);

      const sc = scoreAfterStrict({ userEnchantsMap, inputStars10, sig, filters });
      if (!sc) continue;

      candidates.push({
        uuid: r.uuid,
        item_name: stripStarGlyphs(r.item_name),
        final_price: price,
        ended_ts: Number(r.ended_ts) || 0,
        signature: sig,

        dstars: sigDungeonStars(sig),
        mstars: sigMasterStars(sig),
        stars10: sigStars10(sig),

        wi: sigWI(sig),
        dye: sigDye(sig),
        skin: sigSkin(sig),
        petskin: sigPetSkin(sig),
        petLevel: sigPetLevel(sig),
        petItem: sigPetItem(sig),

        score: sc.score,
        matched: sc.matched,
        allEnchants: Array.from(sc.saleEnchants.entries()).map(([k, v]) => ({
          tier: tierFor(k, v),
          label: displayEnchant(k, v),
        })),
        unverifiable: sc.unverifiable,
        quality: q,
      });
    }

    candidates.sort((a, b) => (b.score - a.score) || (a.final_price - b.final_price));
    const top3 = candidates.slice(0, 3);

    const pricePool = perfectPrices.length ? perfectPrices : partialPrices;
    const med = pricePool.length ? median(pricePool) : null;
    const rangeLow = pricePool.length ? Math.min(...pricePool) : null;
    const rangeHigh = pricePool.length ? Math.max(...pricePool) : null;

    /* =========================
       LIVE BIN (LBIN)
    ========================= */
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
      LIMIT 3000
      `,
      [itemKey, now - 10 * 60 * 1000]
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
      if (!sig) continue;

      const q = strictMatchQuality({ userEnchantsMap, inputStars10, sig, filters });
      if (q === "NONE") continue;

      const sc = scoreAfterStrict({ userEnchantsMap, inputStars10, sig, filters });
      if (!sc) continue;

      const cand = {
        uuid: a.uuid,
        item_name: stripStarGlyphs(a.item_name),
        price,
        signature: sig,
        dstars: sigDungeonStars(sig),
        mstars: sigMasterStars(sig),
        stars10: sigStars10(sig),
        petItem: sigPetItem(sig),
        score: sc.score,
        matched: sc.matched,
        quality: q,
      };

      if (q === "PERFECT") {
        if (!bestPerfect || cand.price < bestPerfect.price) bestPerfect = cand;
      } else {
        if (!bestPartial || cand.price < bestPartial.price) bestPartial = cand;
      }
    }

    const liveBest = bestPerfect || bestPartial || null;

    const note = candidates.length
      ? null
      : "No sales found that match (diff>=2 is excluded) within the selected history window.";

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
    return res.status(500).json({ error: err.message });
  }
});

/* =========================
   /api/items (DEDUPED + match anywhere)
========================= */
app.get("/api/items", async (req, res) => {
  const qRaw = String(req.query.q || "").trim();
  const qNorm = normKey(qRaw);
  if (!qNorm) return res.json({ items: [] });

  const LIMIT = Math.max(10, Math.min(80, Number(req.query.limit || 40)));
  const since = Date.now() - 120 * 24 * 60 * 60 * 1000;

  const tokens = qNorm.split(" ").filter(Boolean).slice(0, 6);
  if (!tokens.length) return res.json({ items: [] });

  const whereParts = [];
  const params = [since];
  let idx = 2;

  for (const t of tokens) {
    whereParts.push(`(item_key ILIKE $${idx} OR item_name ILIKE $${idx})`);
    params.push(`%${t}%`);
    idx++;
  }

  try {
    const { rows } = await pool.query(
      `
      SELECT item_key, item_name, MAX(ended_ts) AS newest, COUNT(*)::int AS cnt
      FROM sales
      WHERE ended_ts >= $1
        AND ${whereParts.join(" AND ")}
      GROUP BY item_key, item_name
      ORDER BY cnt DESC, newest DESC
      LIMIT ${LIMIT}
      `,
      params
    );

    const seen = new Set();
    const out = [];
    for (const r of rows) {
      const raw = String(r.item_key || r.item_name || "");
      const cKey = canonicalItemKey(raw);
      if (!cKey) continue;
      if (seen.has(cKey)) continue;
      seen.add(cKey);
      out.push({ key: cKey, label: canonicalItemDisplay(cKey) });
      if (out.length >= LIMIT) break;
    }

    return res.json({ items: out });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});

/* =========================
   Enchant autocomplete
========================= */
const ENCHANT_CATALOG = getEnchantCatalog();
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

