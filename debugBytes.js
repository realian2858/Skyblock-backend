// debugBytes.js
import dotenv from "dotenv";
import pg from "pg";
import { gunzipSync } from "node:zlib";
import { parse as parseNbt } from "prismarine-nbt";

dotenv.config();
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

function unwrap(node) {
  if (node == null) return node;
  if (typeof node !== "object") return node;
  if ("type" in node && "value" in node) return unwrap(node.value);
  if (Array.isArray(node)) return node.map(unwrap);
  const out = {};
  for (const [k, v] of Object.entries(node)) out[k] = unwrap(v);
  return out;
}

async function parseItemBytes(itemBytes) {
  const b64 = String(itemBytes || "").trim();
  if (!b64) return null;

  let raw;
  try {
    raw = Buffer.from(b64, "base64");
  } catch (e) {
    console.log("Base64 decode failed:", e.message);
    return null;
  }

  console.log("Base64->raw bytes:", raw.length);
  console.log("Raw first 16 bytes hex:", raw.subarray(0, 16).toString("hex"));

  // try gunzip
  let buf = raw;
  try {
    buf = gunzipSync(raw);
    console.log("Gunzip OK. Decompressed bytes:", buf.length);
    console.log("Decompressed first 16 bytes hex:", buf.subarray(0, 16).toString("hex"));
  } catch (e) {
    console.log("Gunzip failed:", e.message);
  }

  // NBT parse attempt
  const parsed = await new Promise((resolve) => {
    parseNbt(buf, (err, data) => {
      if (err) {
        console.log("NBT parse failed:", err.message);
        return resolve(null);
      }
      resolve(data);
    });
  });

  if (!parsed?.parsed) {
    console.log("NBT parsed but no .parsed root");
    return null;
  }

  return parsed.parsed;
}


function findAllExtraAttributes(rootParsed) {
  const root = unwrap(rootParsed);
  if (!root || typeof root !== "object") return [];

  const hits = [];
  const stack = [root];
  const seen = new Set();

  while (stack.length) {
    const cur = stack.pop();
    if (!cur || typeof cur !== "object") continue;
    if (seen.has(cur)) continue;
    seen.add(cur);

    if (cur.ExtraAttributes && typeof cur.ExtraAttributes === "object") hits.push(cur.ExtraAttributes);
    if (cur.tag?.ExtraAttributes && typeof cur.tag.ExtraAttributes === "object") hits.push(cur.tag.ExtraAttributes);

    for (const v of Object.values(cur)) if (typeof v === "object" && v) stack.push(v);
  }
  return hits;
}

function chooseBestExtraAttributes(list) {
  if (!Array.isArray(list) || !list.length) return null;
  let best = null, bestScore = -1;

  for (const ea of list) {
    if (!ea || typeof ea !== "object") continue;
    let score = Object.keys(ea).length;
    if (ea.enchantments && typeof ea.enchantments === "object") score += 200;
    if (ea.ultimate_enchant) score += 50;
    if (score > bestScore) { bestScore = score; best = ea; }
  }
  return best;
}

const UUID = "3b1b31efb53c4acdaff6192e2890037b";

(async () => {
  const { rows } = await pool.query(
    "SELECT uuid, item_bytes FROM sales WHERE uuid = $1",
    [UUID]
  );
  if (!rows.length) {
    console.log("No row for uuid");
    process.exit(0);
  }

  const root = await parseItemBytes(rows[0].item_bytes);
  const extras = findAllExtraAttributes(root);
  const extra = chooseBestExtraAttributes(extras);

  if (!extra) {
    console.log("No ExtraAttributes found");
    process.exit(0);
  }

  console.log("ExtraAttributes keys:", Object.keys(extra).sort());

  // Search for any scroll-like strings anywhere (stringified)
  const txt = JSON.stringify(extra);
  console.log("Has IMPLOSION_SCROLL?", txt.includes("IMPLOSION_SCROLL"));
  console.log("Has SHADOW_WARP_SCROLL?", txt.includes("SHADOW_WARP_SCROLL"));
  console.log("Has WITHER_SHIELD_SCROLL?", txt.includes("WITHER_SHIELD_SCROLL"));

  // Print likely fields if present
  for (const k of ["ability_scroll", "ability_scrolls", "scrolls", "wither_shield", "implosion", "shadow_warp"]) {
    if (k in extra) console.log(k, "=", extra[k]);
  }

  process.exit(0);
})();
