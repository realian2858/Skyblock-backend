// ingest.js (v3 - LBIN-accurate sync) + STAR FIX PATCH
// Goals:
// ✅ Always fetch ALL pages every cycle (full snapshot)
// ✅ Upsert live auctions fast + safely
// ✅ Build signature for BIN auctions (and any auction missing sig) so petskins/dyes work
// ✅ Mark missing-from-snapshot auctions as ended (this is critical for LBIN accuracy)
// ✅ Finalize ended -> sales in batches
// ✅ No per-sync “rebuildSalesItemKeys” (that was killing speed + LBIN freshness)
//
// ✅ PATCH (this file):
// - Force-clean star/master-star signature tokens (dstars/mstars) using glyph-first extraction
// - Overwrite signature when we rebuild it (prevents “bad stars frozen forever”)
// - Clean item_key to remove "Shiny" + reforges for autocomplete stability

import dotenv from "dotenv";
import pg from "pg";
import { canonicalItemKey, buildSignature } from "./parseLore.js";

dotenv.config();

const API = "https://api.hypixel.net/skyblock/auctions";
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

const HYPIXEL_KEY = requireEnv("HYPIXEL_API_KEY");

// tune these if you want
const PAGE_DELAY_MS = 90;          // small delay so you don't hammer API
const PER_SYNC_GRACE_MS = 60_000;  // auctions not seen in this snapshot are ended after grace (1 min)
const FINALIZE_BATCH = 5000;       // ended->sales batch size
const FINALIZE_MAX_LOOPS = 60;     // 60 * 5k = 300k max per sync (won't hit normally)

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchJson(url, tries = 4) {
  let lastErr = null;
  for (let i = 0; i < tries; i++) {
    try {
      const res = await fetch(url, {
        headers: { accept: "application/json" },
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      return data;
    } catch (e) {
      lastErr = e;
      await sleep(250 + i * 350);
    }
  }
  throw lastErr || new Error("fetch failed");
}

async function fetchPage(page) {
  const url = `${API}?page=${page}&key=${encodeURIComponent(HYPIXEL_KEY)}`;
  const data = await fetchJson(url, 4);
  if (!data?.success) throw new Error(`Hypixel API error on page ${page}`);
  return data;
}

function nonEmptyText(x) {
  const s = (x ?? "").toString();
  return s && s.trim() ? s.trim() : null;
}

// ✅ PATCH: strip Minecraft color codes + whitespace
function stripColor(s) {
  return (s || "").replace(/§[0-9A-FK-OR]/gi, "").trim();
}

// ✅ PATCH: Clean item name for canonical key (removes reforges/prefix spam)
function cleanNameForKey(itemName) {
  const raw = stripColor(itemName);

  // Remove star/master glyph spam from title
  let s = raw
    .replace(/[✪⭐★☆]/g, " ")
    .replace(/[➊➋➌➍➎]/g, " ")
    .replace(/[⓵⓶⓷⓸⓹]/g, " ")
    .replace(/[●○◉◌◍◎]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  // Remove common “prefix stacks” (important for Shiny + reforges)
  // We remove up to 3 leading tokens if they look like known prefixes.
  const knownPrefix = new Set([
    // ✅ include shiny
    "shiny",

    // common reforges (weapon/armor/general)
    "ancient","fabled","withered","spiritual","hasty","precise","rapid","spicy","heroic",
    "odd","fast","fair","epic","sharp","gentle","legendary","clean","fierce","heavy",
    "light","mythic","pure","smart","titanic","wise","necrotic","loving","giant",
    "perfect","renowned","jaded","submerged","bizarre","itchy","ominous","pleasant",
    "shaded","silky","bloody","forceful","hurtful","strong","superior","unreal",
    "deadly","fine","grand","neat","rapid","rich","salty","treacherous","stiff",
    "dirty","suspicious"
  ]);

  const parts = s.split(" ");
  let cut = 0;
  for (let i = 0; i < Math.min(3, parts.length); i++) {
    const w = (parts[i] || "").toLowerCase();
    if (knownPrefix.has(w)) cut++;
    else break;
  }
  if (cut > 0) s = parts.slice(cut).join(" ").trim();

  return s;
}

// ✅ PATCH: derive stars from visible glyphs in item name
function deriveStarsFromName(itemName) {
  const s = stripColor(itemName);

  // count normal stars
  const starCount = (s.match(/[✪⭐★]/g) || []).length;

  // master stars as digits; ONLY treat ➊➋➌➍➎ as master (ignore circles!)
  const mDigitCount = (s.match(/[➊➋➌➍➎]/g) || []).length;

  // Sometimes names contain 10 "✪" total; if >5, treat extras as master
  let total = starCount;
  if (starCount > 5 && mDigitCount === 0) total = Math.min(10, starCount);

  // if digits exist, total = min(10, normalStars + masterDigits)
  if (mDigitCount > 0) total = Math.min(10, Math.min(5, starCount) + mDigitCount);

  const dstars = Math.min(5, total);
  const mstars = Math.max(0, total - 5);

  return { total, dstars, mstars };
}

// ✅ PATCH: force signature stars tokens to match derived stars
function forceStarsInSignature(sig, { dstars, mstars }) {
  if (!sig || typeof sig !== "string") return sig;

  let out = sig;

  // remove any existing star tokens (we’ll replace cleanly)
  out = out
    .replace(/\bdstars:\d+\b/g, "")
    .replace(/\bmstars:\d+\b/g, "")
    .replace(/\bstars:\d+\b/g, ""); // if any older format exists

  // normalize separators
  out = out.replace(/\|{2,}/g, "|").replace(/^\|+|\|+$/g, "");

  // append clean tokens
  const parts = out.length ? out.split("|") : [];
  parts.push(`dstars:${Number(dstars) || 0}`);
  parts.push(`mstars:${Number(mstars) || 0}`);

  return parts.filter(Boolean).join("|");
}

/**
 * Build signature BUT NEVER crash sync.
 */
async function safeBuildSignature({ itemName, lore, tier, itemBytes }) {
  try {
    const sig = await buildSignature({
      itemName: itemName || "",
      lore: lore || "",
      tier: tier || "",
      itemBytes: itemBytes || "",
    });
    return typeof sig === "string" && sig.length ? sig : null;
  } catch (e) {
    console.error("⚠️ buildSignature failed:", e?.message || e);
    return null;
  }
}

/**
 * Upsert auctions in a single INSERT .. ON CONFLICT using VALUES bulk.
 * Much faster than 1 query per auction.
 */
async function upsertAuctionsBulk(list, now) {
  if (!Array.isArray(list) || list.length === 0) return 0;

  const rows = [];

  for (const a of list) {
    const uuid = String(a?.uuid || "").trim();
    if (!uuid) continue;

    const itemName = a.item_name || "";

    // ✅ PATCH: clean for item_key so autocomplete stays clean
    const cleanedForKey = cleanNameForKey(itemName);
    const itemKey = canonicalItemKey(cleanedForKey) || null;

    const bin = !!a.bin;
    const start_ts = Number(a.start || 0);
    const end_ts = Number(a.end || 0);
    const starting_bid = Number(a.starting_bid || 0);
    const highest_bid = Number(a.highest_bid || 0);
    const tier = a.tier || null;

    const lore = nonEmptyText(a.item_lore);
    const bytes = nonEmptyText(a.item_bytes);

    // ✅ PATCH: derive stars from NAME glyphs (stable)
    const derivedStars = deriveStarsFromName(itemName);

    // Build signature more aggressively (BIN, OR lore/bytes available)
    let sig = null;
    if (bin || lore || bytes) {
      sig = await safeBuildSignature({
        itemName,
        lore: lore || "",
        tier: tier || "",
        itemBytes: bytes || "",
      });

      // ✅ PATCH: force dstars/mstars into signature
      if (sig) sig = forceStarsInSignature(sig, derivedStars);
    }

    rows.push({
      uuid,
      itemName,
      itemKey,
      bin,
      start_ts,
      end_ts,
      starting_bid,
      highest_bid,
      tier,
      lore,
      bytes,
      now,
      sig,
    });
  }

  if (!rows.length) return 0;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const values = [];
    const placeholders = [];
    let idx = 1;

    for (const r of rows) {
      placeholders.push(
        `($${idx++},$${idx++},$${idx++},$${idx++},$${idx++},$${idx++},$${idx++},$${idx++},$${idx++},$${idx++},$${idx++},$${idx++},$${idx++},false)`
      );
      values.push(
        r.uuid,
        r.itemName,
        r.itemKey,
        r.bin,
        r.start_ts,
        r.end_ts,
        r.starting_bid,
        r.highest_bid,
        r.tier,
        r.lore,
        r.bytes,
        r.now,
        r.sig
      );
    }

    const sql = `
      INSERT INTO auctions
        (uuid, item_name, item_key, bin, start_ts, end_ts,
         starting_bid, highest_bid, tier, item_lore, item_bytes,
         last_seen_ts, signature, is_ended)
      VALUES
        ${placeholders.join(",")}
      ON CONFLICT (uuid) DO UPDATE SET
        item_name    = EXCLUDED.item_name,
        item_key     = EXCLUDED.item_key,
        bin          = EXCLUDED.bin,
        start_ts     = EXCLUDED.start_ts,
        end_ts       = EXCLUDED.end_ts,
        starting_bid = EXCLUDED.starting_bid,
        highest_bid  = EXCLUDED.highest_bid,
        tier         = EXCLUDED.tier,
        item_lore    = COALESCE(EXCLUDED.item_lore, auctions.item_lore),
        item_bytes   = COALESCE(EXCLUDED.item_bytes, auctions.item_bytes),
        last_seen_ts = EXCLUDED.last_seen_ts,
        is_ended     = false,

        -- ✅ PATCH: Overwrite signature when we rebuilt it (prevents bad stars freezing forever)
        signature = CASE
          WHEN EXCLUDED.signature IS NULL OR EXCLUDED.signature = '' THEN auctions.signature
          WHEN auctions.signature IS NULL OR auctions.signature = '' THEN EXCLUDED.signature
          -- if excluded has pet_item and existing doesn't, take excluded
          WHEN auctions.signature NOT LIKE '%pet_item:%' AND EXCLUDED.signature LIKE '%pet_item:%' THEN EXCLUDED.signature
          -- otherwise, take excluded (we want star tokens corrected)
          ELSE EXCLUDED.signature
        END
    `;

    await client.query(sql, values);
    await client.query("COMMIT");
    return rows.length;
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
    client.release();
  }
}

async function markNotSeenInSnapshotAsEnded(now) {
  await pool.query(
    `
    UPDATE auctions
    SET is_ended = true
    WHERE is_ended = false
      AND last_seen_ts < $1
    `,
    [now - PER_SYNC_GRACE_MS]
  );
}

async function finalizeEnded(now) {
  const { rows } = await pool.query(
    `
    SELECT a.uuid, a.item_name, a.item_key, a.bin, a.end_ts, a.starting_bid, a.highest_bid,
           a.tier, a.item_lore, a.item_bytes, a.signature
    FROM auctions a
    WHERE a.end_ts > 0
      AND a.end_ts <= $1
      AND (
        a.is_ended = false
        OR (
          a.is_ended = true
          AND NOT EXISTS (SELECT 1 FROM sales s WHERE s.uuid = a.uuid)
        )
      )
    ORDER BY a.end_ts ASC
    LIMIT $2
    `,
    [now, FINALIZE_BATCH]
  );

  if (!rows.length) return 0;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const upsertSaleSql = `
      INSERT INTO sales
        (uuid, item_name, item_key, bin, final_price, ended_ts, tier, signature, item_lore, item_bytes)
      VALUES
        ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      ON CONFLICT (uuid) DO UPDATE SET
        item_name   = EXCLUDED.item_name,
        item_key    = EXCLUDED.item_key,
        bin         = EXCLUDED.bin,
        final_price = EXCLUDED.final_price,
        ended_ts    = EXCLUDED.ended_ts,
        tier        = EXCLUDED.tier,
        signature = CASE
          WHEN sales.signature IS NULL OR sales.signature = '' THEN EXCLUDED.signature
          WHEN sales.signature NOT LIKE '%pet_item:%' AND EXCLUDED.signature LIKE '%pet_item:%' THEN EXCLUDED.signature
          ELSE EXCLUDED.signature
        END
    `;

    const markEndedSql = `UPDATE auctions SET is_ended = true WHERE uuid=$1`;

    let moved = 0;

    for (const r of rows) {
      const price = Number(r.bin ? r.starting_bid : r.highest_bid) || 0;

      // ✅ PATCH: ensure item_key is clean even for old rows
      const k = r.item_key || canonicalItemKey(cleanNameForKey(r.item_name || "")) || null;

      // rebuild signature if missing, but ALSO keep stars tokens clean
      let sig =
        r.signature ||
        (await safeBuildSignature({
          itemName: r.item_name || "",
          lore: (r.item_lore || "").toString(),
          tier: r.tier || "",
          itemBytes: (r.item_bytes || "").toString(),
        }));

      if (sig) {
        const derivedStars = deriveStarsFromName(r.item_name || "");
        sig = forceStarsInSignature(sig, derivedStars);
      }

      await client.query(upsertSaleSql, [
        r.uuid,
        r.item_name || "",
        k,
        !!r.bin,
        price,
        Number(r.end_ts || 0),
        r.tier || null,
        sig,
        r.item_lore || null,
        r.item_bytes || null,
      ]);

      await client.query(markEndedSql, [r.uuid]);
      moved++;
    }

    await client.query("COMMIT");
    return moved;
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
    client.release();
  }
}

async function backfillSalesItemKeys({ batch = 20000 } = {}) {
  const client = await pool.connect();
  try {
    const { rows } = await client.query(
      `
      SELECT uuid, item_name
      FROM sales
      WHERE item_key IS NULL OR item_key = ''
      ORDER BY ended_ts DESC
      LIMIT $1
      `,
      [batch]
    );
    if (!rows.length) return 0;

    await client.query("BEGIN");
    const upd = `UPDATE sales SET item_key = $2 WHERE uuid = $1`;
    let updated = 0;

    for (const r of rows) {
      const k = canonicalItemKey(cleanNameForKey(r.item_name || ""));
      if (!k) continue;
      await client.query(upd, [r.uuid, k]);
      updated++;
    }
    await client.query("COMMIT");
    return updated;
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
    client.release();
  }
}

export async function syncOnce() {
  const now = Date.now();

  const first = await fetchPage(0);
  const totalPages = Math.max(1, Number(first.totalPages || 1));
  console.log(`📦 Sync: ${totalPages} pages`);

  let upserted = 0;

  upserted += await upsertAuctionsBulk(first.auctions || [], now);

  for (let p = 1; p < totalPages; p++) {
    const data = await fetchPage(p);
    upserted += await upsertAuctionsBulk(data.auctions || [], now);
    if (PAGE_DELAY_MS > 0) await sleep(PAGE_DELAY_MS);
  }

  await markNotSeenInSnapshotAsEnded(now);

  let totalFinalized = 0;
  for (let i = 0; i < FINALIZE_MAX_LOOPS; i++) {
    const n = await finalizeEnded(now);
    totalFinalized += n;
    if (n === 0) break;
    await sleep(30);
  }

  const filled = await backfillSalesItemKeys({ batch: 20000 });
  if (filled > 0) console.log(`🧩 Backfilled sales.item_key: ${filled}`);

  console.log(`✅ Upserted live: ${upserted} | Finalized sales: ${totalFinalized}`);
}

export async function rebuildAllSalesItemKeys(batch = 50000) {
  const client = await pool.connect();
  try {
    let offset = 0;
    while (true) {
      const { rows } = await client.query(
        `SELECT uuid, item_name FROM sales ORDER BY ended_ts DESC LIMIT $1 OFFSET $2`,
        [batch, offset]
      );
      if (!rows.length) break;

      await client.query("BEGIN");
      for (const r of rows) {
        const k = canonicalItemKey(cleanNameForKey(r.item_name || ""));
        await client.query(`UPDATE sales SET item_key=$2 WHERE uuid=$1`, [r.uuid, k]);
      }
      await client.query("COMMIT");

      offset += rows.length;
      console.log("rebuilt", offset);
    }
  } finally {
    client.release();
  }
}
