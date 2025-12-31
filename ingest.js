// ingest.js (v7 - MATCHED PIPELINE: normalized name everywhere, WI fixed by lore fallback, stars10 stable)
//
// Cloud Run Job: RUN ONCE + EXIT

import dotenv from "dotenv";
import pg from "pg";
import { canonicalItemKey, buildSignature } from "./parseLore.js";

dotenv.config();

const API = "https://api.hypixel.net/skyblock/auctions";

function requireEnv(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) throw new Error(`Missing env var: ${name}`);
  return String(v).trim();
}

const DATABASE_URL = requireEnv("DATABASE_URL");
const HYPIXEL_KEY = requireEnv("HYPIXEL_API_KEY");

const PAGE_DELAY_MS = 90;
const PER_SYNC_GRACE_MS = 60_000;

const FINALIZE_BATCH = 5000;
const FINALIZE_MAX_LOOPS = 60;

const SALES_KEY_BACKFILL_BATCH = 20000;
const MAX_PAGES_HARD_CAP = 200;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const pool = new pg.Pool({
  connectionString: DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 15_000,
});

pool.on("error", (err) => {
  console.error("PG pool error:", err?.message || err);
});

async function fetchJson(url, tries = 4) {
  let lastErr = null;
  for (let i = 0; i < tries; i++) {
    try {
      const res = await fetch(url, { headers: { accept: "application/json" } });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
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

/* =========================
   Shared normalize helpers (MATCH server)
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
  const x = String(s || "").normalize("NFKD");
  let out = "";
  for (const ch of x) out += DIGIT_CHAR_MAP.get(ch) ?? ch;
  return out;
}

function stripStarGlyphs(s) {
  return String(s || "")
    .replace(/[✪★☆✯✰●⬤•○◉◎◍]+/g, "")
    .replace(/\s*(?:[➊➋➌➍➎➏➐➑➒➓❶❷❸❹❺❻❼❽❾❿⓵⓶⓷⓸⓹⓺⓻⓼⓽⓾①②③④⑤⑥⑦⑧⑨⓪0-9])\s*$/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

// normalize itemName for signature parsing (circle-stars -> ✪, weird digits -> ascii)
function normalizeNameForSignature(itemName) {
  let s = String(itemName || "");
  s = normalizeWeirdDigits(s);
  s = s.replace(/[●⬤•○◉◎◍]/g, "✪");
  s = s.replace(/[★☆✯✰]/g, "✪");
  return s;
}

function itemNameLooksStarred(itemName) {
  const s = String(itemName ?? "");
  if (!s) return false;
  if (/[✪★☆✯✰●⬤•○◉◎◍]/.test(s)) return true;
  if (/[➊➋➌➍➎➏➐➑➒➓⓪①②③④⑤⑥⑦⑧⑨❶❷❸❹❺❻❼❽❾❿⓵⓶⓷⓸⓹⓺⓻⓼⓽⓾⁰¹²³⁴⁵⁶⁷⁸⁹₀₁₂₃₄₅₆₇₈₉]/.test(s)) return true;
  return false;
}

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

/* =========================
   Bulk upsert auctions
========================= */
async function upsertAuctionsBulk(list, now) {
  if (!Array.isArray(list) || list.length === 0) return 0;

  const rows = [];

  for (const a of list) {
    const uuid = String(a?.uuid || "").trim();
    if (!uuid) continue;

    const itemName = a.item_name || "";
    const itemKey = canonicalItemKey(stripStarGlyphs(itemName)) || null;

    const bin = !!a.bin;
    const start_ts = Number(a.start || 0);
    const end_ts = Number(a.end || 0);
    const starting_bid = Number(a.starting_bid || 0);
    const highest_bid = Number(a.highest_bid || 0);
    const tier = a.tier || null;

    const lore = nonEmptyText(a.item_lore);
    const bytes = nonEmptyText(a.item_bytes);

    const shouldBuildSig = bin || !!lore || !!bytes || itemNameLooksStarred(itemName);

    let sig = null;
    if (shouldBuildSig) {
      sig = await safeBuildSignature({
        itemName: normalizeNameForSignature(itemName),
        lore: lore || "",
        tier: tier || "",
        itemBytes: bytes || "",
      });
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
        signature = CASE
          WHEN EXCLUDED.signature IS NULL OR EXCLUDED.signature = '' THEN auctions.signature
          WHEN auctions.signature IS NULL OR auctions.signature = '' THEN EXCLUDED.signature
          WHEN auctions.signature NOT LIKE '%pet_item:%' AND EXCLUDED.signature LIKE '%pet_item:%' THEN EXCLUDED.signature
          WHEN (
            (regexp_match(auctions.signature, 'stars10:(\\d+)'))[1] IS NOT NULL
            AND (regexp_match(EXCLUDED.signature, 'stars10:(\\d+)'))[1] IS NOT NULL
            AND (regexp_match(auctions.signature, 'stars10:(\\d+)'))[1] <> (regexp_match(EXCLUDED.signature, 'stars10:(\\d+)'))[1]
          ) THEN EXCLUDED.signature
          WHEN (
            (regexp_match(auctions.signature, 'stars10:(\\d+)'))[1] IS NULL
            AND (regexp_match(EXCLUDED.signature, 'stars10:(\\d+)'))[1] IS NOT NULL
          ) THEN EXCLUDED.signature
          ELSE auctions.signature
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
        item_lore   = COALESCE(EXCLUDED.item_lore, sales.item_lore),
        item_bytes  = COALESCE(EXCLUDED.item_bytes, sales.item_bytes),
        signature = CASE
          WHEN EXCLUDED.signature IS NULL OR EXCLUDED.signature = '' THEN sales.signature
          WHEN sales.signature IS NULL OR sales.signature = '' THEN EXCLUDED.signature
          WHEN sales.signature NOT LIKE '%pet_item:%' AND EXCLUDED.signature LIKE '%pet_item:%' THEN EXCLUDED.signature
          WHEN (
            (regexp_match(sales.signature, 'stars10:(\\d+)'))[1] IS NOT NULL
            AND (regexp_match(EXCLUDED.signature, 'stars10:(\\d+)'))[1] IS NOT NULL
            AND (regexp_match(sales.signature, 'stars10:(\\d+)'))[1] <> (regexp_match(EXCLUDED.signature, 'stars10:(\\d+)'))[1]
          ) THEN EXCLUDED.signature
          WHEN (
            (regexp_match(sales.signature, 'stars10:(\\d+)'))[1] IS NULL
            AND (regexp_match(EXCLUDED.signature, 'stars10:(\\d+)'))[1] IS NOT NULL
          ) THEN EXCLUDED.signature
          ELSE sales.signature
        END
    `;

    const markEndedSql = `UPDATE auctions SET is_ended = true WHERE uuid=$1`;

    let moved = 0;

    for (const r of rows) {
      const price = Number(r.bin ? r.starting_bid : r.highest_bid) || 0;

      const itemKey = r.item_key || canonicalItemKey(stripStarGlyphs(r.item_name || "")) || null;

      const shouldBuildSig =
        !!r.signature ||
        !!r.item_lore ||
        !!r.item_bytes ||
        itemNameLooksStarred(r.item_name || "");

      const sig =
        (r.signature && String(r.signature).trim()) ||
        (shouldBuildSig
          ? await safeBuildSignature({
              itemName: normalizeNameForSignature(r.item_name || ""),
              lore: (r.item_lore || "").toString(),
              tier: r.tier || "",
              itemBytes: (r.item_bytes || "").toString(),
            })
          : null);

      await client.query(upsertSaleSql, [
        r.uuid,
        r.item_name || "",
        itemKey,
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

async function backfillSalesItemKeys({ batch = SALES_KEY_BACKFILL_BATCH } = {}) {
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
      const k = canonicalItemKey(stripStarGlyphs(r.item_name || ""));
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

async function syncOnce() {
  const now = Date.now();

  const first = await fetchPage(0);
  let totalPages = Math.max(1, Number(first.totalPages || 1));
  totalPages = Math.min(totalPages, MAX_PAGES_HARD_CAP);

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

  const filled = await backfillSalesItemKeys({ batch: SALES_KEY_BACKFILL_BATCH });
  if (filled > 0) console.log(`🧩 Backfilled sales.item_key: ${filled}`);

  console.log(`✅ Upserted live: ${upserted} | Finalized sales: ${totalFinalized}`);
}

async function main() {
  console.log("🚀 INGEST BOOT", new Date().toISOString());
  console.log("ENV OK:", {
    hasDatabaseUrl: !!process.env.DATABASE_URL,
    hasHypixelKey: !!process.env.HYPIXEL_API_KEY,
  });

  await syncOnce();
  console.log("✅ INGEST DONE", new Date().toISOString());
}

main()
  .then(async () => {
    try { await pool.end(); } catch {}
    process.exit(0);
  })
  .catch(async (e) => {
    console.error("❌ INGEST FAILED", e?.stack || e);
    try { await pool.end(); } catch {}
    process.exit(1);
  });
