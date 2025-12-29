// backfill_pet_item_signatures.js
// One-time backfill: rebuild signatures to include pet_item (and other tokens) for recent rows.
// Targets BOTH sales + auctions (in case you want live/history consistent).
//
// Usage:
//   node backfill_pet_item_signatures.js
//
// Env:
//   DATABASE_URL must be set (same as your app).
//
// Notes:
// - This is safe: it only updates rows where signature is NULL/empty OR missing "pet_item:".
// - It only touches rows that have lore/bytes available (needed to detect Held Item reliably).
// - Tune DAYS and BATCH to your DB size.

import dotenv from "dotenv";
import pg from "pg";
import { buildSignature, canonicalItemKey } from "./parseLore.js";

dotenv.config();

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

const DAYS = Number(process.env.BACKFILL_DAYS || 180);   // lookback window
const BATCH = Number(process.env.BACKFILL_BATCH || 1500);
const SLEEP_MS = Number(process.env.BACKFILL_SLEEP_MS || 30);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function needsBackfill(sig) {
  const s = String(sig || "").trim();
  if (!s) return true;
  return !s.includes("pet_item:");
}

async function safeBuild({ item_name, item_lore, tier, item_bytes }) {
  try {
    const sig = await buildSignature({
      itemName: item_name || "",
      lore: item_lore || "",
      tier: tier || "",
      itemBytes: item_bytes || "",
    });
    const out = String(sig || "").trim();
    return out || null;
  } catch (e) {
    console.error("⚠️ buildSignature failed:", e?.message || e);
    return null;
  }
}

async function backfillTable({
  table,
  timeCol,
  extraWhereSql = "",
  selectCols,
  updateSql,
  updateArgsBuilder,
}) {
  const since = Date.now() - DAYS * 24 * 60 * 60 * 1000;

  let totalScanned = 0;
  let totalUpdated = 0;

  while (true) {
    const { rows } = await pool.query(
      `
      SELECT ${selectCols.join(", ")}
      FROM ${table}
      WHERE ${timeCol} >= $1
        AND (signature IS NULL OR signature = '' OR signature NOT LIKE '%pet_item:%')
        AND (item_lore IS NOT NULL OR item_bytes IS NOT NULL)
        ${extraWhereSql}
      ORDER BY ${timeCol} DESC
      LIMIT $2
      `,
      [since, BATCH]
    );

    if (!rows.length) break;

    totalScanned += rows.length;

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      for (const r of rows) {
        if (!needsBackfill(r.signature)) continue;

        const sig = await safeBuild(r);
        if (!sig) continue;

        // Optional: item_key backfill too, since you have the column on auctions
        // (harmless, helps item autocomplete consistency)
        const item_key = canonicalItemKey(r.item_name || "") || null;

        const args = updateArgsBuilder({ r, sig, item_key });
        await client.query(updateSql, args);
        totalUpdated++;
      }

      await client.query("COMMIT");
    } catch (e) {
      try { await client.query("ROLLBACK"); } catch {}
      throw e;
    } finally {
      client.release();
    }

    console.log(`✅ ${table}: scanned ${totalScanned}, updated ${totalUpdated}`);
    if (SLEEP_MS > 0) await sleep(SLEEP_MS);
  }

  return { totalScanned, totalUpdated };
}

async function main() {
  if (!process.env.DATABASE_URL) {
    console.error("Missing DATABASE_URL");
    process.exit(1);
  }

  console.log(`🔧 Backfill starting (lookback ${DAYS} days, batch ${BATCH})...`);

  // 1) SALES (if your DB has it; your server uses it)
  // If your sales schema differs, adjust selectCols / updateSql accordingly.
  try {
    await backfillTable({
      table: "sales",
      timeCol: "ended_ts",
      selectCols: ["uuid", "item_name", "tier", "item_lore", "item_bytes", "signature"],
      updateSql: `
        UPDATE sales
        SET signature = $2
        WHERE uuid = $1
      `,
      updateArgsBuilder: ({ r, sig }) => [r.uuid, sig],
    });
  } catch (e) {
    console.warn("⚠️ Skipping sales (table missing or schema mismatch). Error:", e?.message || e);
  }

  // 2) AUCTIONS (matches the schema you posted)
  await backfillTable({
    table: "auctions",
    timeCol: "end_ts",
    selectCols: ["uuid", "item_name", "tier", "item_lore", "item_bytes", "signature"],
    updateSql: `
      UPDATE auctions
      SET signature = $2,
          item_key = COALESCE(item_key, $3)
      WHERE uuid = $1
    `,
    updateArgsBuilder: ({ r, sig, item_key }) => [r.uuid, sig, item_key],
  });

  console.log("🎉 Backfill complete.");
  await pool.end();
}

main().catch(async (e) => {
  console.error("❌ Backfill failed:", e);
  try { await pool.end(); } catch {}
  process.exit(1);
});
