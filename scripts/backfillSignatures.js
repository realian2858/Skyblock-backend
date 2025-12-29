// scripts/backfillSignatures.js
import dotenv from "dotenv";
import pg from "pg";
import { buildSignature } from "../parseLore.js";

dotenv.config();
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

async function backfillSales(limit = 20000) {
  const client = await pool.connect();
  try {
    const { rows } = await client.query(
      `
      SELECT uuid, item_name, tier, item_lore, item_bytes
      FROM sales
      WHERE signature IS NULL OR signature = ''
      ORDER BY ended_ts DESC
      LIMIT $1
      `,
      [limit]
    );

    console.log(`sales missing sig: ${rows.length}`);
    if (!rows.length) return;

    let updated = 0;
    for (const r of rows) {
      const sig = await buildSignature({
        itemName: r.item_name || "",
        lore: r.item_lore || "",
        tier: r.tier || "",
        itemBytes: r.item_bytes || "",
      });

      if (!sig) continue;

      await client.query(`UPDATE sales SET signature = $2 WHERE uuid = $1`, [r.uuid, sig]);
      updated++;
      if (updated % 500 === 0) console.log(`updated ${updated}...`);
    }

    console.log(`✅ sales signatures updated: ${updated}`);
  } finally {
    client.release();
  }
}

async function backfillAuctions(limit = 30000) {
  const client = await pool.connect();
  try {
    const { rows } = await client.query(
      `
      SELECT uuid, item_name, tier, item_lore, item_bytes
      FROM auctions
      WHERE is_ended = false
        AND bin = true
        AND (signature IS NULL OR signature = '')
      ORDER BY last_seen_ts DESC
      LIMIT $1
      `,
      [limit]
    );

    console.log(`live auctions missing sig: ${rows.length}`);
    if (!rows.length) return;

    let updated = 0;
    for (const r of rows) {
      const sig = await buildSignature({
        itemName: r.item_name || "",
        lore: r.item_lore || "",
        tier: r.tier || "",
        itemBytes: r.item_bytes || "",
      });

      if (!sig) continue;

      await client.query(`UPDATE auctions SET signature = $2 WHERE uuid = $1`, [r.uuid, sig]);
      updated++;
      if (updated % 500 === 0) console.log(`updated ${updated}...`);
    }

    console.log(`✅ auctions signatures updated: ${updated}`);
  } finally {
    client.release();
  }
}

(async () => {
  await backfillSales(50000);
  await backfillAuctions(50000);
  await pool.end();
})();
