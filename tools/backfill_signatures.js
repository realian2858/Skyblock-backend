// tools/backfill_signatures.js
import dotenv from "dotenv";
import pg from "pg";
import { buildSignature, canonicalItemKey } from "../parseLore.js";

dotenv.config();

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

async function backfill({ limit = 20000 } = {}) {
  const client = await pool.connect();
  try {
    // Only rows that can be recomputed (needs bytes OR lore)
    const { rows } = await client.query(
      `
      SELECT uuid, item_name, tier, item_lore, item_bytes
      FROM sales
      WHERE (signature IS NULL OR signature = '')
        AND (item_bytes IS NOT NULL OR item_lore IS NOT NULL)
      ORDER BY ended_ts DESC
      LIMIT $1
      `,
      [limit]
    );

    console.log(`Found ${rows.length} sales missing signatures`);

    let updated = 0;

    for (const r of rows) {
      const sig = await buildSignature({
        itemName: r.item_name || "",
        lore: r.item_lore || "",
        tier: r.tier || "",
        itemBytes: r.item_bytes || "",
      });

      if (!sig) continue;

      await client.query(
        `UPDATE sales SET signature = $2 WHERE uuid = $1`,
        [r.uuid, sig]
      );

      updated++;
      if (updated % 500 === 0) console.log(`Updated ${updated}/${rows.length}`);
    }

    console.log(`✅ Backfilled signatures: ${updated}`);
  } finally {
    client.release();
    await pool.end();
  }
}

backfill({ limit: Number(process.argv[2] || 20000) }).catch((e) => {
  console.error(e);
  process.exit(1);
});
