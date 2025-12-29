// tools/backfill_sales_from_auctions.js
import dotenv from "dotenv";
import pg from "pg";
import { canonicalItemKey, buildSignature } from "../parseLore.js";

dotenv.config();
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

async function run(batch = 50000) {
  const now = Date.now();
  const client = await pool.connect();
  try {
    const { rows } = await client.query(
      `
      SELECT uuid, item_name, item_key, bin, end_ts, starting_bid, highest_bid,
             tier, item_lore, item_bytes, signature
      FROM auctions a
      WHERE a.is_ended = true
        AND a.end_ts > 0
        AND a.end_ts <= $1
        AND NOT EXISTS (SELECT 1 FROM sales s WHERE s.uuid = a.uuid)
      ORDER BY a.end_ts ASC
      LIMIT $2
      `,
      [now, batch]
    );

    console.log(`Found ${rows.length} auctions to move into sales`);

    let moved = 0;
    await client.query("BEGIN");

    for (const r of rows) {
      const price = Number(r.bin ? r.starting_bid : r.highest_bid) || 0;
      if (price <= 0) continue;

      const itemKey = r.item_key || canonicalItemKey(r.item_name || "") || null;

      const sig =
        (r.signature && String(r.signature).trim()) ||
        (await buildSignature({
          itemName: r.item_name || "",
          lore: r.item_lore || "",
          tier: r.tier || "",
          itemBytes: r.item_bytes || "",
        }));

      await client.query(
        `
        INSERT INTO sales
          (uuid, item_name, item_key, bin, final_price, ended_ts, tier, signature, item_lore, item_bytes)
        VALUES
          ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT (uuid) DO NOTHING
        `,
        [
          r.uuid,
          r.item_name || "",
          itemKey,
          !!r.bin,
          price,
          Number(r.end_ts || 0),
          r.tier || null,
          sig || null,
          r.item_lore || null,
          r.item_bytes || null,
        ]
      );

      moved++;
      if (moved % 1000 === 0) console.log(`Moved ${moved}/${rows.length}`);
    }

    await client.query("COMMIT");
    console.log(`✅ Moved into sales: ${moved}`);
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
    client.release();
    await pool.end();
  }
}

run(Number(process.argv[2] || 50000)).catch((e) => {
  console.error(e);
  process.exit(1);
});
