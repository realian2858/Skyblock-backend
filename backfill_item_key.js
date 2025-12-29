import pg from "pg";
import dotenv from "dotenv";
import { canonicalItemKey } from "./parseLore.js";

dotenv.config();
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

async function main() {
  const client = await pool.connect();
  try {
    const BATCH = 5000;
    let total = 0;

    while (true) {
      const { rows } = await client.query(
        `SELECT uuid, item_name
         FROM sales
         WHERE item_key IS NULL OR item_key = ''
         LIMIT $1`,
        [BATCH]
      );
      if (!rows.length) break;

      await client.query("BEGIN");
      for (const r of rows) {
        const k = canonicalItemKey(r.item_name || "");
        if (!k) continue;
        await client.query(`UPDATE sales SET item_key=$1 WHERE uuid=$2`, [k, r.uuid]);
      }
      await client.query("COMMIT");

      total += rows.length;
      console.log("Backfilled batch:", rows.length, "total:", total);
    }

    console.log("Done. Total updated:", total);
  } catch (e) {
    try { await pool.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
