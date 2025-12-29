import "dotenv/config";
import pg from "pg";
import { canonicalItemKey } from "../parseLore.js";

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

async function run() {
  const client = await pool.connect();
  try {
    console.log("Reading rows...");
    const { rows } = await client.query(`
      SELECT uuid, item_name
      FROM sales
      WHERE item_name IS NOT NULL
    `);

    console.log("Updating rows:", rows.length);
    await client.query("BEGIN");

    let n = 0;
    for (const r of rows) {
      const k = canonicalItemKey(r.item_name || "");
      if (!k) continue;
      await client.query(`UPDATE sales SET item_key=$2 WHERE uuid=$1`, [r.uuid, k]);
      n++;
      if (n % 20000 === 0) console.log("updated", n);
    }

    await client.query("COMMIT");
    console.log("Done. Updated:", n);
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    throw e;
  } finally {
    client.release();
    await pool.end();
  }
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
