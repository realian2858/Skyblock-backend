import pg from "pg";
import dotenv from "dotenv";

dotenv.config();

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

const r = await pool.query("SELECT COUNT(*)::int AS c FROM sales");
console.log("sales rows:", r.rows[0].c);

await pool.end();
