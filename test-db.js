import pg from "pg";
import dotenv from "dotenv";

dotenv.config();

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL
});

const res = await pool.query("SELECT NOW() AS now");
console.log("✅ Connected to Postgres! Time:", res.rows[0].now);

await pool.end();
