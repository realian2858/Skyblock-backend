import fs from "fs";
import pg from "pg";
import dotenv from "dotenv";
dotenv.config();

const sql = fs.readFileSync("./schema.sql", "utf8");
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

await pool.query(sql);
console.log("✅ Tables created/verified.");

await pool.end();
