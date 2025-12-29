// debug-signature.mjs
import pg from "pg";
import dotenv from "dotenv";
import { buildSignature } from "./parseLore.js";

dotenv.config();

const UUID = process.argv[2] || "fe387ed28bd342409ea8b2241b437f2a";

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
});

function countStarGlyphs(s) {
  const txt = String(s || "");
  return {
    "✪": (txt.match(/✪/g) || []).length,
    "⭐": (txt.match(/⭐/g) || []).length,
    "★": (txt.match(/★/g) || []).length,
    "✯": (txt.match(/✯/g) || []).length,
    "☆": (txt.match(/☆/g) || []).length,
  };
}

try {
  const { rows } = await pool.query(
    "SELECT uuid, item_name, item_lore, item_bytes FROM sales WHERE uuid=$1",
    [UUID]
  );

  if (!rows.length) {
    console.log("No row found for uuid:", UUID);
    process.exit(0);
  }

  const r = rows[0];

  console.log("UUID:", r.uuid);
  console.log("Item:", r.item_name);
  console.log("Lore length:", (r.item_lore || "").length);
  console.log("Bytes length:", (r.item_bytes || "").length);

  // Quick check: do we see star glyphs in lore text?
  console.log("Star glyph counts in lore:", countStarGlyphs(r.item_lore));

  const sig = await buildSignature({
    itemName: r.item_name || "",
    lore: r.item_lore || "",
    tier: "", // you don't have a tier column; keep blank
    itemBytes: r.item_bytes || "",
  });

  console.log("\nSIGNATURE:");
  console.log(sig);
} catch (err) {
  console.error("Debug script failed:", err?.message || err);
  process.exit(1);
} finally {
  await pool.end().catch(() => {});
}
