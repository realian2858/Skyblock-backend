// backfill-signatures.mjs (FULL REWRITE)
// Goal: rebuild signatures for old sales so they include NEW pet fields (petlvl + petskin)
// and stay consistent with your current parseLore/buildSignature system.
//
// What this does:
// - Scans sales in chunks from newest → oldest (stable pagination by ended_ts + uuid)
// - Recomputes signature using buildSignature() (which should now encode petlvl/petskin)
// - Updates only when signature actually changes
// - Adds smarter skipping to avoid rewriting rows that already have pet fields (optional)
// - Logs progress incl. stars + pet fields
//
// IMPORTANT:
// - This script assumes your UPDATED parseLore.js buildSignature() writes:
//   petlvl=<number or 0> | petskin=<key or none>
// - If your buildSignature uses different field names, update SIG_KEYS below.

import pg from "pg";
import dotenv from "dotenv";
import { buildSignature } from "./parseLore.js";

dotenv.config();

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

// Tune these:
const BATCH = 300;
const SLEEP_MS = 50;

// If you want to ONLY backfill rows missing pet fields, leave true.
// If you want to rebuild literally everything, set to false.
const ONLY_IF_MISSING_PET_FIELDS = true;

const SIG_KEYS = {
  dstars: "dstars",
  mstars: "mstars",
  petlvl: "petlvl",
  petskin: "petskin",
  item: "item",
};

function getSigField(sig, key) {
  const s = String(sig || "");
  const m = s.match(new RegExp(`(?:^|\\|)${key}=([^|]*)`));
  return m ? m[1] : "";
}

function hasSigField(sig, key) {
  const s = String(sig || "");
  return new RegExp(`(?:^|\\|)${key}=`).test(s);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function looksLikePetItemName(itemName) {
  // Pet auctions often show as: "Lvl 100 Ender Dragon" or "Level 200 Golden Dragon"
  // Your canonicalItemKey already strips this for matching, but for backfill routing,
  // we only need a fast check.
  const s = String(itemName || "").toLowerCase().trim();
  return s.startsWith("lvl ") || s.startsWith("lv ") || s.startsWith("level ");
}

async function main() {
  let scanned = 0;
  let updated = 0;
  let skipped = 0;

  // Pagination cursor (newest -> oldest)
  let lastEnded = Number.MAX_SAFE_INTEGER;
  let lastUuid = "ffffffffffffffffffffffffffffffff";

  while (true) {
    const { rows } = await pool.query(
      `
      SELECT uuid, item_name, item_lore, item_bytes, signature, ended_ts
      FROM sales
      WHERE (ended_ts < $1) OR (ended_ts = $1 AND uuid < $2)
      ORDER BY ended_ts DESC, uuid DESC
      LIMIT $3
      `,
      [lastEnded, lastUuid, BATCH]
    );

    if (!rows.length) break;

    const tail = rows[rows.length - 1];
    lastEnded = Number(tail.ended_ts || 0);
    lastUuid = String(tail.uuid || "");

    for (const r of rows) {
      scanned++;

      const uuid = String(r.uuid || "");
      const itemName = String(r.item_name || "");
      const lore = String(r.item_lore || "");
      const itemBytes = String(r.item_bytes || "");
      const oldSig = String(r.signature || "");

      // Optional optimization: only rebuild rows that likely need pet fields
      if (ONLY_IF_MISSING_PET_FIELDS) {
        const alreadyHasPetFields =
          hasSigField(oldSig, SIG_KEYS.petlvl) || hasSigField(oldSig, SIG_KEYS.petskin);

        // If the row isn't a pet item and already has pet fields (or it's irrelevant), skip.
        // If it IS a pet item but missing pet fields, do rebuild.
        if (alreadyHasPetFields && !looksLikePetItemName(itemName)) {
          skipped++;
          continue;
        }

        // If it's not a pet item AND old signature already contains dyes/skin/stars etc,
        // you could skip too—but we keep it conservative and only skip the case above.
      }

      const newSig = await buildSignature({
        itemName,
        lore,
        tier: "", // tier not needed for backfill; your buildSignature normalizes it anyway
        itemBytes,
      });

      if (newSig && newSig !== oldSig) {
        await pool.query(`UPDATE sales SET signature=$1 WHERE uuid=$2`, [newSig, uuid]);
        updated++;
      } else {
        skipped++;
      }

      // Progress logs (includes pet fields)
      if (scanned % 200 === 0) {
        const d = getSigField(newSig, SIG_KEYS.dstars);
        const m = getSigField(newSig, SIG_KEYS.mstars);

        const petLvl = getSigField(newSig, SIG_KEYS.petlvl);
        const petSkin = getSigField(newSig, SIG_KEYS.petskin);

        const baseItem = getSigField(newSig, SIG_KEYS.item);

        console.log(
          `scanned=${scanned} updated=${updated} skipped=${skipped} ` +
            `last=(ended_ts:${r.ended_ts} uuid:${uuid}) ` +
            `item=${baseItem || "?"} stars=${d || "0"}/${m || "0"} petlvl=${petLvl || "0"} petskin=${petSkin || "none"}`
        );
      }
    }

    await sleep(SLEEP_MS);
  }

  console.log(`DONE. scanned=${scanned} updated=${updated} skipped=${skipped}`);
  await pool.end();
}

main().catch(async (e) => {
  console.error("BACKFILL ERROR:", e);
  try {
    await pool.end();
  } catch {}
  process.exit(1);
});
