// backfillsales.js (STRONG v3 - ultimate + stars(0/0 too) + wither)
//
// Rebuilds signatures for rows that:
//  1) missing signature
//  2) signature contains "ultimate " (old format)
//  3) stars fields missing OR stars are 0/0 (dstars=0 AND mstars=0)
//  4) wither blades with bytes but suspicious wi/scrolls
//
// Notes:
// - Assumes buildSignature writes:
//   item=...|tier=...|dstars=N|mstars=N|wi=...|scrolls=...|ult=none|ench=...
// - This script DOES NOT depend on old "ult=" tokens; it rebuilds from source fields.

import dotenv from "dotenv";
import pg from "pg";
import { buildSignature, canonicalItemKey } from "./parseLore.js";

dotenv.config();

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

function nonEmptyText(x) {
  const s = (x ?? "").toString();
  return s && s.trim() ? s.trim() : "";
}

function parseKeyedField(sig, field) {
  const s = String(sig || "");
  const m = s.match(new RegExp(`${field}=([^|]*)`));
  return m ? m[1] : "";
}

function hasBytesRow(r) {
  return nonEmptyText(r.item_bytes).length > 0;
}

// -------------------------
// Wither blade helpers
// -------------------------
const WITHER_BLADE_SET = new Set(["hyperion", "astraea", "scylla", "valkyrie"]);

function isWitherBladeName(itemName) {
  const k = canonicalItemKey(itemName || "");
  return WITHER_BLADE_SET.has(k);
}

function sigLooksSuspiciousForWitherBlade(sig) {
  const s = String(sig || "");
  if (!s.trim()) return true;
  const wi = parseKeyedField(s, "wi");
  const scrolls = parseKeyedField(s, "scrolls");
  if (!wi || !scrolls) return true;
  if (wi === "0") return true;
  if (scrolls === "none") return true;
  return false;
}

// -------------------------
// Stars helpers
// -------------------------
function sigMissingStarsFields(sig) {
  const s = String(sig || "");
  if (!s.trim()) return true;
  return !s.includes("|dstars=") || !s.includes("|mstars=");
}

function sigStarsAreZeroZero(sig) {
  const s = String(sig || "");
  if (!s.trim()) return true;
  const d = parseKeyedField(s, "dstars");
  const m = parseKeyedField(s, "mstars");
  const dn = Number(d);
  const mn = Number(m);
  if (!Number.isFinite(dn) || !Number.isFinite(mn)) return true;
  return dn === 0 && mn === 0;
}

function sigStarsInvalid(sig) {
  const s = String(sig || "");
  if (!s.trim()) return true;
  const d = parseKeyedField(s, "dstars");
  const m = parseKeyedField(s, "mstars");
  const dn = Number(d);
  const mn = Number(m);
  if (!Number.isFinite(dn) || !Number.isFinite(mn)) return true;
  return dn < 0 || dn > 5 || mn < 0 || mn > 5;
}

// -------------------------
// Reporting queries
// -------------------------
async function countLike(client, whereSql) {
  const { rows } = await client.query(`SELECT COUNT(*)::int AS n FROM sales WHERE ${whereSql}`);
  return rows?.[0]?.n ?? 0;
}

async function reportCounts(client) {
  const total = await countLike(client, "TRUE");
  const missingSig = await countLike(client, "signature IS NULL OR signature = ''");
  const hasUltimate = await countLike(client, "signature LIKE '%ultimate %'");
  const missingStarsFields = await countLike(
    client,
    "signature IS NULL OR signature = '' OR signature NOT LIKE '%|dstars=%|%' OR signature NOT LIKE '%|mstars=%|%'"
  );
  const starsZeroZero = await countLike(
    client,
    "signature LIKE '%|dstars=0|%' AND signature LIKE '%|mstars=0|%'"
  );
  const witherRows = await countLike(
    client,
    "(item_name ILIKE '%hyperion%' OR item_name ILIKE '%astraea%' OR item_name ILIKE '%scylla%' OR item_name ILIKE '%valkyrie%')"
  );
  const witherWithBytes = await countLike(
    client,
    "(item_name ILIKE '%hyperion%' OR item_name ILIKE '%astraea%' OR item_name ILIKE '%scylla%' OR item_name ILIKE '%valkyrie%') AND item_bytes IS NOT NULL AND item_bytes <> ''"
  );

  return {
    total,
    missingSig,
    hasUltimate,
    missingStarsFields,
    starsZeroZero,
    witherRows,
    witherWithBytes,
  };
}

// -------------------------
// Backfill
// -------------------------
async function backfill({
  batch = 200,
  loops = 200000,
  stopAfterNoProgressBatches = 50,
  printEvery = 10,
} = {}) {
  let totalUpdated = 0;
  let totalSelected = 0;
  let noProgress = 0;

  for (let i = 0; i < loops; i++) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      // IMPORTANT: include starsZeroZero rows too, not only missing fields.
      const { rows } = await client.query(
        `
        SELECT uuid, item_name, item_lore, item_bytes, signature, ended_ts
        FROM sales
        WHERE
          (signature IS NULL OR signature = '')
          OR (signature LIKE '%ultimate %')
          OR (signature NOT LIKE '%|dstars=%|%' OR signature NOT LIKE '%|mstars=%|%')
          OR (signature LIKE '%|dstars=0|%' AND signature LIKE '%|mstars=0|%')
          OR (
            (item_name ILIKE '%hyperion%' OR item_name ILIKE '%astraea%' OR item_name ILIKE '%scylla%' OR item_name ILIKE '%valkyrie%')
            AND item_bytes IS NOT NULL AND item_bytes <> ''
            AND (
              signature LIKE '%|scrolls=none|%'
              OR signature LIKE '%|wi=0|%'
              OR signature NOT LIKE '%|wi=%|%'
              OR signature NOT LIKE '%|scrolls=%|%'
            )
          )
        ORDER BY ended_ts DESC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
        `,
        [batch]
      );

      if (!rows.length) {
        await client.query("COMMIT");
        client.release();
        console.log("✅ No more candidate rows. Stopping.");
        break;
      }

      totalSelected += rows.length;

      let updatedThisBatch = 0;
      let skippedNoBytes = 0;
      let skippedNoChange = 0;

      for (const r of rows) {
        const oldSig = String(r.signature || "");
        const bytesOk = hasBytesRow(r);
        const witherBlade = isWitherBladeName(r.item_name);

        const needsUltimateFix = oldSig.includes("ultimate ");
        const needsStarsFix =
          sigMissingStarsFields(oldSig) || sigStarsInvalid(oldSig) || sigStarsAreZeroZero(oldSig);
        const needsWBfix = witherBlade && sigLooksSuspiciousForWitherBlade(oldSig);

        // If wither blade has no bytes and it doesn't need stars/ultimate fixes, skip.
        if (witherBlade && !bytesOk && oldSig.trim() && !needsStarsFix && !needsUltimateFix && !needsWBfix) {
          skippedNoBytes++;
          continue;
        }

        const newSig = await buildSignature({
          itemName: r.item_name || "",
          lore: nonEmptyText(r.item_lore),
          tier: "",
          itemBytes: nonEmptyText(r.item_bytes),
        });

        if (!newSig || newSig.length < 10) {
          skippedNoChange++;
          continue;
        }

        if (newSig === oldSig) {
          skippedNoChange++;
          continue;
        }

        await client.query(`UPDATE sales SET signature = $2 WHERE uuid = $1`, [r.uuid, newSig]);
        updatedThisBatch++;
      }

      await client.query("COMMIT");
      client.release();

      totalUpdated += updatedThisBatch;

      console.log(
        `✅ batch ${i + 1}: selected=${rows.length}, updated=${updatedThisBatch}, ` +
          `skipped(no-bytes)=${skippedNoBytes}, skipped(no-change)=${skippedNoChange}, totalUpdated=${totalUpdated}`
      );

      if ((i + 1) % printEvery === 0) {
        const c = await pool.connect();
        try {
          const counts = await reportCounts(c);
          console.log("📌 Progress:", counts);
        } finally {
          c.release();
        }
      }

      if (updatedThisBatch === 0) {
        noProgress++;
        if (noProgress >= stopAfterNoProgressBatches) {
          console.log(`⚠️ No progress for ${noProgress} batches. Stopping.`);
          break;
        }
      } else {
        noProgress = 0;
      }
    } catch (e) {
      try {
        await client.query("ROLLBACK");
      } catch {}
      client.release();
      throw e;
    }
  }

  return { totalUpdated, totalSelected };
}

(async () => {
  try {
    const c0 = await pool.connect();
    try {
      const counts = await reportCounts(c0);
      console.log("🔎 Candidate overview:", counts);
    } finally {
      c0.release();
    }

    const { totalUpdated, totalSelected } = await backfill({
      batch: 200,
      loops: 200000,
      stopAfterNoProgressBatches: 50,
      printEvery: 10,
    });

    console.log(`✅ Done. selected=${totalSelected}, updated=${totalUpdated}`);

    const c1 = await pool.connect();
    try {
      const counts = await reportCounts(c1);
      console.log("📌 Final counts:", counts);
    } finally {
      c1.release();
    }

    process.exit(0);
  } catch (e) {
    console.error("❌ Backfill failed:", e);
    process.exit(1);
  }
})();
