import snowflake from "snowflake-sdk";
import { v4 as uuidv4 } from "uuid";
import { getSnowflakePrivateKeyParam } from "../utils/keys.js";

const {
  INGEST_API_KEY,
  SNOWFLAKE_ACCOUNT,
  SNOWFLAKE_USER,
  SNOWFLAKE_WAREHOUSE,
  SNOWFLAKE_DATABASE,
  SNOWFLAKE_SCHEMA,
  SNOWFLAKE_ROLE,
} = process.env;

// ---- auth helper ----
function authOK(req) {
  const h = req.headers.get?.("authorization") || req.headers.authorization || "";
  return INGEST_API_KEY && h === `Bearer ${ INGEST_API_KEY }`;
}

// ---- improved chunker with metadata enrichment ----
function chunkBySentences(text, targetTokens = 1000, overlapTokens = 150) {
  const src = String(text || "").replace(/\r/g, "\n");
  
  // Approximate token count (rough estimate: 1 token ≈ 4 characters)
  const estimateTokens = (str) => Math.ceil(str.length / 4);
  
  // Split on sentence boundaries, speaker changes, and natural breaks
  const sentences = src.split(/(?<=[\.\!\?])\s+(?=[A-Z0-9"'(])/g);
  const chunks = [];
  let buf = "";
  let sectionId = 1;
  let sectionTitle = "";

  const flushIfNeeded = () => {
    if (estimateTokens(buf) >= targetTokens) {
      // Generate section title from first sentence if none exists
      if (!sectionTitle) {
        const firstSentence = buf.split(/[.!?]/)[0].trim();
        sectionTitle = firstSentence.length > 50 ? firstSentence.substring(0, 50) + "..." : firstSentence;
      }
      
      chunks.push({
        text: buf.slice(0, targetTokens * 4), // Approximate character limit
        sectionId,
        sectionTitle,
        tokenCount: estimateTokens(buf)
      });
      
      // Keep overlap for next chunk
      const tail = buf.slice(Math.max(0, buf.length - overlapTokens * 4));
      buf = tail;
      sectionId++;
      sectionTitle = "";
    }
  };

  for (const s of sentences) {
    const piece = s?.trim();
    if (!piece) continue;
    if (!buf) buf = piece;
    else buf += (buf.endsWith("\n") ? "" : " ") + piece;
    flushIfNeeded();
  }
  
  if (buf.trim()) {
    if (!sectionTitle) {
      const firstSentence = buf.split(/[.!?]/)[0].trim();
      sectionTitle = firstSentence.length > 50 ? firstSentence.substring(0, 50) + "..." : firstSentence;
    }
    chunks.push({
      text: buf.trim(),
      sectionId,
      sectionTitle,
      tokenCount: estimateTokens(buf)
    });
  }
  
  return chunks;
}

// ---- snowflake helpers ----
async function getConn() {
  const conn = snowflake.createConnection({
    account: SNOWFLAKE_ACCOUNT,
    username: SNOWFLAKE_USER, // must match your Snowflake user exactly (case + @)
    authenticator: "SNOWFLAKE_JWT",
    privateKey: getSnowflakePrivateKeyParam(),
    warehouse: SNOWFLAKE_WAREHOUSE,
    database: SNOWFLAKE_DATABASE,
    schema: SNOWFLAKE_SCHEMA,
    role: SNOWFLAKE_ROLE,
  });
  await new Promise((res, rej) => conn.connect((e) => (e ? rej(e) : res())));
  return conn;
}

function exec(conn, sqlText, binds = []) {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText,
      binds,
      complete: (err, stmt, rows) => (err ? reject(err) : resolve(rows || [])),
    });
  });
}

// ---- bootstrap helper ----
async function ensureEmbed1024Column(conn) {
  // Check if EMBED_1024 column already exists
  const checkSQL = `
    SELECT COUNT(*) as col_exists
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'CHUNKS' 
    AND COLUMN_NAME = 'EMBED_1024'
    AND TABLE_SCHEMA = '${process.env.SNOWFLAKE_SCHEMA}'
  `;
  
  const rows = await exec(conn, checkSQL);
  const columnExists = rows[0]?.COL_EXISTS > 0;
  
  if (!columnExists) {
    const alterSQL = `ALTER TABLE CHUNKS ADD COLUMN EMBED_1024 VECTOR(FLOAT, 1024)`;
    await exec(conn, alterSQL);
    console.log('Added EMBED_1024 column to CHUNKS table');
  } else {
    console.log('EMBED_1024 column already exists in CHUNKS table');
  }
}

// ---- data access ----
async function fetchTranscript(conn, meetingId) {
  const rows = await exec(
    conn,
    `select MEETING_ID, to_varchar(TRANSCRIPT) as TRANSCRIPT
       from MEETINGS
      where MEETING_ID = ?`,
    [meetingId]
  );
  if (!rows.length) throw new Error("meeting_not_found");
  return rows[0].TRANSCRIPT; // stored as JSON string inside VARIANT -> varchar
}

async function insertChunks(conn, meetingId, chunks) {
  // Get meeting metadata for headers
  const meetingRows = await exec(
    conn,
    `SELECT TITLE, DATETIME, PARTICIPANTS FROM MEETINGS WHERE MEETING_ID = ?`,
    [meetingId]
  );
  
  if (!meetingRows.length) {
    throw new Error("meeting_not_found");
  }
  
  const meeting = meetingRows[0];
  const meetingTitle = meeting.TITLE || "Unknown Meeting";
  const meetingDate = meeting.DATETIME ? new Date(meeting.DATETIME).toISOString().split('T')[0] : "Unknown Date";
  
  // Parse participants to get customer info (assuming first participant is customer)
  let customer = "Unknown Customer";
  try {
    const participants = JSON.parse(meeting.PARTICIPANTS || '[]');
    if (participants.length > 0) {
      customer = participants[0];
    }
  } catch (e) {
    console.log("Could not parse participants, using default customer");
  }

  // idempotent refresh
  await exec(conn, `DELETE FROM CHUNKS WHERE MEETING_ID = ?`, [meetingId]);

  let idx = 0;
  for (const chunk of chunks) {
    // Create header for the chunk
    const header = `[Meeting: ${meetingTitle} | Customer: ${customer} | Date: ${meetingDate} | Section: ${chunk.sectionTitle} | t=${idx}]`;
    const headerizedText = `${header}\n${chunk.text}`;
    
    // Generate content hash for deduplication
    const crypto = await import('crypto');
    const contentHash = crypto.createHash('sha1').update(headerizedText).digest('hex');
    
    // Check if chunk with same hash already exists
    const existingChunks = await exec(
      conn,
      `SELECT COUNT(*) as count FROM CHUNKS WHERE MEETING_ID = ? AND CONTENT_HASH = ?`,
      [meetingId, contentHash]
    );
    
    if (existingChunks[0].COUNT > 0) {
      console.log(`Skipping duplicate chunk for meeting ${meetingId}, section ${chunk.sectionId}`);
      continue;
    }
    
    await exec(
      conn,
      `INSERT INTO CHUNKS (
        CHUNK_ID, MEETING_ID, IDX, TEXT, 
        MEETING_TITLE, MEETING_DATE, CUSTOMER, 
        SECTION_ID, SECTION_TITLE, TOKEN_COUNT, CONTENT_HASH
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        uuidv4(), meetingId, idx, headerizedText,
        meetingTitle, meetingDate, customer,
        chunk.sectionId, chunk.sectionTitle, chunk.tokenCount, contentHash
      ]
    );
    idx += 1;
  }

  // Compute embeddings for all chunks of this meeting using Snowflake Cortex
  if (idx > 0) {
    await exec(
      conn,
      `UPDATE CHUNKS
          SET EMBED_1024 = AI_EMBED('snowflake-arctic-embed-l-v2.0', TEXT)
        WHERE MEETING_ID = ?
          AND EMBED_1024 IS NULL`,
      [meetingId]
    );
  }

  return idx;
}

export default async function handler(req, res) {
  // Add CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  
  // Handle preflight OPTIONS request
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  try {
    if (req.method !== "POST") return res.status(405).json({ error: "method_not_allowed" });
    if (!authOK(req)) return res.status(401).json({ error: "unauthorized" });

    // Body: { meeting_id } OR { backfill_since: ISO8601 }
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    const { meeting_id, backfill_since } = body || {};

    if (!meeting_id && !backfill_since) {
      return res.status(400).json({ error: "missing_arg", detail: "Provide meeting_id or backfill_since (ISO date)." });
    }

    const conn = await getConn();

    // Ensure EMBED_1024 column exists
    await ensureEmbed1024Column(conn);

    const processOne = async (id) => {
      const t = await fetchTranscript(conn, id);
      if (!t || !String(t).trim()) return { meeting_id: id, chunks: 0, skipped: "empty_transcript" };
      
      // Use improved chunking with target 800-1200 tokens and 100-200 token overlap
      const chunks = chunkBySentences(String(t), 1000, 150);
      const n = await insertChunks(conn, id, chunks);
      return { meeting_id: id, chunks: n };
    };

    let results = [];
    if (meeting_id) {
      results.push(await processOne(meeting_id));
    } else {
      const rows = await exec(
        conn,
        `select MEETING_ID
           from MEETINGS
          where CREATED_AT >= to_timestamp_tz(?)
          order by CREATED_AT desc
          limit 200`,
        [backfill_since]
      );
      for (const r of rows) results.push(await processOne(r.MEETING_ID));
    }

    res.status(200).json({ ok: true, results });
  } catch (e) {
    console.error("rechunk error:", e);
    res.status(500).json({ error: "rechunk_failed", detail: String(e?.message || e) });
  }
}

/*
-- Example semantic search:
-- select MEETING_ID, IDX, TEXT,
--        VECTOR_COSINE_SIMILARITY(EMBED_1024, AI_EMBED('snowflake-arctic-embed-l-v2.0', :query)) as sim
--   from CHUNKS
--  order by sim desc
--  limit 12;
*/
