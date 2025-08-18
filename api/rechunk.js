import snowflake from "snowflake-sdk";
import OpenAI from "openai";
import { v4 as uuidv4 } from "uuid";
import { getSnowflakePrivateKeyParam } from "../utils/keys.js";

const {
  INGEST_API_KEY,
  OPENAI_API_KEY,
  SNOWFLAKE_ACCOUNT,
  SNOWFLAKE_USER,
  SNOWFLAKE_WAREHOUSE,
  SNOWFLAKE_DATABASE,
  SNOWFLAKE_SCHEMA,
  SNOWFLAKE_ROLE,
} = process.env;

const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

// ---- auth helper ----
function authOK(req) {
  const h = req.headers.get?.("authorization") || req.headers.authorization || "";
  return INGEST_API_KEY && h === `Bearer ${ INGEST_API_KEY }`;
}

// ---- basic sentence-aware chunker with overlap ----
function chunkBySentences(text, maxChars = 3000, overlap = 400) {
  const src = String(text || "").replace(/\r/g, "\n");
  // naive sentence splitter: split on end punctuation followed by space + capital/quote/paren
  const sentences = src.split(/(?<=[\.\!\?])\s+(?=[A-Z0-9"'(])/g);
  const chunks = [];
  let buf = "";

  const flushIfNeeded = () => {
    if (buf.length >= maxChars) {
      chunks.push(buf.slice(0, maxChars));
      const tail = buf.slice(Math.max(0, buf.length - overlap));
      buf = tail;
    }
  };

  for (const s of sentences) {
    const piece = s?.trim();
    if (!piece) continue;
    if (!buf) buf = piece;
    else buf += (buf.endsWith("\n") ? "" : " ") + piece;
    flushIfNeeded();
  }
  if (buf.trim()) chunks.push(buf.trim());
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

async function embed(text) {
  const r = await openai.embeddings.create({
    model: "text-embedding-3-small",
    input: text,
  });
  return r.data[0].embedding; // array of floats, len 1536
}

async function insertChunks(conn, meetingId, chunks) {
  // idempotent refresh
  await exec(conn, `delete from CHUNKS where MEETING_ID = ?`, [meetingId]);

  let idx = 0;
  for (const c of chunks) {
    const emb = await embed(c);
    await exec(
      conn,
      `insert into CHUNKS (CHUNK_ID, MEETING_ID, IDX, TEXT, EMBEDDING)
       select :cid, :mid, :idx, :txt,
              TO_VECTOR(PARSE_JSON(:emb))::VECTOR(FLOAT, 1536)`,
      {
        cid: uuidv4(),
        mid: meetingId,
        idx,
        txt: c,
        emb: JSON.stringify(emb),
      }
    );
    idx += 1;
  }
  return idx;
}

export default async function handler(req, res) {
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

    const processOne = async (id) => {
      const t = await fetchTranscript(conn, id);
      if (!t || !String(t).trim()) return { meeting_id: id, chunks: 0, skipped: "empty_transcript" };
      const chunks = chunkBySentences(String(t), 3000, 400);
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
