import snowflake from "snowflake-sdk";
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

function authOK(req) {
  const h = req.headers.get?.("authorization") || req.headers.authorization || "";
  return INGEST_API_KEY && h === `Bearer ${INGEST_API_KEY}`;
}

async function getConn() {
  const conn = snowflake.createConnection({
    account: SNOWFLAKE_ACCOUNT,
    username: SNOWFLAKE_USER,
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
function exec(conn, sqlText, binds = {}) {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText,
      binds,
      complete: (err, stmt, rows) => (err ? reject(err) : resolve(rows || [])),
    });
  });
}

function buildFilterClause(filters, binds) {
  let where = `c.EMBED_1024 IS NOT NULL`;
  if (!filters) return { where, binds };
  if (filters.meeting_id) { where += ` AND c.MEETING_ID = :f_meeting_id`; binds.f_meeting_id = filters.meeting_id; }
  if (filters.title_like) { where += ` AND m.TITLE ILIKE :f_title_like`; binds.f_title_like = `%${filters.title_like}%`; }
  if (filters.participants_contains) { where += ` AND m.PARTICIPANTS ILIKE :f_participant`; binds.f_participant = `%${filters.participants_contains}%`; }
  if (filters.date_from) { where += ` AND m.DATETIME >= TO_TIMESTAMP_TZ(:f_from)`; binds.f_from = filters.date_from; }
  if (filters.date_to) { where += ` AND m.DATETIME <= TO_TIMESTAMP_TZ(:f_to)`; binds.f_to = filters.date_to; }
  return { where, binds };
}

function mkPrompt(question, rows) {
  const context = rows.map((r, i) =>
    `# Source ${i+1} — ${r.TITLE || r.MEETING_ID} [${r.MEETING_ID}#${r.IDX}] (sim=${r.SIM.toFixed(3)})
${r.TEXT}`.trim()
  ).join("\n\n---\n\n");

  return `You are an assistant grounded strictly in the provided meeting chunks.
- Answer the user's question using ONLY the context.
- If the answer isn't in the context, say you don't know.
- Cite sources as [MEETING_ID#IDX].

Question: ${question}

Context:
${context}

Now produce a concise, actionable answer with bullet points and explicit citations.`;
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") return res.status(405).json({ error: "method_not_allowed" });
    if (!authOK(req)) return res.status(401).json({ error: "unauthorized" });

    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    const question = (body.question || "").trim();
    if (!question) return res.status(400).json({ error: "missing_question" });

    const topK = Math.min(Math.max(Number(body.top_k || 8), 1), 20);
    const model = (body.model || "snowflake-arctic-instruct"); // can be "mistral-large-2" etc.

    const conn = await getConn();

    // 1) Retrieve top-K chunks using AI_EMBED + VECTOR_COSINE_SIMILARITY
    const binds = { q: question, k: topK };
    const { where, binds: withFilters } = buildFilterClause(body.filters, binds);
    const rows = await exec(conn, `
      WITH q AS (
        SELECT AI_EMBED('snowflake-arctic-embed-l-v2.0', :q) AS QV
      )
      SELECT c.CHUNK_ID, c.MEETING_ID, c.IDX, c.TEXT,
             m.TITLE,
             VECTOR_COSINE_SIMILARITY(c.EMBED_1024, q.QV) AS SIM
        FROM CHUNKS c
        JOIN MEETINGS m ON m.MEETING_ID = c.MEETING_ID
        JOIN q
       WHERE ${where}
       ORDER BY SIM DESC
       LIMIT :k
    `, withFilters);

    if (!rows.length) {
      return res.status(200).json({ ok: true, answer: "I couldn't find anything relevant.", sources: [] });
    }

    // 2) Build prompt and call AI_COMPLETE in Snowflake
    const prompt = mkPrompt(question, rows.slice(0, topK));
    const ansRows = await exec(conn, `
      SELECT AI_COMPLETE(:model, :prompt, OBJECT_CONSTRUCT('temperature', 0.2, 'max_output_tokens', 800)) AS ANSWER
    `, { model, prompt });

    const answer = ansRows?.[0]?.ANSWER || "";

    // Return answer plus lightweight citations
    const sources = rows.map(r => ({
      meeting_id: r.MEETING_ID,
      idx: r.IDX,
      sim: Number(r.SIM),
      title: r.TITLE,
      snippet: String(r.TEXT).slice(0, 240)
    }));

    res.status(200).json({ ok: true, answer, sources });
  } catch (e) {
    console.error("ask error:", e);
    res.status(500).json({ error: "ask_failed", detail: String(e?.message || e) });
  }
}
