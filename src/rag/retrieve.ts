import snowflake from "snowflake-sdk";
import { getSnowflakePrivateKeyParam } from "../../utils/keys.js";

const {
  SNOWFLAKE_ACCOUNT,
  SNOWFLAKE_USER,
  SNOWFLAKE_WAREHOUSE,
  SNOWFLAKE_DATABASE,
  SNOWFLAKE_SCHEMA,
  SNOWFLAKE_ROLE,
} = process.env;

export type Scope = { meeting?: string; customer?: string } | undefined;

export interface ChunkResult {
  id: string;
  meetingId: string;
  meetingTitle: string;
  customer: string;
  sectionTitle: string;
  text: string;
  score: number;
  vec: number[];
}

// Snowflake connection helper
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

// Execute SQL with binds
function exec(conn: any, sqlText: string, binds: any[] = []) {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText,
      binds,
      complete: (err: any, stmt: any, rows: any) => (err ? reject(err) : resolve(rows || [])),
    });
  });
}

// MMR (Maximal Marginal Relevance) implementation
export function mmr(
  queryVec: number[],
  docs: Array<{ CHUNK_ID: string; sim: number; vec: number[] }>,
  k = 12,
  lambda = 0.7
) {
  const chosen: typeof docs = [];
  const dot = (a: number[], b: number[]) => a.reduce((s, ai, i) => s + ai * b[i], 0);
  const norm = (v: number[]) => Math.hypot(...v);
  const cos = (a: number[], b: number[]) => dot(a, b) / (norm(a) * norm(b));

  const pool = [...docs];
  while (chosen.length < Math.min(k, pool.length)) {
    let best = -Infinity, bestDoc: typeof docs[number] | null = null;
    for (const d of pool) {
      const simQ = d.sim;
      const simToChosen = chosen.length ? Math.max(...chosen.map(c => cos(d.vec, c.vec))) : 0;
      const score = lambda * simQ - (1 - lambda) * simToChosen;
      if (score > best) { best = score; bestDoc = d; }
    }
    if (!bestDoc) break;
    chosen.push(bestDoc);
    const idx = pool.findIndex(x => x.CHUNK_ID === bestDoc!.CHUNK_ID);
    if (idx >= 0) pool.splice(idx, 1);
  }
  return chosen;
}

// Main retrieval function
export async function retrieveChunks({ 
  question, 
  scope, 
  k = 12 
}: { 
  question: string; 
  scope?: Scope; 
  k?: number; 
}): Promise<ChunkResult[]> {
  const conn = await getConn();
  
  try {
    // 1. Embed the question
    const embedRows = await exec(
      conn,
      `SELECT AI_EMBED('snowflake-arctic-embed-l-v2.0', ?) AS Q_EMBED`,
      [question]
    );
    
    if (!embedRows.length) {
      throw new Error("Failed to generate question embedding");
    }
    
    const qEmbed = embedRows[0].Q_EMBED;
    
    // 2. Execute scope-then-rank SQL
    const scopeSQL = `
      WITH SCOPE AS (
        SELECT *
        FROM FARM_FRESH_PET.INTERNAL.CHUNKS
        WHERE (:meeting IS NULL OR MEETING_TITLE ILIKE '%' || :meeting || '%')
          AND (:customer IS NULL OR CUSTOMER ILIKE '%' || :customer || '%')
      ),
      SCORED AS (
        SELECT
          CHUNK_ID, MEETING_ID, MEETING_TITLE, CUSTOMER, START_SEC, END_SEC, 
          SECTION_ID, SECTION_TITLE, TEXT, EMBED_1024,
          1 - VECTOR_COSINE_DISTANCE(EMBED_1024, ?) AS sim,
          (CASE
             WHEN :meeting IS NOT NULL AND (TEXT ILIKE '%'||:meeting||'%' OR SECTION_TITLE ILIKE '%'||:meeting||'%') THEN 0.05 ELSE 0
           END
           + CASE
             WHEN :customer IS NOT NULL AND (TEXT ILIKE '%'||:customer||'%' OR SECTION_TITLE ILIKE '%'||:customer||'%') THEN 0.05 ELSE 0
           END) AS kw_bonus
        FROM SCOPE
      ),
      RERANK AS (
        SELECT *, (sim + kw_bonus) AS score
        FROM SCORED
      )
      SELECT *
      FROM RERANK
      QUALIFY ROW_NUMBER() OVER (PARTITION BY MEETING_ID ORDER BY score DESC) <= 6
      ORDER BY score DESC
      LIMIT ?
    `;
    
    const scopeBinds = [
      qEmbed, // question embedding
      scope?.meeting || null,
      scope?.customer || null,
      scope?.meeting || null,
      scope?.customer || null,
      Math.min(k * 3, 50) // fetch more for MMR
    ];
    
    const rows = await exec(conn, scopeSQL, scopeBinds);
    
    // Log top candidates
    console.log('Top candidates before MMR:', rows.slice(0, 10).map(r => ({
      id: r.CHUNK_ID,
      meeting: r.MEETING_TITLE,
      score: r.score
    })));
    
    if (rows.length === 0) {
      if (scope) {
        console.warn(`no_context_for_scope: ${JSON.stringify(scope)}`);
      }
      return [];
    }
    
    // 3. Fetch vectors for MMR (if not already in results)
    const chunkIds = rows.map(r => r.CHUNK_ID);
    const vectorRows = await exec(
      conn,
      `SELECT CHUNK_ID, TO_ARRAY(EMBED_1024) as vec FROM CHUNKS WHERE CHUNK_ID IN (${chunkIds.map(() => '?').join(',')})`,
      chunkIds
    );
    
    // Create vector lookup
    const vectorMap = new Map(vectorRows.map(r => [r.CHUNK_ID, r.VEC]));
    
    // 4. Run MMR to select final k chunks
    const docsWithVectors = rows
      .map(r => ({
        CHUNK_ID: r.CHUNK_ID,
        sim: r.score,
        vec: vectorMap.get(r.CHUNK_ID) || []
      }))
      .filter(d => d.vec.length > 0);
    
    const finalChunks = mmr([], docsWithVectors, k, 0.7);
    
    // Log final chosen chunks
    console.log('Final chosen chunks after MMR:', finalChunks.map(c => ({
      id: c.CHUNK_ID,
      score: c.sim
    })));
    
    // 5. Fetch full chunk data for final results
    const finalChunkIds = finalChunks.map(c => c.CHUNK_ID);
    const finalRows = await exec(
      conn,
      `SELECT 
        CHUNK_ID, MEETING_ID, MEETING_TITLE, CUSTOMER, 
        SECTION_ID, SECTION_TITLE, TEXT
       FROM CHUNKS 
       WHERE CHUNK_ID IN (${finalChunkIds.map(() => '?').join(',')})`,
      finalChunkIds
    );
    
    // 6. Map to result format
    const results: ChunkResult[] = finalRows.map((row, idx) => ({
      id: row.CHUNK_ID,
      meetingId: row.MEETING_ID,
      meetingTitle: row.MEETING_TITLE,
      customer: row.CUSTOMER,
      sectionTitle: row.SECTION_TITLE,
      text: row.TEXT,
      score: finalChunks[idx]?.sim || 0,
      vec: finalChunks[idx]?.vec || []
    }));
    
    return results;
    
  } finally {
    conn.destroy();
  }
}
