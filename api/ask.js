import snowflake from "snowflake-sdk";
import { BedrockRuntimeClient, ConverseCommand } from "@aws-sdk/client-bedrock-runtime";
import { getSnowflakePrivateKeyParam } from "../utils/keys.js";
import { retrieveChunks } from "../src/rag/retrieve.js";

const {
  INGEST_API_KEY,
  AWS_ACCESS_KEY_ID,
  AWS_SECRET_ACCESS_KEY,
  AWS_REGION,
  SNOWFLAKE_ACCOUNT,
  SNOWFLAKE_USER,
  SNOWFLAKE_WAREHOUSE,
  SNOWFLAKE_DATABASE,
  SNOWFLAKE_SCHEMA,
  SNOWFLAKE_ROLE,
} = process.env;

// Debug AWS credentials (remove in production)
console.log('AWS Credentials check:', {
  hasAccessKey: !!AWS_ACCESS_KEY_ID,
  hasSecretKey: !!AWS_SECRET_ACCESS_KEY,
  region: AWS_REGION || 'us-east-1',
  accessKeyPrefix: AWS_ACCESS_KEY_ID?.substring(0, 5) + '...'
});

// Validate AWS credentials before creating client
if (!AWS_ACCESS_KEY_ID || !AWS_SECRET_ACCESS_KEY) {
  throw new Error('Missing AWS credentials: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required');
}

const bedrockClient = new BedrockRuntimeClient({
  region: AWS_REGION || 'us-east-1',
  credentials: {
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
  },
  maxAttempts: 1, // Reduce retries for faster error detection
});

function authOK(req) {
  const h = req.headers.get?.("authorization") || req.headers.authorization || "";
  return INGEST_API_KEY && h === `Bearer ${INGEST_API_KEY}`;
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

    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    const question = (body.question || "").trim();
    if (!question) return res.status(400).json({ error: "missing_question" });

    const k = body.k ?? 12;
    const scope = body.scope; // { meeting?: string, customer?: string }

    // Build system seatbelts based on scope
    const system = `
You are summarizing/or answering questions about meeting content.
${scope?.meeting ? `If scope.meeting is set (e.g., "${scope.meeting}"), ONLY use context whose MEETING_TITLE contains that value.` : ''}
${scope?.customer ? `If scope.customer is set (e.g., "${scope.customer}"), ONLY use context whose CUSTOMER contains that value.` : ''}
${scope ? 'If no matching context exists, say: "I don\'t have notes for that meeting/customer."' : ''}
${scope ? 'Do NOT draw from meetings that don\'t match scope.' : ''}
Return concise answers and cite CHUNK_IDs you used.
`.trim();

    // Retrieve chunks using the new retrieval system
    const chunks = await retrieveChunks({ question, scope, k });
    
    if (chunks.length === 0) {
      if (scope) {
        return res.status(200).json({ 
          ok: true, 
          answer: `I don't have notes for that ${scope.meeting ? 'meeting' : 'customer'}.`, 
          sources: [] 
        });
      } else {
        return res.status(200).json({ 
          ok: true, 
          answer: "I couldn't find anything relevant to your question.", 
          sources: [] 
        });
      }
    }

    // Build context block for the LLM
    const contextBlock = chunks.map(c =>
      `[${c.id} | ${c.meetingTitle} | ${c.sectionTitle}]\n${c.text}`
    ).join("\n\n---\n\n");

    // Build Nova Pro messages
    const messages = [
      { role: "system", content: [{ text: system }] },
      { role: "user", content: [{ text: `Question: ${question}

Context:
${contextBlock}

Instructions:
* Use ONLY the context above.
${scope ? '* If none is relevant to the scope, say you don\'t have notes.' : ''}
* End with: "Sources: " followed by the CHUNK_IDs used, comma-separated.` }] }
    ];

    console.log('Calling Bedrock with:', {
      modelId: 'amazon.nova-pro-v1:0',
      promptLength: contextBlock.length,
      hasClient: !!bedrockClient,
      scope,
      chunksCount: chunks.length
    });
    
    const bedrockResponse = await bedrockClient.send(new ConverseCommand({
      modelId: 'amazon.nova-pro-v1:0',
      messages: messages,
      inferenceConfig: {
        maxTokens: 800,
        temperature: 0.2,
        topP: 0.9
      }
    }));

    const responseBody = bedrockResponse;
    const answer = responseBody?.output?.message?.content?.[0]?.text || "";

    // Return answer plus lightweight citations
    const sources = chunks.map(c => ({
      chunk_id: c.id,
      meeting_id: c.meetingId,
      meeting_title: c.meetingTitle,
      customer: c.customer,
      section_title: c.sectionTitle,
      score: c.score,
      snippet: String(c.text).slice(0, 240)
    }));

    res.status(200).json({ ok: true, answer, sources });
  } catch (e) {
    console.error("ask error:", e);
    res.status(500).json({ error: "ask_failed", detail: String(e?.message || e) });
  }
}
