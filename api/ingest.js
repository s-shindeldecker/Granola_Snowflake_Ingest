import snowflake from 'snowflake-sdk';
import { getSnowflakePrivateKeyParam, computePrivateKeyFingerprint, detectKeySource } from "../utils/keys.js";

// Configure Snowflake connection
const snowflakeConfig = {
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USER,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_DATABASE,
  schema: process.env.SNOWFLAKE_SCHEMA,
  role: process.env.SNOWFLAKE_ROLE,
  authenticator: 'SNOWFLAKE_JWT',
};

// Validate required environment variables
function validateEnvironment() {
  const required = [
    'SNOWFLAKE_ACCOUNT',
    'SNOWFLAKE_USER', 
    'SNOWFLAKE_WAREHOUSE',
    'SNOWFLAKE_DATABASE',
    'SNOWFLAKE_SCHEMA',
    'SNOWFLAKE_ROLE',
    'INGEST_API_KEY'
  ];
  
  const missing = required.filter(key => !process.env[key]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

// Validate API key
function validateApiKey(authHeader) {
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    throw new Error('Invalid authorization header. Must be "Bearer <API_KEY>"');
  }
  
  const apiKey = authHeader.substring(7);
  if (apiKey !== process.env.INGEST_API_KEY) {
    throw new Error('Invalid API key');
  }
}

// Validate request payload
function validatePayload(payload) {
  const required = ['meeting_id', 'title', 'datetime', 'participants', 'note_url', 'granola_summary', 'transcript'];
  const missing = required.filter(key => !payload[key]);
  
  if (missing.length > 0) {
    throw new Error(`Missing required fields: ${missing.join(', ')}`);
  }
  
  // Validate datetime format
  if (isNaN(Date.parse(payload.datetime))) {
    throw new Error('Invalid datetime format');
  }
  
  // Ensure participants is an array
  if (!Array.isArray(payload.participants)) {
    throw new Error('Participants must be an array');
  }
}

// Create Snowflake connection
async function createSnowflakeConnection() {
  return new Promise((resolve, reject) => {
    // Load and validate the private key
    let keyParam;
    try {
      keyParam = getSnowflakePrivateKeyParam();
      console.info("Snowflake key source:", detectKeySource());
      try {
        const fp = computePrivateKeyFingerprint(keyParam);
        console.info("Snowflake key fingerprint (base64):", fp);
      } catch (e) {
        console.warn("Could not compute key fingerprint:", e?.message || e);
      }
    } catch (e) {
      reject(new Error(`Private key configuration error: ${e.message}`));
      return;
    }

    // Create connection config with the validated key
    const connectionConfig = {
      ...snowflakeConfig,
      privateKey: keyParam,
    };

    console.log('Creating Snowflake connection with config:', {
      account: connectionConfig.account,
      username: connectionConfig.username,
      warehouse: connectionConfig.warehouse,
      database: connectionConfig.database,
      schema: connectionConfig.schema,
      role: connectionConfig.role,
      authenticator: connectionConfig.authenticator,
      hasPrivateKey: !!connectionConfig.privateKey
    });
    
    const connection = snowflake.createConnection(connectionConfig);
    
    connection.connect((err, conn) => {
      if (err) {
        console.error('Snowflake connection error:', err);
        reject(new Error(`Failed to connect to Snowflake: ${err.message}`));
        return;
      }
      console.log('Successfully connected to Snowflake');
      
      // Optional: confirm identity after connection
      conn.execute({
        sqlText: 'SELECT CURRENT_USER() as current_user',
        complete: (err, stmt, rows) => {
          if (!err && rows && rows.length > 0) {
            console.info('Connected as Snowflake user:', rows[0].CURRENT_USER);
          }
        }
      });
      
      resolve(conn);
    });
  });
}

// Create MEETINGS table if it doesn't exist
async function ensureTableExists(connection) {
  // First, check if the table exists and what its current schema is
  const checkTableSQL = `
    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'MEETINGS' 
    AND TABLE_SCHEMA = '${process.env.SNOWFLAKE_SCHEMA}'
    ORDER BY ORDINAL_POSITION
  `;
  
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: checkTableSQL,
      complete: (err, stmt, rows) => {
        if (err) {
          // If table doesn't exist, create it
          createNewTable(connection, resolve, reject);
          return;
        }
        
        if (rows && rows.length > 0) {
          console.log('Existing table schema:', rows);
          
          // Check if participants column has the right type
          const participantsCol = rows.find(row => row.COLUMN_NAME === 'PARTICIPANTS');
          if (participantsCol && participantsCol.DATA_TYPE !== 'ARRAY') {
            console.log('Participants column has wrong type:', participantsCol.DATA_TYPE, '- recreating table');
            // Drop and recreate table with correct schema
            dropAndRecreateTable(connection, resolve, reject);
          } else {
            console.log('Table schema is correct, proceeding with insert');
            resolve();
          }
        } else {
          // No table exists, create it
          createNewTable(connection, resolve, reject);
        }
      }
    });
  });
}

function createNewTable(connection, resolve, reject) {
  const createTableSQL = `
    CREATE TABLE MEETINGS (
      meeting_id VARCHAR(255) NOT NULL,
      title VARCHAR(500),
      datetime TIMESTAMP_NTZ,
      participants ARRAY,
      note_url VARCHAR(1000),
      granola_summary TEXT,
      transcript TEXT,
      created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      PRIMARY KEY (meeting_id)
    )
  `;
  
  connection.execute({
    sqlText: createTableSQL,
    complete: (err, stmt, rows) => {
      if (err) {
        reject(new Error(`Failed to create table: ${err.message}`));
        return;
      }
      console.log('Created new MEETINGS table with correct schema');
      resolve();
    }
  });
}

function dropAndRecreateTable(connection, resolve, reject) {
  const dropSQL = `DROP TABLE IF EXISTS MEETINGS`;
  
  connection.execute({
    sqlText: dropSQL,
    complete: (err, stmt, rows) => {
      if (err) {
        reject(new Error(`Failed to drop table: ${err.message}`));
        return;
      }
      console.log('Dropped existing MEETINGS table');
      createNewTable(connection, resolve, reject);
    }
  });
}

// Insert meeting data
async function insertMeeting(connection, payload) {
  const insertSQL = `
    INSERT INTO MEETINGS (
      meeting_id, title, datetime, participants, note_url, granola_summary, transcript
    ) VALUES (?, ?, ?, PARSE_JSON(?), ?, ?, ?)
  `;
  
  // Convert participants array to JSON string for PARSE_JSON function
  let participantsParam;
  if (Array.isArray(payload.participants)) {
    // Convert array to JSON string for Snowflake's PARSE_JSON function
    participantsParam = JSON.stringify(payload.participants);
  } else {
    // Fallback: convert to array format if it's not already
    participantsParam = JSON.stringify([payload.participants]);
  }
  
  const params = [
    payload.meeting_id,
    payload.title,
    payload.datetime,
    participantsParam, // Pass as JSON string for PARSE_JSON
    payload.note_url,
    payload.granola_summary,
    payload.transcript
  ];
  
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: insertSQL,
      binds: params,
      complete: (err, stmt, rows) => {
        if (err) {
          reject(new Error(`Failed to insert meeting: ${err.message}`));
          return;
        }
        resolve();
      }
    });
  });
}

// Main handler function
export default async function handler(req, res) {
  try {
    // Only allow POST requests
    if (req.method !== 'POST') {
      return res.status(405).json({ 
        error: 'Method not allowed. Only POST requests are supported.' 
      });
    }
    
    // Validate environment variables
    validateEnvironment();
    
    // Validate authorization header
    const authHeader = req.headers.authorization;
    validateApiKey(authHeader);
    
    // Parse and validate request body
    let payload;
    try {
      // Handle both string and already-parsed JSON body
      if (typeof req.body === 'string') {
        payload = JSON.parse(req.body);
      } else {
        payload = req.body;
      }
    } catch (err) {
      return res.status(400).json({ 
        error: 'Invalid JSON in request body' 
      });
    }
    
    validatePayload(payload);
    
    // Connect to Snowflake
    const connection = await createSnowflakeConnection();
    
    try {
      // Ensure table exists
      await ensureTableExists(connection);
      
      // Insert the meeting data
      await insertMeeting(connection, payload);
      
      // Close connection
      connection.destroy();
      
      // Return success response
      return res.status(200).json({ ok: true });
      
    } catch (err) {
      connection.destroy();
      throw err;
    }
    
  } catch (error) {
    console.error('Ingest API error:', error);
    console.error('Error details:', {
      message: error.message,
      stack: error.stack,
      name: error.name
    });
    
    // Return appropriate error response
    if (error.message.includes('Invalid authorization') || error.message.includes('Invalid API key')) {
      return res.status(401).json({ error: error.message });
    }
    
    if (error.message.includes('Missing required') || error.message.includes('Invalid')) {
      return res.status(400).json({ error: error.message });
    }
    
    // Default to 500 for unexpected errors
    return res.status(500).json({ 
      error: 'Internal server error. Please try again later.',
      details: error.message
    });
  }
}
