# Granola Snowflake Ingest API

A simple Vercel serverless function that receives JSON payloads from Zapier (Granola transcripts) and inserts them into Snowflake.

## Features

- **Secure API**: Bearer token authentication
- **Data Validation**: Comprehensive payload validation
- **Snowflake Integration**: Automatic table creation and data insertion
- **Error Handling**: Production-ready error responses
- **Vercel Ready**: Optimized for serverless deployment

## API Endpoint

**POST** `/api/ingest`

### Headers
```
Authorization: Bearer <INGEST_API_KEY>
Content-Type: application/json
```

### Request Body
```json
{
  "meeting_id": "string",
  "title": "string", 
  "datetime": "ISO 8601 datetime string",
  "participants": ["array", "of", "strings"],
  "note_url": "string",
  "granola_summary": "string",
  "transcript": "string"
}
```

### Response
- **Success (200)**: `{ "ok": true }`
- **Error (4xx/5xx)**: `{ "error": "error message" }`

## Setup

### 1. Install Dependencies
```bash
npm install
```

### 2. Environment Variables
Set the following environment variables in your Vercel project:

#### Snowflake Configuration
- `SNOWFLAKE_ACCOUNT` - Your Snowflake account identifier
- `SNOWFLAKE_USER` - Snowflake username
- `SNOWFLAKE_PASSWORD` - Snowflake password
- `SNOWFLAKE_WAREHOUSE` - Snowflake warehouse name
- `SNOWFLAKE_DATABASE` - Snowflake database name
- `SNOWFLAKE_SCHEMA` - Snowflake schema name
- `SNOWFLAKE_ROLE` - Snowflake role name

#### API Security
- `INGEST_API_KEY` - Secret key for API authentication

### 3. Deploy to Vercel
```bash
npm run deploy
```

## Local Development

```bash
npm run dev
```

## Database Schema

The API automatically creates a `MEETINGS` table with the following structure:

```sql
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
```

## Usage Examples

### cURL
```bash
curl -X POST https://your-vercel-app.vercel.app/api/ingest \
  -H "Authorization: Bearer your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "meeting_id": "meeting_123",
    "title": "Weekly Team Standup",
    "datetime": "2024-01-15T10:00:00Z",
    "participants": ["John Doe", "Jane Smith"],
    "note_url": "https://granola.com/notes/123",
    "granola_summary": "Team discussed Q1 goals and progress",
    "transcript": "John: Let's start with Q1 goals..."
  }'
```

### JavaScript/Fetch
```javascript
const response = await fetch('/api/ingest', {
  method: 'POST',
  headers: {
    'Authorization': 'Bearer your-api-key',
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    meeting_id: 'meeting_123',
    title: 'Weekly Team Standup',
    datetime: '2024-01-15T10:00:00Z',
    participants: ['John Doe', 'Jane Smith'],
    note_url: 'https://granola.com/notes/123',
    granola_summary: 'Team discussed Q1 goals and progress',
    transcript: 'John: Let\'s start with Q1 goals...'
  })
});

const result = await response.json();
```

## Error Handling

The API returns appropriate HTTP status codes:

- **400 Bad Request**: Invalid payload or missing fields
- **401 Unauthorized**: Invalid or missing API key
- **405 Method Not Allowed**: Non-POST requests
- **500 Internal Server Error**: Server or database errors

## Security Notes

- API key should be kept secret and rotated regularly
- Consider using Vercel's environment variable encryption
- Monitor API usage and implement rate limiting if needed
- Ensure Snowflake credentials have minimal required permissions

## Dependencies

- `snowflake-sdk`: Snowflake database connector
- `vercel`: Development and deployment tools

## License

MIT
