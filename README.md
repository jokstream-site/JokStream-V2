# JokStream V2

A Node.js/Express IPTV web app foundation with a dark English-language interface, Xtream/M3U configuration, HLS proxying, shared MPEG-TS upstream sessions, and a protected admin settings page.

## Requirements
- Node.js 20+
- An IPTV subscription you are authorized to access and redistribute

## Run locally
1. `npm install`
2. Copy `.env.example` to `.env` and fill in provider settings.
3. `npm start`
4. Open `http://localhost:3000`.

## Render deployment
Create a **Web Service** connected to this repository. Build command: `npm install`. Start command: `npm start`. Add secrets in Render Environment, never commit `.env` or provider credentials.

## Environment
For Xtream use `IPTV_PROVIDER=xtream`, `XTREAM_BASE_URL`, `XTREAM_USERNAME`, and `XTREAM_PASSWORD`. For M3U use `IPTV_PROVIDER=m3u` and `M3U_URL`. Optional: `TMDB_API_KEY`, `EPG_URL`, `VAST_TAG_URL`, `ADMIN_PASSWORD`, and `SESSION_SECRET`.

## Shared stream behaviour
- MPEG-TS: one upstream connection per canonical channel URL per Node process; connected viewers receive the same live byte stream.
- HLS: playlists are rewritten through the server and identical segment/key requests are coalesced and cached in bounded RAM. HLS is segment-based, so it cannot truthfully be described as one continuously open provider connection.
- When the last viewer leaves, TS upstream sessions close after `STREAM_IDLE_CLOSE_MS` (default 10 seconds). A returning viewer cancels the timer.
- This in-memory coordination is per process. Multiple Render instances each have their own cache and upstream session. Use a dedicated media gateway/shared cache if cross-instance single-origin sharing is required.

## API
- `GET /api/health`
- `GET /api/live`, `/api/movies`, `/api/series`
- `GET /api/stream/:id` (provider-aware proxy)
- `GET /api/admin/config` and `POST /api/admin/config` (HTTP Basic auth)

## Security notes
Use a strong `ADMIN_PASSWORD`; keep credentials only in environment variables. Do not expose this service as an unrestricted open proxy. Provider URLs are constructed server-side from configured provider details. Confirm your IPTV provider permits proxying and concurrent redistribution.

## Current scope
This is the initial V2 deployable foundation. TMDB enrichment, sports fixtures, VAST playback integration, and production-grade multi-instance coordination are not yet implemented; their environment placeholders are provided for the next integration phase.
