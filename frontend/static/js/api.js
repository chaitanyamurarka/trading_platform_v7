/**
 * @file api.js
 * @description Manages all API communication with the backend server.
 * This module abstracts the details of HTTP requests using the Fetch API,
 * providing dedicated functions for session management and data retrieval.
 */

// --- API Configuration ---

// Dynamically determine the protocol and hostname for the API endpoint based on the current window location.
let API_PROTOCOL = window.location.protocol;
let API_HOSTNAME = window.location.hostname;
const API_PORT = '8000'; // The port where the FastAPI backend is expected to be running.

// If the frontend is opened directly from the filesystem (`file:` protocol),
// it lacks a hostname. We must fall back to 'localhost' for API calls to work in this common development scenario.
if (API_PROTOCOL === 'file:') {
    API_PROTOCOL = 'http:';
    API_HOSTNAME = 'localhost'; // Or '127.0.0.1'
    console.warn('Frontend is opened as a local file. API calls will be directed to http://localhost:8000.');
}

// The final, constructed base URL for all API requests.
const API_BASE_URL = `${API_PROTOCOL}//${API_HOSTNAME}:${API_PORT}`;

// --- Session Management Functions ---

/**
 * Initiates a new user session with the backend. This is the first call made
 * to ensure all subsequent data requests are associated with a unique session.
 * @returns {Promise<Object>} A promise that resolves to the session data, containing a unique `session_token`.
 * @example
 * // returns { session_token: "some-unique-string" }
 */
export function initiateSession() {
    // Uses fetch to make a GET request and parses the JSON response.
    return fetch(`${API_BASE_URL}/utils/session/initiate`).then(res => res.json());
}

/**
 * Sends a periodic heartbeat to the backend to keep the current session alive.
 * This prevents the backend from cleaning up session-specific cached data (like historical data requests)
 * that might still be needed by the client.
 * @param {string} token - The user's current session token obtained from `initiateSession`.
 * @returns {Promise<Object>} A promise that resolves to the heartbeat status message.
 */
export function sendHeartbeat(token) {
    // Uses fetch to make a POST request with the session token in the body.
    return fetch(`${API_BASE_URL}/utils/session/heartbeat`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ session_token: token }),
    }).then(res => res.json());
}


// --- Historical Data Functions ---

/**
 * Constructs the full URL for fetching the initial set of historical candle data.
 * The parameters are encoded into the URL's query string.
 * @param {string} sessionToken - The unique session token for the user.
 * @param {string} exchange - The exchange name (e.g., "NASDAQ").
 * @param {string} token - The asset symbol/token (e.g., "AAPL").
 * @param {string} interval - The data interval (e.g., "1m", "1d").
 * @param {string} startTime - The start time in ISO format (e.g., "2023-01-01T00:00").
 * @param {string} endTime - The end time in ISO format.
 * @param {string} timezone - The target timezone for the data (e.g., "America/New_York").
 * @returns {string} The complete, URL-encoded API endpoint for the initial historical data request.
 */
export function getHistoricalDataUrl(sessionToken, exchange, token, interval, startTime, endTime, timezone) {
    const params = new URLSearchParams({
        session_token: sessionToken,
        exchange: exchange,
        token: token,
        interval: interval,
        start_time: startTime,
        end_time: endTime,
        timezone: timezone
    });
    return `${API_BASE_URL}/historical/?${params.toString()}`;
}

/**
 * Constructs the URL for fetching a subsequent chunk of historical data for pagination (infinite scroll).
 * This allows for lazily loading older data as the user scrolls back in time on the chart.
 * @param {string} requestId - The unique ID for the data request session, obtained from the initial data fetch response.
 * @param {number} offset - The starting index (offset) of the data to fetch. This is used by the backend to find the correct data chunk.
 * @param {number} [limit=5000] - The number of data points (candles) to fetch in this chunk.
 * @returns {string} The full API URL for fetching a subsequent data chunk.
 */
export function getHistoricalDataChunkUrl(requestId, offset, limit = 5000) {
    const params = new URLSearchParams({
        request_id: requestId,
        offset: offset,
        limit: limit
    });
    return `${API_BASE_URL}/historical/chunk?${params.toString()}`;
}

// ===== 1. Update frontend/static/js/api.js - Add Heikin Ashi API functions =====

/**
 * Constructs the URL for fetching Heikin Ashi candle data.
 * @param {string} sessionToken - The unique session token for the user.
 * @param {string} exchange - The exchange name (e.g., "NASDAQ").
 * @param {string} token - The asset symbol/token (e.g., "AAPL").
 * @param {string} interval - The data interval (e.g., "1m", "1d").
 * @param {string} startTime - The start time in ISO format.
 * @param {string} endTime - The end time in ISO format.
 * @param {string} timezone - The target timezone for the data.
 * @returns {string} The complete API endpoint for Heikin Ashi data.
 */
export function getHeikinAshiDataUrl(sessionToken, exchange, token, interval, startTime, endTime, timezone) {
    const params = new URLSearchParams({
        session_token: sessionToken,
        exchange: exchange,
        token: token,
        interval: interval,
        start_time: startTime,
        end_time: endTime,
        timezone: timezone
    });
    return `${API_BASE_URL}/heikin-ashi/?${params.toString()}`;
}

/**
 * Fetches Heikin Ashi data from the backend.
 * @param {string} sessionToken - Session token
 * @param {string} exchange - Exchange name
 * @param {string} token - Symbol
 * @param {string} interval - Time interval
 * @param {string} startTime - Start time
 * @param {string} endTime - End time
 * @param {string} timezone - Timezone
 * @returns {Promise<Object>} Promise resolving to Heikin Ashi data
 */
export async function fetchHeikinAshiData(sessionToken, exchange, token, interval, startTime, endTime, timezone) {
    const url = getHeikinAshiDataUrl(sessionToken, exchange, token, interval, startTime, endTime, timezone);
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
}

/**
 * Constructs the URL for fetching a subsequent chunk of Heikin Ashi data for pagination (infinite scroll).
 * @param {string} requestId - The unique ID for the Heikin Ashi data request session
 * @param {number} offset - The starting index (offset) of the data to fetch
 * @param {number} [limit=5000] - The number of Heikin Ashi candles to fetch in this chunk
 * @returns {string} The full API URL for fetching a subsequent Heikin Ashi data chunk
 */
export function getHeikinAshiDataChunkUrl(requestId, offset, limit = 5000) {
    const params = new URLSearchParams({
        request_id: requestId,
        offset: offset,
        limit: limit
    });
    return `${API_BASE_URL}/heikin-ashi/chunk?${params.toString()}`;
}

/**
 * Fetches a chunk of Heikin Ashi data from the backend for pagination.
 * @param {string} requestId - Request ID from initial Heikin Ashi data fetch
 * @param {number} offset - Starting offset for the chunk
 * @param {number} [limit=5000] - Number of candles to fetch
 * @returns {Promise<Object>} Promise resolving to Heikin Ashi chunk data
 */
export async function fetchHeikinAshiChunk(requestId, offset, limit = 5000) {
    const url = getHeikinAshiDataChunkUrl(requestId, offset, limit);
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
}