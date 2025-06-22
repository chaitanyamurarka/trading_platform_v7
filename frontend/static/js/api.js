/**
 * @file api.js
 * @description Manages all API communication with the backend server using the new unified endpoints.
 */

// Construct the base URL for all API requests.
const API_BASE_URL = `${window.location.protocol}//${window.location.hostname}:8000`;

// --- Unified Historical Data ---

/**
 * Fetches historical data from the unified /data/historical endpoint.
 * @param {object} params - The request parameters, including the new 'dataType'.
 * @returns {Promise<Object>} A promise that resolves to the unified data response.
 */
export async function getHistoricalData(params) {
    const query = new URLSearchParams({
        session_token: params.sessionToken,
        exchange: params.exchange,
        token: params.symbol,
        interval: params.interval,
        start_time: params.startTime,
        end_time: params.endTime,
        timezone: params.timezone,
        data_type: params.dataType,
    });
    const response = await fetch(`${API_BASE_URL}/data/historical?${query.toString()}`);
    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: 'Network error' }));
        throw new Error(error.detail);
    }
    return response.json();
}

/**
 * Fetches a subsequent chunk of historical data from the unified /data/historical/chunk endpoint.
 * @param {string} requestId - The ID from the initial data response.
 * @param {string} dataType - The type of data to fetch ('regular', 'heikin_ashi', 'tick').
 * @param {number} limit - The number of data points to fetch.
 * @returns {Promise<Object>} A promise that resolves to the data chunk.
 */
export async function getHistoricalDataChunk(requestId, dataType, limit) {
    const query = new URLSearchParams({
        request_id: requestId,
        data_type: dataType,
        limit: limit,
    });
    const response = await fetch(`${API_BASE_URL}/data/historical/chunk?${query.toString()}`);
    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: 'Network error' }));
        throw new Error(error.detail);
    }
    return response.json();
}

// --- Unified Session Management ---

/**
 * Initiates a new user session with the /data/session endpoint.
 */
export async function initiateSession() {
    const response = await fetch(`${API_BASE_URL}/data/session`);
    if (!response.ok) throw new Error('Failed to initiate session');
    return response.json();
}

/**
 * ADDED: Sends a periodic heartbeat to the backend to keep the current session alive.
 * @param {string} token - The user's current session token.
 * @returns {Promise<Object>} A promise that resolves to the heartbeat status message.
 */
export async function sendHeartbeat(token) {
    const response = await fetch(`${API_BASE_URL}/data/session/heartbeat`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ session_token: token }),
    });
    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: 'Session heartbeat failed' }));
        throw new Error(error.detail);
    }
    return response.json();
}