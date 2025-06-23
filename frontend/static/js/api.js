/**
 * @file api.js
 * @description Manages all API communication with the backend's new unified endpoints.
 */

const API_BASE_URL = `${window.location.protocol}//${window.location.hostname}:8000`;

/**
 * Performs a fetch request and handles common error scenarios.
 * @param {string} url - The URL to fetch.
 * @param {object} options - Fetch options.
 * @returns {Promise<any>} The JSON response from the API.
 */
async function apiFetch(url, options = {}) {
    try {
        const response = await fetch(url, options);
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: response.statusText }));
            throw new Error(errorData.detail || 'An unknown API error occurred');
        }
        return response.json();
    } catch (error) {
        console.error('API Fetch Error:', error);
        throw error;
    }
}

/**
 * Initiates a new user session.
 * @returns {Promise<{session_token: string}>}
 */
export function initiateSession() {
    return apiFetch(`${API_BASE_URL}/data/session`);
}

/**
 * Fetches historical data for any supported data type.
 * @param {object} params - The parameters for the data request.
 * @returns {Promise<object>} The initial data response with candles and a cursor.
 */
export function getHistoricalData(params) {
    const query = new URLSearchParams(params);
    return apiFetch(`${API_BASE_URL}/data/historical?${query.toString()}`);
}

/**
 * Fetches a subsequent chunk of historical data using a cursor.
 * @param {string} requestId - The cursor from the previous response.
 * @param {number} limit - The number of candles to fetch.
 * @returns {Promise<object>} The data chunk response.
 */
export function getHistoricalChunk(requestId, limit = 5000) {
    const query = new URLSearchParams({ request_id: requestId, limit });
    return apiFetch(`${API_BASE_URL}/data/historical/chunk?${query.toString()}`);
}

/**
 * Creates and manages a WebSocket connection for live data.
 * @param {object} params - Connection parameters { dataType, symbol, interval, timezone }.
 * @param {function} onMessage - Callback function for incoming messages.
 * @param {function} onOpen - Callback for when the connection opens.
 * @param {function} onClose - Callback for when the connection closes.
 * @returns {WebSocket} The WebSocket instance.
 */
export function createLiveDataConnection({ dataType, symbol, interval, timezone }, onMessage, onOpen, onClose) {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsURL = `${wsProtocol}//${window.location.host}/data/live/${dataType}/${symbol}/${interval}/${timezone}`;
    
    const ws = new WebSocket(wsURL);
    ws.onmessage = (event) => onMessage(JSON.parse(event.data));
    ws.onopen = onOpen;
    ws.onclose = onClose;
    ws.onerror = (error) => console.error('WebSocket Error:', error);

    return ws;
}