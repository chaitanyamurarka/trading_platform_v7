// frontend/static/js/app/simplified-app-service.js
import { state } from './2-state.js';
import * as api from '../api.js';
import { showToast } from './4-ui-helpers.js';
import { loadChartData as refreshChart } from './6-api-service.js';

let liveDataSocket = null;

// Centralized function to load all historical data
export async function loadHistoricalData() {
    if (!state.sessionToken) {
        showToast('Session not initialized.', 'error');
        return;
    }

    const params = {
        sessionToken: state.sessionToken,
        exchange: document.getElementById('exchange').value,
        symbol: document.getElementById('symbol').value,
        interval: document.getElementById('interval').value,
        dataType: document.getElementById('interval').value.includes('tick') ? 'tick' : document.getElementById('candle-type-select').value,
        startTime: document.getElementById('start_time').value,
        endTime: document.getElementById('end_time').value,
        timezone: document.getElementById('timezone').value,
    };

    try {
        const response = await api.getHistoricalData(params); // Uses the new unified API call
        state.processInitialData(response, params.dataType); // State manager processes the unified response
        showToast(response.message, 'success');
    } catch (error) {
        showToast(`Error loading data: ${error.message}`, 'error');
    }
}

// Centralized function to manage live data connection
export function connectToLiveDataFeed() {
    if (liveDataSocket) {
        liveDataSocket.close();
    }

    const params = {
        symbol: document.getElementById('symbol').value,
        interval: document.getElementById('interval').value,
        dataType: document.getElementById('interval').value.includes('tick') ? 'tick' : document.getElementById('candle-type-select').value,
        timezone: document.getElementById('timezone').value,
    };

    liveDataSocket = api.createLiveDataConnection(params, (data) => {
        // Logic to handle incoming unified live data messages
        handleLiveUpdate(data);
    });
}

export function disconnectFromLiveDataFeed() {
    if (liveDataSocket) {
        liveDataSocket.close();
        liveDataSocket = null;
    }
}