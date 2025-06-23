import * as api from '../api.js';
import { state } from './2-state.js';
import * as elements from './1-dom-elements.js';
import { showToast } from './4-ui-helpers.js';
import { applyAutoscaling } from './5-chart-drawing.js';

let liveDataSocket = null;

/**
 * Main function to load or reload chart data based on the current UI state.
 */
export async function loadChartData() {
    if (!state.sessionToken || state.currentlyFetching) return;

    state.resetAllData();
    disconnectFromLiveFeed();
    state.currentlyFetching = true;
    elements.loadingIndicator.style.display = 'flex';

    try {
        const params = {
            session_token: state.sessionToken,
            exchange: elements.exchangeSelect.value,
            token: elements.symbolSelect.value,
            interval: elements.intervalSelect.value,
            data_type: elements.candleTypeSelect.value,
            start_time: elements.startTimeInput.value,
            end_time: elements.endTimeInput.value,
            timezone: elements.timezoneSelect.value,
        };

        const responseData = await api.getHistoricalData(params);
        state.processInitialData(responseData, params.data_type);
        showToast(responseData.message, 'success');

        if (elements.liveToggle.checked) {
            connectToLiveFeed();
        }

    } catch (error) {
        console.error('Failed to load chart data:', error);
        showToast(`Error: ${error.message}`, 'error');
    } finally {
        state.currentlyFetching = false;
        elements.loadingIndicator.style.display = 'none';
        applyAutoscaling();
    }
}

/**
 * Fetches the next chunk of historical data for infinite scroll.
 */
export async function fetchAndPrependChunk() {
    if (state.isAllDataLoaded() || state.currentlyFetching || !state.getCurrentRequestId()) {
        return;
    }

    state.currentlyFetching = true;
    elements.loadingIndicator.style.display = 'flex';

    try {
        const chunkData = await api.getHistoricalChunk(state.getCurrentRequestId());
        state.processChunkData(chunkData);
        showToast(`Loaded ${chunkData.candles.length} older bars`, 'success');
    } catch (error) {
        console.error("Failed to prepend data chunk:", error);
        showToast("Could not load older data.", 'error');
    } finally {
        elements.loadingIndicator.style.display = 'none';
        state.currentlyFetching = false;
    }
}

/**
 * Connects to the unified live data WebSocket.
 */
export function connectToLiveFeed() {
    disconnectFromLiveFeed();
    const params = {
        dataType: elements.candleTypeSelect.value,
        symbol: elements.symbolSelect.value,
        interval: elements.intervalSelect.value,
        timezone: elements.timezoneSelect.value,
    };

    const onOpen = () => showToast(`Live feed connected for ${params.symbol}!`, 'success');
    const onMessage = (data) => handleLiveUpdate(data);
    const onClose = () => console.log('Live data WebSocket closed.');

    liveDataSocket = api.createLiveDataConnection(params, onMessage, onOpen, onClose);
}

/**
 * Disconnects from the current live data feed.
 */
export function disconnectFromLiveFeed() {
    if (liveDataSocket) {
        liveDataSocket.onclose = null; // Prevent onclose handler from firing on manual close
        liveDataSocket.close();
        liveDataSocket = null;
    }
}

/**
 * Handles incoming live data messages from the WebSocket.
 * @param {object} data - The message payload from the server.
 */
function handleLiveUpdate(data) {
    if (Array.isArray(data)) { // Handle backfill data
        state.processBackfillData(data);
    } else { // Handle live update
        state.processLiveUpdate(data);
    }
    // Auto-scroll logic can be placed here if desired
    const autoScaleBtn = document.getElementById('scaling-auto-btn');
    if (elements.liveToggle.checked && autoScaleBtn?.classList.contains('btn-active')) {
        state.chart.timeScale().scrollToRealTime();
    }
}

/**
 * Starts the user session and fetches initial data.
 */
export async function startSession() {
    try {
        const sessionData = await api.initiateSession();
        state.sessionToken = sessionData.session_token;
        showToast('Session started.', 'info');
        loadChartData(); // Initial data load
    } catch (error) {
        showToast('Could not start a session. Please reload.', 'error');
    }
}