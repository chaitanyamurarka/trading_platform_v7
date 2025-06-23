import * as api from '../api.js';
import { state, constants } from './2-state.js';
import * as elements from './1-dom-elements.js';
import { showToast } from './4-ui-helpers.js';
import { connectToLiveFeed, disconnectFromAllLiveFeeds } from './9-websocket-service.js';
import { applyAutoscaling } from './5-chart-drawing.js';

/**
 * NEW: Main entry point for the application.
 * Initiates a session and then performs the initial data load.
 */
export async function startApplication() {
    try {
        const sessionData = await api.initiateSession();
        state.sessionToken = sessionData.session_token;
        showToast('Session started.', 'info');
        
        // Now that we have a session, load the initial chart data
        await loadChartData();

    } catch (error) {
        console.error('Failed to initiate session:', error);
        showToast('Could not start a session. Please reload.', 'error');
    }
}

/**
 * Loads or reloads chart data based on the current UI state.
 */
export async function loadChartData() {
    // This check is important to ensure startApplication has run first.
    if (!state.sessionToken) {
        showToast('Session not active. Cannot load data.', 'error');
        return;
    };
    if (state.currentlyFetching) return;

    state.resetAllData();
    disconnectFromAllLiveFeeds();

    state.currentlyFetching = true;
    elements.loadingIndicator.style.display = 'flex';

    try {
        const selectedInterval = elements.intervalSelect.value;
        const dataType = selectedInterval.includes('tick') ? 'tick' : elements.candleTypeSelect.value;
        
        const params = {
            session_token: state.sessionToken,
            exchange: elements.exchangeSelect.value,
            token: elements.symbolSelect.value,
            interval: selectedInterval,
            data_type: dataType,
            start_time: elements.startTimeInput.value,
            end_time: elements.endTimeInput.value,
            timezone: elements.timezoneSelect.value,
        };

        const responseData = await api.getHistoricalData(params);
        state.processInitialData(responseData, dataType);
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
    if (state.allDataLoaded || state.currentlyFetching || !state.requestId) {
        return;
    }

    state.currentlyFetching = true;
    elements.loadingIndicator.style.display = 'flex';

    try {
        const chunkData = await api.getHistoricalChunk(state.requestId, constants.DATA_CHUNK_SIZE);
        state.processChunkData(chunkData);
        showToast(`Loaded ${chunkData.candles.length} older bars`, 'success');
    } catch (error) {
        console.error("Failed to prepend data chunk:", error);
        showToast("Could not load older data.", 'error');
    } finally {
        state.currentlyFetching = false;
        elements.loadingIndicator.style.display = 'none';
    }
}