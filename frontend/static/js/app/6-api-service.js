// frontend/static/js/app/6-api-service.js
// REFACTORED to use the new unified API service

import * as api from '../api.js'; // Uses the new, simplified api.js
import { state, constants } from './2-state.js';
import * as elements from './1-dom-elements.js';
import { showToast } from './4-ui-helpers.js';
import { connectToLiveDataFeed, disconnectFromAllLiveFeeds } from './9-websocket-service.js';
import { applyAutoscaling } from './13-drawing-toolbar-listeners.js';

/**
 * Unified function to load the initial set of chart data, regardless of type.
 * This function replaces the previous separate logic for regular, Heikin Ashi, and tick data.
 */
export async function loadChartData() {
    if (!state.sessionToken) {
        showToast('Session not active. Please refresh.', 'error');
        return;
    }

    const selectedInterval = elements.intervalSelect.value;
    const isTickChart = selectedInterval.includes('tick');
    // Set the candleType in the global state, which will be used by other functions.
    state.candleType = isTickChart ? 'tick' : elements.candleTypeSelect.value;

    state.resetAllData();
    disconnectFromAllLiveFeeds();

    elements.loadingIndicator.style.display = 'flex';

    // A single parameters object is built for the unified API endpoint.
    const params = {
        sessionToken: state.sessionToken,
        exchange: elements.exchangeSelect.value,
        symbol: elements.symbolSelect.value,
        interval: selectedInterval,
        startTime: elements.startTimeInput.value,
        endTime: elements.endTimeInput.value,
        timezone: elements.timezoneSelect.value,
        dataType: state.candleType, // This key parameter tells the backend which data to return.
    };

    try {
        // A single, clean API call now handles all historical data types.
        const responseData = await api.getHistoricalData(params);

        if (!responseData || !responseData.candles || responseData.candles.length === 0) {
            showToast(responseData?.message || 'No data found for the selection.', 'warning');
            state.mainSeries.setData([]);
            state.volumeSeries.setData([]);
            return;
        }

        // The state manager processes the unified response from the backend.
        state.processInitialData(responseData, state.candleType);
        showToast(responseData.message, 'success');

        // If the live toggle is enabled, connect to the unified WebSocket.
        if (elements.liveToggle.checked) {
            connectToLiveDataFeed(); // This function in websocket-service.js is also simplified.
        }

    } catch (error) {
        console.error('Failed to fetch initial chart data:', error);
        showToast(`Error: ${error.message}`, 'error');
    } finally {
        elements.loadingIndicator.style.display = 'none';
        state.currentlyFetching = false;
        applyAutoscaling();
    }
}

/**
 * Unified function to fetch and prepend older data chunks for infinite scroll.
 * This single function replaces the previous `fetchAndPrependTickChunk` and `fetchAndPrependHeikinAshiChunk`.
 */
export async function fetchAndPrependChunk() {
    if (state.allDataLoadedForCurrentView || state.currentlyFetching || !state.requestId) {
        return;
    }

    state.currentlyFetching = true;
    elements.loadingIndicator.style.display = 'flex';

    try {
        // A single API call fetches the next chunk, using the unified endpoint.
        const chunkData = await api.getHistoricalDataChunk(
            state.requestId,
            state.candleType, // Pass the current data type
            constants.DATA_CHUNK_SIZE
        );

        if (chunkData && chunkData.candles.length > 0) {
            state.prependDataChunk(chunkData); // State manager prepends the new data.
            state.requestId = chunkData.request_id; // Update request ID for the next chunk.

            if (!chunkData.is_partial) {
                state.allDataLoadedForCurrentView = true;
                showToast('Loaded all available historical data.', 'info');
            } else {
                showToast(`Loaded ${chunkData.candles.length} older bars.`, 'success');
            }
        } else {
            state.allDataLoadedForCurrentView = true;
        }
    } catch (error) {
        console.error("Failed to prepend data chunk:", error);
        showToast("Could not load older data.", 'error');
    } finally {
        elements.loadingIndicator.style.display = 'none';
        state.currentlyFetching = false;
    }
}