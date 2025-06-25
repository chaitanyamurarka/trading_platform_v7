// frontend/static/js/app/6-api-service.js
import { getHistoricalDataUrl, getHistoricalDataChunkUrl, getHeikinAshiDataUrl, getHeikinAshiDataChunkUrl, fetchHeikinAshiData, fetchHeikinAshiChunk } from '../api.js';
import { state, constants } from './2-state.js';
import * as elements from './1-dom-elements.js';
import { showToast } from './4-ui-helpers.js';
import { connectToLiveDataFeed, connectToLiveHeikinAshiData, disconnectFromAllLiveFeeds, processMessageBuffer } from './9-websocket-service.js';
import { applyAutoscaling } from './13-drawing-toolbar-listeners.js';

// NEW: Function to get the URL for the new tick endpoint
function getTickDataUrl(sessionToken, exchange, token, interval, startTime, endTime, timezone) {
    const params = new URLSearchParams({
        session_token: sessionToken,
        exchange: exchange,
        token: token,
        interval: interval,
        start_time: startTime,
        end_time: endTime,
        timezone: timezone
    });
    // Note the new endpoint path `/tick/`
    return `/tick/?${params.toString()}`;
}

// NEW: Function to fetch the initial set of tick data
async function fetchInitialTickData(sessionToken, exchange, token, interval, startTime, endTime, timezone) {
    const url = getTickDataUrl(sessionToken, exchange, token, interval, startTime, endTime, timezone);
    const response = await fetch(url);
    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: 'Network error' }));
        throw new Error(error.detail);
    }
    return response.json();
}

// Re-using existing function for fetching historical data
async function fetchHistoricalData(url) {
    const response = await fetch(url);
    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: 'Network error' }));
        throw new Error(error.detail);
    }
    return response.json();
}

export async function fetchAndPrependRegularCandleChunk() {
    // REFACTORED to use cursor-based pagination
    if (state.allDataLoaded || state.currentlyFetching || !state.chartRequestId) {
        return;
    }

    state.currentlyFetching = true;
    elements.loadingIndicator.style.display = 'flex';

    try {
        // Use the request_id as a cursor. The offset parameter is now ignored.
        const chunkUrl = getHistoricalDataChunkUrl(state.chartRequestId, 0, constants.DATA_CHUNK_SIZE);
        const chunkData = await fetchHistoricalData(chunkUrl);

        if (chunkData && chunkData.candles.length > 0) {
            const chartFormattedData = chunkData.candles.map(c => ({ time: c.unix_timestamp, open: c.open, high: c.high, low: c.low, close: c.close }));
            const volumeFormattedData = chunkData.candles.map(c => ({ time: c.unix_timestamp, value: c.volume, color: c.close >= c.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' }));

            state.allChartData = [...chartFormattedData, ...state.allChartData];
            state.allVolumeData = [...volumeFormattedData, ...state.allVolumeData];

            state.mainSeries.setData(state.allChartData);
            state.volumeSeries.setData(state.allVolumeData);

            // Update the cursor (request_id) for the next fetch
            state.chartRequestId = chunkData.request_id;

            // Use the 'is_partial' flag to determine if we've reached the end
            if (!chunkData.is_partial || !chunkData.request_id) {
                state.allDataLoaded = true;
                showToast('Loaded all available historical data.', 'info');
            } else {
                showToast(`Loaded ${chunkData.candles.length} older candles`, 'success');
            }
        } else {
            state.allDataLoaded = true;
        }
    } catch (error) {
        console.error("Failed to prepend regular data chunk:", error);
        showToast("Could not load older historical data.", 'error');
    } finally {
        elements.loadingIndicator.style.display = 'none';
        state.currentlyFetching = false;
    }
}


export async function fetchAndPrependHeikinAshiChunk() {
    // REFACTORED to use cursor-based pagination
    if (state.allHeikinAshiDataLoaded || state.currentlyFetching || !state.heikinAshiRequestId) {
        return;
    }

    state.currentlyFetching = true;
    elements.loadingIndicator.style.display = 'flex';

    try {
        // Use the request_id as a cursor. The offset parameter is now ignored.
        const chunkData = await fetchHeikinAshiChunk(state.heikinAshiRequestId, 0, constants.DATA_CHUNK_SIZE); 
        
        if (chunkData && chunkData.candles.length > 0) {
            const chartFormattedData = chunkData.candles.map(c => ({ time: c.unix_timestamp, open: c.open, high: c.high, low: c.low, close: c.close }));
            const volumeFormattedData = chunkData.candles.map(c => ({ time: c.unix_timestamp, value: c.volume || 0, color: c.close >= c.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' }));

            state.allHeikinAshiData = [...chartFormattedData, ...state.allHeikinAshiData];
            state.allHeikinAshiVolumeData = [...volumeFormattedData, ...state.allHeikinAshiVolumeData];
            state.mainSeries.setData(state.allHeikinAshiData);
            state.volumeSeries.setData(state.allHeikinAshiVolumeData);
            
            // Update the cursor (request_id) for the next fetch
            state.heikinAshiRequestId = chunkData.request_id;

            // Use the 'is_partial' flag to determine if we've reached the end
            if (!chunkData.is_partial || !chunkData.request_id) {
                state.allHeikinAshiDataLoaded = true;
                showToast('Loaded all available Heikin Ashi data.', 'info');
            } else {
                showToast(`Loaded ${chunkData.candles.length} older Heikin Ashi candles`, 'success');
            }
        } else {
            state.allHeikinAshiDataLoaded = true;
        }
    } catch (error) {
        console.error("Failed to prepend Heikin Ashi data chunk:", error);
        showToast("Could not load older Heikin Ashi data.", 'error');
    } finally {
        elements.loadingIndicator.style.display = 'none';
        state.currentlyFetching = false;
    }
}

async function fetchTickDataChunk(cursor) {
    // The new endpoint doesn't need offset, just the cursor which is stored in request_id
    const url = `/tick/chunk?request_id=${encodeURIComponent(cursor)}&limit=${constants.DATA_CHUNK_SIZE}`;
    const response = await fetch(url);
    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: 'Network error' }));
        throw new Error(error.detail);
    }
    return response.json();
}

export async function fetchAndPrependTickChunk() {
    if (state.allTickDataLoaded || state.currentlyFetching || !state.tickRequestId) {
        return;
    }

    state.currentlyFetching = true;
    elements.loadingIndicator.style.display = 'flex';

    try {
        // Fetch the next chunk using the stored cursor (which is in tickRequestId)
        const chunkData = await fetchTickDataChunk(state.tickRequestId);

        if (chunkData && chunkData.candles.length > 0) {
            const chartFormattedData = chunkData.candles.map(c => ({ time: c.unix_timestamp, open: c.open, high: c.high, low: c.low, close: c.close }));
            const volumeFormattedData = chunkData.candles.map(c => ({ time: c.unix_timestamp, value: c.volume, color: c.close >= c.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' }));

            state.allTickData = [...chartFormattedData, ...state.allTickData];
            state.allTickVolumeData = [...volumeFormattedData, ...state.allTickVolumeData];
            
            // This is important: re-apply the full dataset to the chart
            state.mainSeries.setData(state.allTickData);
            state.volumeSeries.setData(state.allTickVolumeData);

            // Update the cursor for the next request
            state.tickRequestId = chunkData.request_id;

            // Check if this was the last page
            if (!chunkData.is_partial || !chunkData.request_id) {
                state.allTickDataLoaded = true;
                showToast('Loaded all available tick data.', 'info');
            } else {
                showToast(`Loaded ${chunkData.candles.length} older tick bars`, 'success');
            }
        } else {
            // No more data to load
            state.allTickDataLoaded = true;
        }
    } catch (error) {
        console.error("Failed to prepend tick data chunk:", error);
        showToast("Could not load older tick data.", 'error');
    } finally {
        elements.loadingIndicator.style.display = 'none';
        state.currentlyFetching = false;
    }
}

export async function loadChartData() {
    if (!state.sessionToken) return;

    // This part is important for determining which data to load
    const selectedInterval = elements.intervalSelect.value;
    const isTickChart = selectedInterval.includes('tick');
    state.candleType = isTickChart ? 'tick' : elements.candleTypeSelect.value;
    
    // Reset all data states
    state.resetAllData();
    disconnectFromAllLiveFeeds();

    state.isLoadingHistoricalData = true; // Set flag to true
    elements.loadingIndicator.style.display = 'flex';
    
    try {
        let responseData;
        const isLive = elements.liveToggle.checked;
        const args = [
            state.sessionToken, 
            elements.exchangeSelect.value, 
            elements.symbolSelect.value, 
            selectedInterval, 
            elements.startTimeInput.value, 
            elements.endTimeInput.value, 
            elements.timezoneSelect.value
        ];

        // Fetch initial data based on type
        if (isTickChart) {
            // For ticks, we use the new endpoint structure.
            const url = getTickDataUrl(...args); 
            responseData = await fetchInitialTickData(...args);
        } else if (state.candleType === 'heikin_ashi') {
            responseData = await fetchHeikinAshiData(...args);
        } else {
            const url = getHistoricalDataUrl(...args);
            responseData = await fetchHistoricalData(url);
        }

        if (!responseData || !responseData.candles || responseData.candles.length === 0) {
            showToast(responseData?.message || 'No data found for the selection.', 'warning');
            state.mainSeries.setData([]);
            state.volumeSeries.setData([]);
            return;
        }

        // Process and display the data
        state.processInitialData(responseData, state.candleType);
        showToast(responseData.message, 'success');
        
        // Connect to the appropriate live feed if the toggle is enabled
        if (isLive) {
            if (state.candleType === 'heikin_ashi') {
                connectToLiveHeikinAshiData();
            } else if (state.candleType === 'regular' || state.candleType === 'tick') {
                // The /ws/live endpoint now handles both regular and tick intervals
                connectToLiveDataFeed();
            }
        }
    } catch (error) {
        console.error('Failed to fetch initial chart data:', error);
        showToast(`Error: ${error.message}`, 'error');
    } finally {
        state.isLoadingHistoricalData = false;
        processMessageBuffer();
        elements.loadingIndicator.style.display = 'none';
        state.currentlyFetching = false;
        applyAutoscaling();
    }
}