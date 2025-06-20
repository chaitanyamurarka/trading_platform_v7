// frontend/static/js/app/6-api-service.js
import { getHistoricalDataUrl, getHistoricalDataChunkUrl, getHeikinAshiDataUrl, getHeikinAshiDataChunkUrl, fetchHeikinAshiData, fetchHeikinAshiChunk } from '../api.js';
import { state, constants } from './2-state.js';
import * as elements from './1-dom-elements.js';
import { showToast } from './4-ui-helpers.js';
import { connectToLiveDataFeed, connectToLiveHeikinAshiData, disconnectFromAllLiveFeeds } from './9-websocket-service.js';

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
    // This function remains the same, but we will call it for ticks as well
    const isTickChart = state.candleType === 'tick';
    const requestId = isTickChart ? state.tickRequestId : state.chartRequestId;
    let currentOffset = isTickChart ? state.tickCurrentOffset : state.chartCurrentOffset;

    if (currentOffset <= 0) {
        if (isTickChart) state.allTickDataLoaded = true;
        else state.allDataLoaded = true;
        return;
    }

    const nextOffset = currentOffset - constants.DATA_CHUNK_SIZE;
    
    state.currentlyFetching = true;
    elements.loadingIndicator.style.display = 'flex';

    try {
        const chunkUrl = isTickChart 
            ? `/tick/chunk?request_id=${requestId}&offset=${nextOffset}&limit=${constants.DATA_CHUNK_SIZE}`
            : getHistoricalDataChunkUrl(requestId, nextOffset, constants.DATA_CHUNK_SIZE);
            
        const chunkData = await fetchHistoricalData(chunkUrl);

        if (chunkData && chunkData.candles.length > 0) {
            const chartFormattedData = chunkData.candles.map(c => ({ time: c.unix_timestamp, open: c.open, high: c.high, low: c.low, close: c.close }));
            const volumeFormattedData = chunkData.candles.map(c => ({ time: c.unix_timestamp, value: c.volume, color: c.close >= c.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' }));

            if (isTickChart) {
                state.allTickData = [...chartFormattedData, ...state.allTickData];
                state.allTickVolumeData = [...volumeFormattedData, ...state.allTickVolumeData];
                state.mainSeries.setData(state.allTickData);
                state.volumeSeries.setData(state.allTickVolumeData);
                state.tickCurrentOffset = chunkData.offset;
                if (state.tickCurrentOffset === 0) state.allTickDataLoaded = true;
            } else {
                state.allChartData = [...chartFormattedData, ...state.allChartData];
                state.allVolumeData = [...volumeFormattedData, ...state.allVolumeData];
                state.mainSeries.setData(state.allChartData);
                state.volumeSeries.setData(state.allVolumeData);
                state.chartCurrentOffset = chunkData.offset;
                if (state.chartCurrentOffset === 0) state.allDataLoaded = true;
            }
            showToast(`Loaded ${chunkData.candles.length} older bars`, 'success');
        } else {
            if (isTickChart) state.allTickDataLoaded = true;
            else state.allDataLoaded = true;
        }
    } catch (error) {
        console.error("Failed to prepend data chunk:", error);
        showToast("Could not load older data.", 'error');
    } finally {
        elements.loadingIndicator.style.display = 'none';
        state.currentlyFetching = false;
    }
}


export async function fetchAndPrependHeikinAshiChunk() {
    const nextOffset = state.heikinAshiCurrentOffset - constants.DATA_CHUNK_SIZE;
    if (nextOffset < 0) {
        state.allHeikinAshiDataLoaded = true;
        return;
    }

    state.currentlyFetching = true;
    elements.loadingIndicator.style.display = 'flex';

    try {
        const chunkData = await fetchHeikinAshiChunk(state.heikinAshiRequestId, nextOffset, constants.DATA_CHUNK_SIZE);
        if (chunkData && chunkData.candles.length > 0) {
            const chartFormattedData = chunkData.candles.map(c => ({ time: c.unix_timestamp, open: c.open, high: c.high, low: c.low, close: c.close }));
            const volumeFormattedData = chunkData.candles.map(c => ({ time: c.unix_timestamp, value: c.volume || 0, color: c.close >= c.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' }));

            state.allHeikinAshiData = [...chartFormattedData, ...state.allHeikinAshiData];
            state.allHeikinAshiVolumeData = [...volumeFormattedData, ...state.allHeikinAshiVolumeData];
            state.mainSeries.setData(state.allHeikinAshiData);
            state.volumeSeries.setData(state.allHeikinAshiVolumeData);
            state.heikinAshiCurrentOffset = chunkData.offset;

            if (state.heikinAshiCurrentOffset === 0) {
                state.allHeikinAshiDataLoaded = true;
            }
            showToast(`Loaded ${chunkData.candles.length} older Heikin Ashi candles`, 'success');
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


// Modified loadChartData to be more generic
export async function loadChartData() {
    if (!state.sessionToken) return;

    // Determine chart type based on selected interval
    const selectedInterval = elements.intervalSelect.value;
    if (selectedInterval.includes('tick')) {
        state.candleType = 'tick';
    } else {
        // Use the dedicated candle type selector for regular vs. heikin ashi
        state.candleType = elements.candleTypeSelect.value;
    }
    
    // Clear all data arrays and reset state
    state.resetAllData();
    disconnectFromAllLiveFeeds();

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

        if (state.candleType === 'tick') {
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

        // Connect to live feed if applicable (Note: live tick data is not implemented in this version)
        if (isLive) {
            if (state.candleType === 'heikin_ashi') {
                connectToLiveHeikinAshiData();
            } else if (state.candleType === 'regular') {
                connectToLiveDataFeed();
            } else {
                showToast("Live data is not available for tick-based charts.", "info");
            }
        }
    } catch (error) {
        console.error('Failed to fetch initial chart data:', error);
        showToast(`Error: ${error.message}`, 'error');
    } finally {
        elements.loadingIndicator.style.display = 'none';
        state.currentlyFetching = false;
    }
}