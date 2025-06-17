// frontend/static/js/app/6-api-service.js
import { getHistoricalDataUrl, getHistoricalDataChunkUrl, getHeikinAshiDataUrl, getHeikinAshiDataChunkUrl, fetchHeikinAshiData, fetchHeikinAshiChunk } from '../api.js';
import { state, constants } from './2-state.js';
import * as elements from './1-dom-elements.js';
import { showToast } from './4-ui-helpers.js';
import { connectToLiveDataFeed, connectToLiveHeikinAshiData, disconnectFromAllLiveFeeds } from './9-websocket-service.js';

async function fetchInitialHistoricalData(sessionToken, exchange, token, interval, startTime, endTime, timezone) {
    const url = getHistoricalDataUrl(sessionToken, exchange, token, interval, startTime, endTime, timezone);
    const response = await fetch(url);
    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: 'Network error' }));
        throw new Error(error.detail);
    }
    return response.json();
}

async function fetchHistoricalChunk(requestId, offset, limit) {
    const url = getHistoricalDataChunkUrl(requestId, offset, limit);
    const response = await fetch(url);
    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: 'Network error' }));
        throw new Error(error.detail);
    }
    return response.json();
}

export async function fetchAndPrependRegularCandleChunk() {
    const nextOffset = state.chartCurrentOffset - constants.DATA_CHUNK_SIZE;
    if (nextOffset < 0) {
        state.allDataLoaded = true;
        return;
    }

    state.currentlyFetching = true;
    elements.loadingIndicator.style.display = 'flex';

    try {
        const chunkData = await fetchHistoricalChunk(state.chartRequestId, nextOffset, constants.DATA_CHUNK_SIZE);
        if (chunkData && chunkData.candles.length > 0) {
            const chartFormattedData = chunkData.candles.map(c => ({ time: c.unix_timestamp, open: c.open, high: c.high, low: c.low, close: c.close }));
            const volumeFormattedData = chunkData.candles.map(c => ({ time: c.unix_timestamp, value: c.volume, color: c.close >= c.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' }));

            state.allChartData = [...chartFormattedData, ...state.allChartData];
            state.allVolumeData = [...volumeFormattedData, ...state.allVolumeData];
            state.mainSeries.setData(state.allChartData);
            state.volumeSeries.setData(state.allVolumeData);
            state.chartCurrentOffset = chunkData.offset;

            if (state.chartCurrentOffset === 0) {
                state.allDataLoaded = true;
            }

            showToast(`Loaded ${chunkData.candles.length} older candles`, 'success');
        } else {
            state.allDataLoaded = true;
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


export async function loadInitialChart() {
    state.currentlyFetching = true;
    elements.loadingIndicator.style.display = 'flex';
    state.allDataLoaded = false;

    try {
        const responseData = await fetchInitialHistoricalData(state.sessionToken, elements.exchangeSelect.value, elements.symbolSelect.value, elements.intervalSelect.value, elements.startTimeInput.value, elements.endTimeInput.value, elements.timezoneSelect.value);
        if (!responseData || !responseData.request_id || responseData.candles.length === 0) {
            showToast(responseData.message || 'No historical data found.', 'warning');
            if (state.mainSeries) state.mainSeries.setData([]);
            if (state.volumeSeries) state.volumeSeries.setData([]);
            return;
        }
        state.chartRequestId = responseData.request_id;
        state.chartCurrentOffset = responseData.offset;
        if (state.chartCurrentOffset === 0 && !responseData.is_partial) state.allDataLoaded = true;

        state.allChartData = responseData.candles.map(c => ({ time: c.unix_timestamp, open: c.open, high: c.high, low: c.low, close: c.close }));
        state.allVolumeData = responseData.candles.map(c => ({ time: c.unix_timestamp, value: c.volume, color: c.close >= c.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' }));

        state.mainSeries.setData(state.allChartData);
        state.volumeSeries.setData(state.allVolumeData);

        if (state.allChartData.length > 0) {
            const dataSize = state.allChartData.length;
            state.mainChart.timeScale().setVisibleLogicalRange({ from: Math.max(0, dataSize - 100), to: dataSize - 1 });
        } else {
            state.mainChart.timeScale().fitContent();
        }

        state.mainChart.priceScale().applyOptions({ autoscale: true });
    } catch (error) {
        console.error('Failed to fetch initial chart data:', error);
        showToast(`Error: ${error.message}`, 'error');
    } finally {
        elements.loadingIndicator.style.display = 'none';
        state.currentlyFetching = false;
    }
}


export async function loadHeikinAshiChart() {
    state.currentlyFetching = true;
    elements.loadingIndicator.style.display = 'flex';

    try {
        const responseData = await fetchHeikinAshiData(state.sessionToken, elements.exchangeSelect.value, elements.symbolSelect.value, elements.intervalSelect.value, elements.startTimeInput.value, elements.endTimeInput.value, elements.timezoneSelect.value);

        if (!responseData || !responseData.candles || responseData.candles.length === 0) {
            showToast(responseData?.message || 'No Heikin Ashi data found.', 'warning');
            if (state.mainSeries) state.mainSeries.setData([]);
            if (state.volumeSeries) state.volumeSeries.setData([]);
            return;
        }

        state.heikinAshiRequestId = responseData.request_id;
        state.heikinAshiCurrentOffset = responseData.offset || 0;
        state.allHeikinAshiDataLoaded = !responseData.is_partial;

        state.allHeikinAshiData = responseData.candles.map(c => ({ time: c.unix_timestamp, open: c.open, high: c.high, low: c.low, close: c.close }));
        state.allHeikinAshiVolumeData = responseData.candles.map(c => ({ time: c.unix_timestamp, value: c.volume || 0, color: c.close >= c.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' }));

        state.mainSeries.setData(state.allHeikinAshiData);
        state.volumeSeries.setData(state.allHeikinAshiVolumeData);

        if (state.allHeikinAshiData.length > 0) {
            const dataSize = state.allHeikinAshiData.length;
            state.mainChart.timeScale().setVisibleLogicalRange({ from: Math.max(0, dataSize - 100), to: dataSize - 1 });
        }

        state.mainChart.priceScale().applyOptions({ autoscale: true });

        showToast(`Heikin Ashi data loaded successfully. ${responseData.message}`, 'success');

    } catch (error) {
        console.error('Failed to fetch Heikin Ashi data:', error);
        showToast(`Error loading Heikin Ashi data: ${error.message}`, 'error');
    } finally {
        elements.loadingIndicator.style.display = 'none';
        state.currentlyFetching = false;
    }
}


export async function loadChartData() {
    if (!state.sessionToken) return;

    if (state.mainSeries) state.mainSeries.setData([]);
    if (state.volumeSeries) state.volumeSeries.setData([]);

    if (state.candleType === 'heikin_ashi') {
        state.allHeikinAshiData = [];
        state.allHeikinAshiVolumeData = [];
        state.heikinAshiCurrentOffset = 0;
        state.allHeikinAshiDataLoaded = false;
        state.heikinAshiRequestId = null;
    } else {
        state.allChartData = [];
        state.allVolumeData = [];
        state.chartCurrentOffset = 0;
        state.allDataLoaded = false;
        state.chartRequestId = null;
    }
    
    disconnectFromAllLiveFeeds();

    try {
        const isLive = elements.liveToggle.checked;

        if (state.candleType === 'heikin_ashi') {
            await loadHeikinAshiChart();
            if (isLive) connectToLiveHeikinAshiData();
        } else {
            await loadInitialChart();
            if (isLive) connectToLiveDataFeed();
        }
    } catch (error) {
        console.error('Error loading chart data:', error);
        showToast(`Error loading ${state.candleType === 'heikin_ashi' ? 'Heikin Ashi' : 'regular'} data`, 'error');
    }
}