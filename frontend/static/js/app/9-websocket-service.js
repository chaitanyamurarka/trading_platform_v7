// frontend/static/js/app/9-websocket-service.js
import { state } from './2-state.js';
import * as elements from './1-dom-elements.js';
import { showToast } from './4-ui-helpers.js';

let liveDataSocket = null;
let haLiveDataSocket = null;

/**
 * Generic handler for processing live bar updates from a WebSocket.
 * @param {object} data - The payload from the server, containing optional completed_bar and current_bar.
 */
function handleLiveUpdate(data) {
    if (!data || !state.mainSeries) return;

    const { completed_bar, current_bar } = data;

    // A completed bar is a finalized bar from the previous interval.
    // We update it first to ensure it's locked in on the chart.
    if (completed_bar) {
        const completedChartBar = { time: completed_bar.unix_timestamp, open: completed_bar.open, high: completed_bar.high, low: completed_bar.low, close: completed_bar.close };
        state.mainSeries.update(completedChartBar);
        if (state.volumeSeries) {
            const volumeData = { time: completed_bar.unix_timestamp, value: completed_bar.volume, color: completed_bar.close >= completed_bar.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' };
            state.volumeSeries.update(volumeData);
        }
    }

    // The current bar is the new, in-progress bar for the current interval.
    // Calling update() with its data will either create it (if its timestamp is new)
    // or update it (if its timestamp is the same as the last point).
    if (current_bar) {
        const currentChartBar = { time: current_bar.unix_timestamp, open: current_bar.open, high: current_bar.high, low: current_bar.low, close: current_bar.close };
        state.mainSeries.update(currentChartBar);
        if (state.volumeSeries) {
            const volumeData = { time: current_bar.unix_timestamp, value: current_bar.volume, color: current_bar.close >= current_bar.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' };
            state.volumeSeries.update(volumeData);
        }
    }
    
}


function handleRegularLiveData(data) {
    if (Array.isArray(data)) { // Handle backfill data
        if (data.length === 0) return;
        console.log(`Received backfill data with ${data.length} bars.`);
        const formattedBackfillBars = data.map(c => ({ time: c.unix_timestamp, open: c.open, high: c.high, low: c.low, close: c.close }));
        const formattedVolumeBars = data.map(c => ({ time: c.unix_timestamp, value: c.volume, color: c.close >= c.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' }));
        const lastHistoricalTime = state.allChartData.length > 0 ? state.allChartData[state.allChartData.length - 1].time : 0;
        
        const newOhlcBars = formattedBackfillBars.filter(d => d.time > lastHistoricalTime);
        const newVolumeBars = formattedVolumeBars.filter(d => d.time > lastHistoricalTime);

        if (newOhlcBars.length > 0) {
            state.allChartData.push(...newOhlcBars);
            state.allVolumeData.push(...newVolumeBars);
            state.mainSeries.setData(state.allChartData);
            state.volumeSeries.setData(state.allVolumeData);
        }
    } else { // Handle live update for both regular and tick-based charts
        handleLiveUpdate(data);
    }
}

function handleLiveHeikinAshiData(data) {
    if (Array.isArray(data)) { // Handle HA backfill
        if (data.length === 0) return;
        const chartData = data.map(c => ({ time: c.unix_timestamp, open: c.open, high: c.high, low: c.low, close: c.close }));
        const volumeData = data.map(c => ({ time: c.unix_timestamp, value: c.volume || 0, color: c.close >= c.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' }));
        
        state.mainSeries.setData(chartData);
        if (state.volumeSeries) state.volumeSeries.setData(volumeData);
        
        if (chartData.length > 0) {
            const dataSize = chartData.length;
            state.mainChart.timeScale().setVisibleLogicalRange({ from: Math.max(0, dataSize - 100), to: dataSize - 1 });
        }
    } else { // Handle live HA update
       handleLiveUpdate(data);
    }
}


export function connectToLiveDataFeed() {
    disconnectFromAllLiveFeeds();
    const symbol = elements.symbolSelect.value;
    const interval = elements.intervalSelect.value;
    const timezone = elements.timezoneSelect.value;

    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsURL = `${wsProtocol}//${window.location.host}/ws/live/${encodeURIComponent(symbol)}/${interval}/${encodeURIComponent(timezone)}`;

    showToast(`Connecting to live feed for ${symbol}...`, 'info');
    liveDataSocket = new WebSocket(wsURL);

    liveDataSocket.onopen = () => showToast(`Live feed connected for ${symbol}!`, 'success');
    liveDataSocket.onmessage = (event) => handleRegularLiveData(JSON.parse(event.data));
    liveDataSocket.onclose = () => console.log('Live data WebSocket closed.');
    liveDataSocket.onerror = (error) => console.error('Live data WebSocket error:', error);
}

export function connectToLiveHeikinAshiData() {
    disconnectFromAllLiveFeeds();
    const symbol = elements.symbolSelect.value;
    const interval = elements.intervalSelect.value;
    const timezone = elements.timezoneSelect.value;

    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${window.location.host}/ws-ha/live/${encodeURIComponent(symbol)}/${interval}/${encodeURIComponent(timezone)}`;

    showToast('Connecting to live Heikin Ashi data...', 'info');
    haLiveDataSocket = new WebSocket(wsUrl);

    haLiveDataSocket.onopen = () => showToast('Connected to live Heikin Ashi data', 'success');
    haLiveDataSocket.onmessage = (event) => handleLiveHeikinAshiData(JSON.parse(event.data));
    haLiveDataSocket.onclose = () => console.log('HA live data WebSocket closed.');
    haLiveDataSocket.onerror = (error) => console.error('HA live data WebSocket error:', error);
}

export function disconnectFromAllLiveFeeds() {
    if (liveDataSocket) {
        liveDataSocket.onclose = null;
        liveDataSocket.close();
        liveDataSocket = null;
    }
    if (haLiveDataSocket) {
        haLiveDataSocket.onclose = null;
        haLiveDataSocket.close();
        haLiveDataSocket = null;
    }
}