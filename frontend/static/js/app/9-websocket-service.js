// frontend/static/js/app/9-websocket-service.js
import { state } from './2-state.js';
import * as elements from './1-dom-elements.js';
import { showToast } from './4-ui-helpers.js';

let liveDataSocket = null;
let haLiveDataSocket = null;

function autoScrollView() {
    const autoScaleBtn = document.getElementById('scaling-auto-btn');
    // Check if live toggle is on AND autoscale button is active
    if (elements.liveToggle.checked && autoScaleBtn?.classList.contains('btn-active')) {
        state.mainChart.timeScale().scrollToRealTime();
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
    } else if (data.current_bar && state.mainSeries) { // Handle live update
        const { current_bar } = data;
        const chartFormattedBar = { time: current_bar.unix_timestamp, open: current_bar.open, high: current_bar.high, low: current_bar.low, close: current_bar.close };
        state.mainSeries.update(chartFormattedBar);
        if (state.volumeSeries) {
            const volumeData = { time: current_bar.unix_timestamp, value: current_bar.volume, color: current_bar.close >= current_bar.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' };
            state.volumeSeries.update(volumeData);
        }
        autoScrollView();
    }
}

function handleLiveHeikinAshiData(data) {
    if (!state.mainSeries) return;

    if (Array.isArray(data) && data.length > 0) { // Handle HA backfill
        const chartData = data.map(c => ({ time: c.unix_timestamp, open: c.open, high: c.high, low: c.low, close: c.close }));
        const volumeData = data.map(c => ({ time: c.unix_timestamp, value: c.volume || 0, color: c.close >= c.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' }));
        
        state.mainSeries.setData(chartData);
        if (state.volumeSeries) state.volumeSeries.setData(volumeData);
        
        if (chartData.length > 0) {
            const dataSize = chartData.length;
            state.mainChart.timeScale().setVisibleLogicalRange({ from: Math.max(0, dataSize - 100), to: dataSize - 1 });
        }
        
    } else if (data.current_bar) { // Handle live HA update
        const { current_bar } = data;
        const chartFormattedBar = { time: current_bar.unix_timestamp, open: current_bar.open, high: current_bar.high, low: current_bar.low, close: current_bar.close };
        state.mainSeries.update(chartFormattedBar);
        if (state.volumeSeries) {
            const volumeData = { time: current_bar.unix_timestamp, value: current_bar.volume || 0, color: current_bar.close >= current_bar.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' };
            state.volumeSeries.update(volumeData);
        }
        autoScrollView();
    }
}

// Helper function to check if interval is tick-based
function isTickInterval(interval) {
    return interval.endsWith('t');
}

export function connectToLiveDataFeed() {
    disconnectFromAllLiveFeeds();
    const symbol = elements.symbolSelect.value;
    const interval = elements.intervalSelect.value;
    const timezone = elements.timezoneSelect.value;

    if (isTickInterval(interval)) {
        showToast("Live mode is not supported for tick intervals.", 'warning');
        return;
    }

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

    if (isTickInterval(interval)) {
        showToast("Live mode is not supported for tick intervals.", 'warning');
        return;
    }

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