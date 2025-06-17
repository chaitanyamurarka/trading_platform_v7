// app/6-api-service.js
import { getHistoricalDataUrl, getHistoricalDataChunkUrl, initiateSession, sendHeartbeat, fetchHeikinAshiData } from '../api.js';
import { state, constants } from './2-state.js';
import * as elements from './1-dom-elements.js';
import { showToast, updateDataSummary } from './4-ui-helpers.js';
import { fetchHeikinAshiChunk } from '../api.js';

let liveDataSocket = null;

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

export function setAutomaticDateTime() {
    const selectedTimezone = elements.timezoneSelect.value || 'America/New_York';

    const now = new Date();
    const nyParts = getDatePartsInZone(now, 'America/New_York');

    // Create a Date object representing 8:00 PM New York time
    const eightPMNY = new Date(Date.UTC(nyParts.year, nyParts.month - 1, nyParts.day, 0, 0, 0));
    eightPMNY.setUTCHours(getUTCHourOffset('America/New_York', 20, now));

    // If NY current date is same but time < 20:00 → subtract a day
    const currentNY = new Date();
    const currentParts = getDatePartsInZone(currentNY, 'America/New_York');

    if (currentParts.year === nyParts.year && currentParts.month === nyParts.month && currentParts.day === nyParts.day) {
        const nowNY = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
        const endNY = new Date(nowNY);
        endNY.setHours(20, 0, 0, 0); // set 8 PM NY today

        if (nowNY < endNY) {
            endNY.setDate(endNY.getDate() - 1);
        }
    }

    const finalEndUTC = new Date(eightPMNY);
    const finalStartUTC = new Date(finalEndUTC);
    finalStartUTC.setUTCDate(finalEndUTC.getUTCDate() - 30);

    const startFormatted = formatDateInZone(finalStartUTC, selectedTimezone);
    const endFormatted = formatDateInZone(finalEndUTC, selectedTimezone);

    elements.startTimeInput.value = startFormatted;
    elements.endTimeInput.value = endFormatted;

    console.log(`[${selectedTimezone}] Start: ${startFormatted}, End: ${endFormatted}`);
}

function formatDateInZone(date, timeZone) {
    const parts = new Intl.DateTimeFormat('en-US', {
        timeZone,
        year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit',
        hour12: false
    }).formatToParts(date);

    const map = Object.fromEntries(parts.map(p => [p.type, p.value]));
    return `${map.year}-${map.month}-${map.day}T${map.hour}:${map.minute}`;
}

function getCurrentHourInTimezone(timeZone) {
    const now = new Date();
    const parts = new Intl.DateTimeFormat('en-US', {
        timeZone,
        hour: '2-digit',
        hour12: false
    }).formatToParts(now);
    return parseInt(parts.find(p => p.type === 'hour').value, 10);
}

function getDatePartsInZone(date, timeZone) {
    const parts = new Intl.DateTimeFormat('en-US', {
        timeZone,
        year: 'numeric', month: '2-digit', day: '2-digit'
    }).formatToParts(date);

    return Object.fromEntries(parts.map(p => [p.type, parseInt(p.value, 10)]));
}

function getUTCHourOffset(timeZone, targetHourInZone, referenceDate) {
    const testDate = new Date(referenceDate);
    testDate.setUTCHours(0, 0, 0, 0); // midnight UTC

    const zoneHour = new Intl.DateTimeFormat('en-US', {
        timeZone,
        hour: '2-digit',
        hour12: false
    }).formatToParts(testDate).find(p => p.type === 'hour').value;

    const offset = targetHourInZone - parseInt(zoneHour, 10);
    return 0 + offset;
}

export async function fetchAndPrependDataChunk() {
    if (state.candleType === 'heikin_ashi') {
        await fetchAndPrependHeikinAshiDataChunk();
    } else {
        // Regular candle chunk logic (existing function)
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
                const chartFormattedData = chunkData.candles.map(c => ({ 
                    time: c.unix_timestamp, 
                    open: c.open, 
                    high: c.high, 
                    low: c.low, 
                    close: c.close 
                }));
                const volumeFormattedData = chunkData.candles.map(c => ({ 
                    time: c.unix_timestamp, 
                    value: c.volume, 
                    color: c.close >= c.open ? 
                        elements.volUpColorInput.value + '80' : 
                        elements.volDownColorInput.value + '80' 
                }));
                
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
        } catch(error) {
            console.error("Failed to prepend data chunk:", error);
            showToast("Could not load older data.", 'error');
        } finally {
            elements.loadingIndicator.style.display = 'none';
            state.currentlyFetching = false;
        }
    }
}

export async function loadInitialChart() {
    if (!state.sessionToken) return;

    // --- REMOVED THIS LINE TO PREVENT RESETTING THE TIME ON EVERY LOAD ---
    // setAutomaticDateTime();

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
            state.mainChart.timeScale().setVisibleLogicalRange({
                from: Math.max(0, dataSize - 100),
                to: dataSize - 1,
            });
        } else {
            state.mainChart.timeScale().fitContent();
        }

        state.mainChart.priceScale().applyOptions({ autoscale: true });

        updateDataSummary(state.allChartData.at(-1));
    } catch (error) {
        console.error('Failed to fetch initial chart data:', error);
        showToast(`Error: ${error.message}`, 'error');
    } finally {
        elements.loadingIndicator.style.display = 'none';
        state.currentlyFetching = false;
    }
}

export async function startSession() {
    try {
        const sessionData = await initiateSession();
        state.sessionToken = sessionData.session_token;
        console.log(`Session started with token: ${state.sessionToken}`);
        showToast('Session started.', 'info');
        loadInitialChart();
        if (state.heartbeatIntervalId) clearInterval(state.heartbeatIntervalId);
        state.heartbeatIntervalId = setInterval(() => {
            if (state.sessionToken) sendHeartbeat(state.sessionToken).catch(e => console.error('Heartbeat failed', e));
        }, 60000);
    } catch (error) {
        console.error('Failed to initiate session:', error);
        showToast('Could not start a session. Please reload.', 'error');
    }
}

// --- Live data functions remain unchanged ---
export function disconnectFromLiveDataFeed() {
    if (liveDataSocket) {
        console.log('Closing existing WebSocket connection.');
        liveDataSocket.onclose = null;
        liveDataSocket.close();
        liveDataSocket = null;
    }
}

/**
 * Disconnects from live Heikin Ashi data WebSocket
 */
export function disconnectFromLiveHeikinAshiData() {
    if (liveDataSocket && liveDataSocket.readyState === WebSocket.OPEN) {
        console.log('Closing live Heikin Ashi data connection...');
        liveDataSocket.close();
        liveDataSocket = null;
        console.log('Disconnected from live Heikin Ashi data');
        showToast('Disconnected from live Heikin Ashi data', 'info');
    }
}

/**
 * Generic disconnect function that handles both connection types
 */
export function disconnectFromAllLiveFeeds() {
    disconnectFromLiveDataFeed();      // Regular candles
    disconnectFromLiveHeikinAshiData(); // Heikin Ashi candles
}

export function connectToLiveDataFeed(symbol, interval) {
    disconnectFromLiveDataFeed();
    const timezone = elements.timezoneSelect.value;
    if (!timezone) {
        showToast('Please select a timezone before connecting to live feed.', 'error');
        return;
    }
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsURL = `${wsProtocol}//${window.location.host}/ws/live/${encodeURIComponent(symbol)}/${interval}/${encodeURIComponent(timezone)}`;    console.log(`Connecting to WebSocket: ${wsURL}`);
    showToast(`Connecting to live feed for ${symbol}...`, 'info');
    const socket = new WebSocket(wsURL);
    socket.onopen = () => {
        console.log('WebSocket connection established.');
        showToast(`Live feed connected for ${symbol}!`, 'success');
    };
    socket.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (Array.isArray(data)) {
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
                console.log(`Applied ${newOhlcBars.length} new bars from backfill.`);
            } else {
                console.log('Backfill data did not contain any new bars.');
            }
        }
        else if (data.current_bar && state.mainSeries) {
            const { current_bar } = data;
            const chartFormattedBar = { time: current_bar.unix_timestamp, open: current_bar.open, high: current_bar.high, low: current_bar.low, close: current_bar.close };
            state.mainSeries.update(chartFormattedBar);
            if (state.volumeSeries) {
                const volumeData = { time: current_bar.unix_timestamp, value: current_bar.volume, color: current_bar.close >= current_bar.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' };
                state.volumeSeries.update(volumeData);
            }
            updateDataSummary(current_bar);
        }
    };
    liveDataSocket = socket;
}

/**
 * Establishes WebSocket connection for live Heikin Ashi data
 */
export function connectToLiveHeikinAshiData() {
    if (!state.sessionToken || liveDataSocket) return;

    if (state.candleType !== 'heikin_ashi') {
        console.log('Not connecting to HA live data - candle type is not heikin_ashi');
        return;
    }

    const symbol = elements.symbolSelect.value;
    const interval = elements.intervalSelect.value;
    const timezone = elements.timezoneSelect.value;
    
    // Use the new Heikin Ashi live WebSocket endpoint
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${window.location.host}/ws-ha/live/${encodeURIComponent(symbol)}/${interval}/${encodeURIComponent(timezone)}`;
    

    console.log('Connecting to live Heikin Ashi data:', wsUrl);
    
    const socket = new WebSocket(wsUrl);
    
    socket.onopen = () => {
        console.log('Connected to live Heikin Ashi data stream');
        showToast('Connected to live Heikin Ashi data', 'success');
    };
    
    socket.onclose = () => {
        console.log('Disconnected from live Heikin Ashi data stream');
        liveDataSocket = null;
    };
    
    socket.onerror = (error) => {
        console.error('Heikin Ashi WebSocket error:', error);
        showToast('Connection error for live Heikin Ashi data', 'error');
    };
    
    socket.onmessage = (event) => {
        const data = JSON.parse(event.data);
        handleLiveHeikinAshiData(data);
    };
    
    liveDataSocket = socket;
}

/**
 * Handles incoming live Heikin Ashi data
 */
function handleLiveHeikinAshiData(data) {
    if (!state.mainSeries) return;

    // Handle initial historical data batch
    if (Array.isArray(data) && data.length > 0) {
        console.log(`Received ${data.length} historical Heikin Ashi candles`);
        
        // Convert to chart format
        const chartData = data.map(candle => ({
            time: candle.unix_timestamp,
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close
        }));

        const volumeData = data.map(candle => ({
            time: candle.unix_timestamp,
            value: candle.volume || 0,
            color: candle.close >= candle.open ? 
                elements.volUpColorInput.value + '80' : 
                elements.volDownColorInput.value + '80'
        }));

        // Update chart
        state.mainSeries.setData(chartData);
        if (state.volumeSeries) {
            state.volumeSeries.setData(volumeData);
        }

        // Set visible range to show last 100 bars
        if (chartData.length > 0) {
            const dataSize = chartData.length;
            state.mainChart.timeScale().setVisibleLogicalRange({
                from: Math.max(0, dataSize - 100),
                to: dataSize - 1,
            });
        }

        // Update summary with latest bar
        updateDataSummary(data[data.length - 1]);
        
        showToast(`Loaded ${data.length} Heikin Ashi candles`, 'success');
        return;
    }

    // Handle live updates (single candle)
    if (data.completed_bar && state.mainSeries) {
        const { completed_bar } = data;
        const chartFormattedBar = {
            time: completed_bar.unix_timestamp,
            open: completed_bar.open,
            high: completed_bar.high,
            low: completed_bar.low,
            close: completed_bar.close
        };
        
        state.mainSeries.update(chartFormattedBar);
        
        if (state.volumeSeries) {
            const volumeData = { 
                time: current_bar.unix_timestamp, 
                value: current_bar.volume, 
                color: current_bar.close >= current_bar.open ? 
                    elements.volUpColorInput.value + '80' : 
                    elements.volDownColorInput.value + '80'
            };
            state.volumeSeries.update(volumeData);
            
            // Also update the state array for consistency
            if (state.allHeikinAshiVolumeData.length > 0) {
                const lastIndex = state.allHeikinAshiVolumeData.length - 1;
                state.allHeikinAshiVolumeData[lastIndex] = volumeData;
            }
        }
        
        updateDataSummary(completed_bar);
        console.log('Updated completed Heikin Ashi bar:', completed_bar.unix_timestamp);
    }
    else if (data.current_bar && state.mainSeries) {
        const { current_bar } = data;
        const chartFormattedBar = {
            time: current_bar.unix_timestamp,
            open: current_bar.open,
            high: current_bar.high,
            low: current_bar.low,
            close: current_bar.close
        };
        
        state.mainSeries.update(chartFormattedBar);
        
        if (state.volumeSeries) {
            const volumeData = {
                time: current_bar.unix_timestamp,
                value: current_bar.volume || 0,
                color: current_bar.close >= current_bar.open ? 
                    elements.volUpColorInput.value + '80' : 
                    elements.volDownColorInput.value + '80'
            };
            state.volumeSeries.update(volumeData);
        }
        
        updateDataSummary(current_bar);
        console.log('Updated current Heikin Ashi bar:', current_bar.unix_timestamp);
    }
}

/**
 * Fetches and prepends older Heikin Ashi data for infinite scroll
 */
export async function fetchAndPrependHeikinAshiDataChunk() {
    const nextOffset = state.heikinAshiCurrentOffset - constants.DATA_CHUNK_SIZE;
    if (nextOffset < 0) { 
        state.allHeikinAshiDataLoaded = true; 
        return; 
    }
    
    state.currentlyFetching = true;
    elements.loadingIndicator.style.display = 'flex';
    
    try {
        const chunkData = await fetchHeikinAshiChunk(
            state.heikinAshiRequestId, 
            nextOffset, 
            constants.DATA_CHUNK_SIZE
        );
        
        if (chunkData && chunkData.candles.length > 0) {
            // Convert HA chunk to chart format
            const chartFormattedData = chunkData.candles.map(c => ({ 
                time: c.unix_timestamp, 
                open: c.open, 
                high: c.high, 
                low: c.low, 
                close: c.close 
            }));
            
            // Volume data (same as regular candles but from HA candle data)
            const volumeFormattedData = chunkData.candles.map(c => ({ 
                time: c.unix_timestamp, 
                value: c.volume || 0, 
                color: c.close >= c.open ? 
                    elements.volUpColorInput.value + '80' : 
                    elements.volDownColorInput.value + '80' 
            }));
            
            // Prepend to existing data
            state.allHeikinAshiData = [...chartFormattedData, ...state.allHeikinAshiData];
            state.allHeikinAshiVolumeData = [...volumeFormattedData, ...state.allHeikinAshiVolumeData];

            // Update chart series
            state.mainSeries.setData(state.allHeikinAshiData);
            state.volumeSeries.setData(state.allHeikinAshiVolumeData);
            
            // Update offset tracking
            state.heikinAshiCurrentOffset = chunkData.offset;
            
            // Check if we've reached the beginning
            if (state.heikinAshiCurrentOffset === 0) {
                state.allHeikinAshiDataLoaded = true;
            }
            
            showToast(`Loaded ${chunkData.candles.length} older Heikin Ashi candles`, 'success');
        } else {
            state.allHeikinAshiDataLoaded = true;
        }
    } catch(error) {
        console.error("Failed to prepend Heikin Ashi data chunk:", error);
        showToast("Could not load older Heikin Ashi data.", 'error');
    } finally {
        elements.loadingIndicator.style.display = 'none';
        state.currentlyFetching = false;
    }
}

/**
 * Enhanced loadHeikinAshiChart function with proper state initialization
 */
export async function loadHeikinAshiChart() {
    if (!state.sessionToken) return;

    state.currentlyFetching = true;
    elements.loadingIndicator.style.display = 'flex';

    try {
        const responseData = await fetchHeikinAshiData(
            state.sessionToken,
            elements.exchangeSelect.value,
            elements.symbolSelect.value,
            elements.intervalSelect.value,
            elements.startTimeInput.value,
            elements.endTimeInput.value,
            elements.timezoneSelect.value
        );

        if (!responseData || !responseData.candles || responseData.candles.length === 0) {
            showToast(responseData?.message || 'No Heikin Ashi data found.', 'warning');
            if (state.mainSeries) state.mainSeries.setData([]);
            if (state.volumeSeries) state.volumeSeries.setData([]);
            return;
        }

        // Initialize Heikin Ashi state
        state.heikinAshiRequestId = responseData.request_id;
        state.heikinAshiCurrentOffset = responseData.offset || 0;
        state.allHeikinAshiDataLoaded = !responseData.is_partial;
        
        // Convert HA data to chart format
        state.allHeikinAshiData = responseData.candles.map(c => ({
            time: c.unix_timestamp,
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close
        }));

        // Volume data (same as regular candles)
        state.allHeikinAshiVolumeData = responseData.candles.map(c => ({ 
            time: c.unix_timestamp, 
            value: c.volume || 0, 
            color: c.close >= c.open ? 
                elements.volUpColorInput.value + '80' : 
                elements.volDownColorInput.value + '80' 
        }));
        // Update chart with HA data
        state.mainSeries.setData(state.allHeikinAshiData);
        state.volumeSeries.setData(state.allHeikinAshiVolumeData);

        // Set visible range
        if (state.allHeikinAshiData.length > 0) {
            const dataSize = state.allHeikinAshiData.length;
            state.mainChart.timeScale().setVisibleLogicalRange({
                from: Math.max(0, dataSize - 100),
                to: dataSize - 1,
            });
        }

        state.mainChart.priceScale().applyOptions({ autoscale: true });
        updateDataSummary(state.allHeikinAshiData.at(-1));

        showToast(`Heikin Ashi data loaded successfully. ${responseData.message}`, 'success');

    } catch (error) {
    console.error('Failed to fetch Heikin Ashi data:', error);
    
    // Fallback: Try to load regular candles if HA fails
    if (error.message.includes('Heikin Ashi') || error.message.includes('HA')) {
        console.log('Attempting fallback to regular candles...');
        showToast('Heikin Ashi data unavailable, switching to regular candles', 'warning');
        
        // Switch back to regular candles
        state.candleType = 'regular';
        elements.candleTypeSelect.value = 'regular';
        
        // Load regular data instead
        try {
            await loadInitialChart();
            connectToLiveDataFeed();
        } catch (fallbackError) {
            console.error('Fallback to regular candles also failed:', fallbackError);
            showToast('Unable to load any chart data', 'error');
        }
    } else {
        showToast(`Error loading Heikin Ashi data: ${error.message}`, 'error');
    }
} finally {
    elements.loadingIndicator.style.display = 'none';
    state.currentlyFetching = false;
}
}

/**
 * Enhanced loadChartData function with proper state cleanup
 */
export async function loadChartData() {
    // Clean previous data and state
    if (state.mainSeries) {
        state.mainSeries.setData([]);
    }
    if (state.volumeSeries) {
        state.volumeSeries.setData([]);
    }
    
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
    // Disconnect any existing live data connection
    disconnectFromAllLiveFeeds();
    
    try {
        if (state.candleType === 'heikin_ashi') {
            await loadHeikinAshiChart();
            // Connect to live Heikin Ashi data after loading historical data
            connectToLiveHeikinAshiData();
        } else {
            await loadInitialChart();
            // Connect to regular live data
            connectToLiveDataFeed();
        }
    } catch (error) {
        console.error('Error loading chart data:', error);
        showToast(`Error loading ${state.candleType === 'heikin_ashi' ? 'Heikin Ashi' : 'regular'} data`, 'error');
    }
}