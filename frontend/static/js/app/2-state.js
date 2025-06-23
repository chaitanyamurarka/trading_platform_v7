import * as elements from './1-dom-elements.js';

/**
 * Shared constants for the application.
 */
export const constants = {
    DATA_CHUNK_SIZE: 5000,
};

/**
 * Main application state management, now fully unified.
 */
export const state = {
    // Chart objects
    chart: null,
    mainSeries: null,
    volumeSeries: null,

    // Session and UI state
    sessionToken: null,
    currentlyFetching: false,
    
    // --- Unified Data State ---
    // The type of data currently loaded (e.g., 'regular', 'heikin_ashi', 'tick')
    currentDataType: 'regular',
    // The cursor for fetching the next chunk of data.
    requestId: null,
    // Flag indicating if all historical data for the current view has been loaded.
    allDataLoaded: false,
    // A single array for the main series data (Candle, Bar, Line, etc.).
    allChartData: [],
    // A single array for the volume series data.
    allVolumeData: [],

    /**
     * Resets all data-related states before a new request.
     */
    resetAllData() {
        this.currentlyFetching = false;
        this.requestId = null;
        this.allDataLoaded = false;
        this.allChartData = [];
        this.allVolumeData = [];
        if (this.mainSeries) this.mainSeries.setData([]);
        if (this.volumeSeries) this.volumeSeries.setData([]);
    },
    
    /**
     * Processes the initial data payload from the unified API.
     */
    processInitialData(responseData, dataType) {
        this.currentDataType = dataType;
        this.requestId = responseData.request_id;
        this.allDataLoaded = !responseData.is_partial;

        this.allChartData = responseData.candles.map(c => ({
            time: c.unix_timestamp, open: c.open, high: c.high, low: c.low, close: c.close
        }));
        this.allVolumeData = responseData.candles.map(c => ({
            time: c.unix_timestamp, value: c.volume || 0,
            color: c.close >= c.open ? (elements.volUpColorInput.value + '80') : (elements.volDownColorInput.value + '80')
        }));

        this.mainSeries.setData(this.allChartData);
        this.volumeSeries.setData(this.allVolumeData);
    },

    /**
     * Processes a chunk of older data from the unified chunk endpoint.
     */
    processChunkData(chunkData) {
        this.requestId = chunkData.request_id;
        this.allDataLoaded = !chunkData.is_partial;

        const chartChunk = chunkData.candles.map(c => ({
            time: c.unix_timestamp, open: c.open, high: c.high, low: c.low, close: c.close
        }));
        const volumeChunk = chunkData.candles.map(c => ({
            time: c.unix_timestamp, value: c.volume || 0,
            color: c.close >= c.open ? (elements.volUpColorInput.value + '80') : (elements.volDownColorInput.value + '80')
        }));
        
        this.allChartData = [...chartChunk, ...this.allChartData];
        this.allVolumeData = [...volumeChunk, ...this.allVolumeData];
        
        this.mainSeries.setData(this.allChartData);
        this.volumeSeries.setData(this.allVolumeData);
    },

    /**
     * Processes a live WebSocket update message.
     */
    processLiveUpdate(data) {
        if (!data || !this.mainSeries) return;
        
        const updateSeries = (bar, series, isVolume) => {
            if (!bar) return;
            const point = isVolume
                ? { time: bar.unix_timestamp, value: bar.volume, color: bar.close >= bar.open ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80' }
                : { time: bar.unix_timestamp, open: bar.open, high: bar.high, low: bar.low, close: bar.close };
            series.update(point);
        };
        
        updateSeries(data.completed_bar, this.mainSeries, false);
        updateSeries(data.completed_bar, this.volumeSeries, true);
        updateSeries(data.current_bar, this.mainSeries, false);
        updateSeries(data.current_bar, this.volumeSeries, true);
    },
};