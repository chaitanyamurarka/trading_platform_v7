// frontend/static/js/app/2-state.js

import * as elements from './1-dom-elements.js';

export const constants = {
    DATA_CHUNK_SIZE: 5000,
    INITIAL_FETCH_LIMIT: 5000, // Matches backend limit
};

export const state = {
    // Chart and Series objects
    chart: null,
    mainSeries: null,
    volumeSeries: null,

    // Session and UI state
    isLoadingHistoricalData: false,
    websocketMessageBuffer: [],
    sessionToken: null,
    heartbeatIntervalId: null,
    showOHLCLegend: true,
    candleType: 'regular', // 'regular' or 'heikin_ashi' or 'tick'

    // Flags
    isLive: false,
    currentlyFetching: false,

    // Time Interval
    interval: '1m',

    // Time Zone
    timezone: 'America/New_York',
    
    // State for Regular Candle Data
    chartRequestId: null,
    allChartData: [],
    allVolumeData: [],
    chartCurrentOffset: 0,
    allDataLoaded: false,

    // State for Heikin Ashi Data
    heikinAshiRequestId: null,
    allHeikinAshiData: [],
    allHeikinAshiVolumeData: [],
    heikinAshiCurrentOffset: 0,
    allHeikinAshiDataLoaded: false,

    // NEW: State for Aggregated Tick Data
    tickRequestId: null,
    allTickData: [],
    allTickVolumeData: [],
    tickCurrentOffset: 0,
    allTickDataLoaded: false,

    // Helper method to get current chart data based on candle type
    getCurrentChartData() {
        // --- FIX START ---
        // Add the 'tick' case to return the correct data array.
        if (this.candleType === 'heikin_ashi') {
            return this.allHeikinAshiData;
        } else if (this.candleType === 'tick') {
            return this.allTickData;
        } else {
            return this.allChartData;
        }
        // --- FIX END ---
    },
    
    // Helper method to get current volume data based on candle type
    getCurrentVolumeData() {
        // --- FIX START ---
        // Also update this helper for consistency with volume data.
        if (this.candleType === 'heikin_ashi') {
            return this.allHeikinAshiVolumeData;
        } else if (this.candleType === 'tick') {
            return this.allTickVolumeData;
        } else {
            return this.allVolumeData;
        }
        // --- FIX END ---
    },
    
    // Helper method to get current request ID based on candle type
    getCurrentRequestId() {
        return this.candleType === 'heikin_ashi' ? this.heikinAshiRequestId : this.chartRequestId;
    },
    
    // Helper method to get current offset based on candle type
    getCurrentOffset() {
        return this.candleType === 'heikin_ashi' ? this.heikinAshiCurrentOffset : this.chartCurrentOffset;
    },
    
    // Helper method to check if all data is loaded based on candle type
    isAllDataLoaded() {
        return this.candleType === 'heikin_ashi' ? this.allHeikinAshiDataLoaded : this.allDataLoaded;
    },

    // NEW: Function to reset all data arrays and flags
    resetAllData() {
        this.chartRequestId = null;
        this.allChartData = [];
        this.allVolumeData = [];
        this.chartCurrentOffset = 0;
        this.allDataLoaded = false;

        this.heikinAshiRequestId = null;
        this.allHeikinAshiData = [];
        this.allHeikinAshiVolumeData = [];
        this.heikinAshiCurrentOffset = 0;
        this.allHeikinAshiDataLoaded = false;
        
        this.tickRequestId = null;
        this.allTickData = [];
        this.allTickVolumeData = [];
        this.tickCurrentOffset = 0;
        this.allTickDataLoaded = false;

        console.log("All chart data states have been reset.");
    },

    // UPDATED: Function to process the initial data load for any type
    processInitialData(responseData, dataType) {
        const chartFormattedData = responseData.candles.map(c => ({
            time: c.unix_timestamp,
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close
        }));

        const volumeFormattedData = responseData.candles.map(c => ({
            time: c.unix_timestamp,
            value: c.volume || 0,
            color: c.close >= c.open ? (elements.volUpColorInput.value + '80') : (elements.volDownColorInput.value + '80')
        }));
        
        // Assign data to the correct state variables based on dataType
        switch (dataType) {
            case 'tick':
                this.allTickData = chartFormattedData;
                this.allTickVolumeData = volumeFormattedData;
                this.tickRequestId = responseData.request_id;
                this.tickCurrentOffset = responseData.offset;
                this.allTickDataLoaded = !responseData.is_partial;
                this.mainSeries.setData(this.allTickData);
                this.volumeSeries.setData(this.allTickVolumeData);
                break;
            case 'heikin_ashi':
                this.allHeikinAshiData = chartFormattedData;
                this.allHeikinAshiVolumeData = volumeFormattedData;
                this.heikinAshiRequestId = responseData.request_id;
                this.heikinAshiCurrentOffset = responseData.offset;
                this.allHeikinAshiDataLoaded = !responseData.is_partial;
                this.mainSeries.setData(this.allHeikinAshiData);
                this.volumeSeries.setData(this.allHeikinAshiVolumeData);
                break;
            default: // 'regular'
                this.allChartData = chartFormattedData;
                this.allVolumeData = volumeFormattedData;
                this.chartRequestId = responseData.request_id;
                this.chartCurrentOffset = responseData.offset;
                this.allDataLoaded = !responseData.is_partial;
                this.mainSeries.setData(this.allChartData);
                this.volumeSeries.setData(this.allVolumeData);
                break;
        }

        // Fit content after setting data to ensure the view is correct
        if (this.chart) {
            this.chart.timeScale().fitContent();
        }
    }
};