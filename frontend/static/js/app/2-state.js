// app/2-state.js
export const state = {
    // Chart data arrays - separate for each candle type
    allChartData: [],           // Regular candle data
    allHeikinAshiData: [],      // Heikin Ashi candle data
    
    // Volume data arrays - separate for each candle type
    allVolumeData: [],          // Regular candle volume
    allHeikinAshiVolumeData: [], // Heikin Ashi volume
    
    // Fetching state
    currentlyFetching: false,
    
    // Pagination state for regular candles
    allDataLoaded: false,
    chartRequestId: null,
    chartCurrentOffset: 0,
    
    // Pagination state for Heikin Ashi candles
    allHeikinAshiDataLoaded: false,
    heikinAshiRequestId: null,
    heikinAshiCurrentOffset: 0,
    
    // Chart instances
    mainChart: null,
    mainSeries: null,
    volumeSeries: null,
    
    // Session and UI state
    sessionToken: null,
    heartbeatIntervalId: null,
    showOHLCLegend: true,
    candleType: 'regular', // 'regular' or 'heikin_ashi'
    
    // Helper method to get current chart data based on candle type
    getCurrentChartData() {
        return this.candleType === 'heikin_ashi' ? this.allHeikinAshiData : this.allChartData;
    },
    
    // Helper method to get current volume data based on candle type
    getCurrentVolumeData() {
        return this.candleType === 'heikin_ashi' ? this.allHeikinAshiVolumeData : this.allVolumeData;
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
    }
};

export const constants = {
    DATA_CHUNK_SIZE: 5000
};