// frontend/static/js/app/1-dom-elements.js
export const chartContainer = document.getElementById('chartContainer');
export const exchangeSelect = document.getElementById('exchange');
export const symbolSelect = document.getElementById('symbol');
export const intervalSelect = document.getElementById('interval');
export const startTimeInput = document.getElementById('start_time');
export const endTimeInput = document.getElementById('end_time');
export const themeToggle = document.getElementById('theme-toggle');
export const dataSummaryElement = document.getElementById('dataSummary');
export const loadingIndicator = document.getElementById('loadingIndicator');
export const timezoneSelect = document.getElementById('timezone');
export const chartTypeSelect = document.getElementById('chart-type');
export const screenshotBtn = document.getElementById('screenshot-btn');

// Live mode controls
export const liveToggle = document.getElementById('live-toggle');

// OHLC Data Legend - This is the key element for the tooltip functionality
export const dataLegendElement = document.getElementById('data-legend');

// Drawing Tools Toolbar
export const toolTrendLineBtn = document.getElementById('tool-trend-line');
export const toolHorizontalLineBtn = document.getElementById('tool-horizontal-line');
export const toolFibRetracementBtn = document.getElementById('tool-fib-retracement');
export const toolRectangleBtn = document.getElementById('tool-rectangle');
export const toolBrushBtn = document.getElementById('tool-brush');
export const toolRemoveSelectedBtn = document.getElementById('tool-remove-selected');
export const toolRemoveAllBtn = document.getElementById('tool-remove-all');

// Settings Modal
export const settingsModal = document.getElementById('settings_modal');
export const bgColorInput = document.getElementById('setting-bg-color');
export const gridColorInput = document.getElementById('setting-grid-color');
export const watermarkInput = document.getElementById('setting-watermark-text');
export const upColorInput = document.getElementById('setting-up-color');
export const downColorInput = document.getElementById('setting-down-color');
export const wickUpColorInput = document.getElementById('setting-wick-up-color');
export const wickDownColorInput = document.getElementById('setting-wick-down-color');
export const volUpColorInput = document.getElementById('setting-vol-up-color');
export const volDownColorInput = document.getElementById('setting-vol-down-color');
export const disableWicksInput = document.getElementById('setting-disable-wicks');

export const showOHLCLegendToggle = document.getElementById('setting-show-ohlc-legend');

// Add this to the existing exports:
export const candleTypeSelect = document.getElementById('candle-type-select');

// Utility function to check if an element exists
export function elementExists(element) {
    return element && element instanceof HTMLElement;
}

// Utility function to safely get element by ID with error handling
export function safeGetElementById(id) {
    const element = document.getElementById(id);
    if (!element) {
        console.warn(`Element with ID '${id}' not found in DOM`);
    }
    return element;
}

// Validation function to ensure all critical elements are present
export function validateCriticalElements() {
    const criticalElements = {
        chartContainer,
        exchangeSelect,
        symbolSelect,
        intervalSelect,
        startTimeInput,
        endTimeInput,
        dataLegendElement
    };
    
    const missingElements = [];
    
    for (const [name, element] of Object.entries(criticalElements)) {
        if (!elementExists(element)) {
            missingElements.push(name);
        }
    }
    
    if (missingElements.length > 0) {
        console.error('Critical DOM elements missing:', missingElements);
        return false;
    }
    
    return true;
}

// Initialize validation on module load
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', validateCriticalElements);
} else {
    validateCriticalElements();
}