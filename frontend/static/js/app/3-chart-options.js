// frontend/static/js/app/3-chart-options.js

import * as elements from './1-dom-elements.js';

/**
 * Generates the main options object for creating the chart.
 * @param {string} theme - The current theme ('light' or 'dark').
 * @returns {object} The chart options for the Lightweight Charts library.
 */
// frontend/static/js/app/3-chart-options.js

export const chartOptions = (theme) => {
    const isDark = theme === 'dark';
    const gridColor = isDark ? '#333' : '#e0e0e0';
    const textColor = isDark ? '#fff' : '#333';

    return {
        layout: {
            background: { color: isDark ? '#1a1a1a' : '#ffffff' },
            textColor: textColor,
        },
        grid: {
            vertLines: { color: gridColor },
            horzLines: { color: gridColor },
        },
        crosshair: {
            mode: LightweightCharts.CrosshairMode.Normal,
        },
        rightPriceScale: {
            borderColor: gridColor,
        },
        timeScale: {
            timeVisible: true,
            secondsVisible: true,
            borderColor: gridColor,
            
            // =================================================================
            // --- NEW FIX: Prevent the chart from automatically shifting ---
            // This ensures that the initial data, including after-hours bars,
            // remains in view after being loaded.
            // =================================================================
            shiftVisibleRangeOnNewBar: false,
        },
        watermark: {
            color: 'rgba(150, 150, 150, 0.2)',
            visible: true,
            text: 'My Trading Platform',
            fontSize: 48,
            horzAlign: 'center',
            vertAlign: 'center',
        }
    };
};

/**
 * Generates the options for the main candlestick/bar series.
 * @returns {object} The series options.
 */
export function getSeriesOptions() {
    const disableWicks = elements.disableWicksInput.checked;
    const backgroundColor = elements.bgColorInput.value; // Get the current background color

    return {
        upColor: elements.upColorInput.value,
        downColor: elements.downColorInput.value,
        borderDownColor: elements.downColorInput.value,
        borderUpColor: elements.upColorInput.value,
        // Use the background color to make wicks "invisible" when disabled
        wickDownColor: disableWicks ? backgroundColor : elements.wickDownColorInput.value,
        wickUpColor: disableWicks ? backgroundColor : elements.wickUpColorInput.value,
    };
}

export function getChartTheme(theme) {
    const isDarkMode = theme === 'dark';
    return {
        layout: { background: { type: 'solid', color: isDarkMode ? '#1d232a' : '#ffffff' }, textColor: isDarkMode ? '#a6adba' : '#1f2937' },
        grid: { vertLines: { color: isDarkMode ? '#2a323c' : '#e5e7eb' }, horzLines: { color: isDarkMode ? '#2a323c' : '#e5e7eb' } },
        // --- ADD THIS
        timeScale: {
            timeVisible: true,
            secondsVisible: true, // You can set this to true if you need seconds precision
        }
        // ---
    };
}