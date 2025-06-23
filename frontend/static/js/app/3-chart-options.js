import { createChart } from '/dist/lightweight-charts.esm.js';
import { state } from './2-state.js';
import * as elements from './1-dom-elements.js';

/**
 * Creates the main chart object, adds the primary series, and stores them in the state.
 * This is the central function for initializing the chart view.
 */
export function initializeNewChartObject() {
    const theme = localStorage.getItem('chartTheme') || 'light';
    const options = chartOptions(theme);
    
    state.chart = createChart(elements.chartContainer, options);
    
    state.mainSeries = state.chart.addCandlestickSeries(getSeriesOptions());
    state.volumeSeries = state.chart.addHistogramSeries({
        priceFormat: {
            type: 'volume',
        },
        priceScaleId: '', // Set to an empty string to display the volume series on its own scale
    });
    
    // Assign the volume series to the main price scale initially
    state.chart.priceScale('').applyOptions({
        scaleMargins: {
            top: 0.8, // 80% of the chart height for the main series
            bottom: 0,
        },
    });
    
    state.volumeSeries.priceScale().applyOptions({
        scaleMargins: {
            top: 0,
            bottom: 0.2, // 20% of the chart height for the volume series
        }
    });
}

/**
 * Generates the main options object for creating the chart.
 * @param {string} theme - The current theme ('light' or 'dark').
 * @returns {object} The chart options for the Lightweight Charts library.
 */
export const chartOptions = (theme) => {
    const isDark = theme === 'dark';
    const gridColor = isDark ? '#2a323c' : '#e5e7eb';
    const textColor = isDark ? '#a6adba' : '#1f2937';

    return {
        layout: {
            background: { color: isDark ? '#1d232a' : '#ffffff' },
            textColor: textColor,
        },
        grid: {
            vertLines: { color: gridColor },
            horzLines: { color: gridColor },
        },
        crosshair: {
            mode: 1, // LightweightCharts.CrosshairMode.Normal
        },
        rightPriceScale: {
            borderColor: gridColor,
        },
        timeScale: {
            timeVisible: true,
            secondsVisible: true,
            borderColor: gridColor,
            shiftVisibleRangeOnNewBar: false,
        },
        watermark: {
            color: 'rgba(150, 150, 150, 0.2)',
            visible: true,
            text: 'EigenKor Trading',
            fontSize: 48,
            horzAlign: 'center',
            vertAlign: 'center',
        }
    };
};

/**
 * Generates the options for the main candlestick/bar series based on UI settings.
 * @returns {object} The series options.
 */
export function getSeriesOptions() {
    const disableWicks = elements.disableWicksInput.checked;

    const baseOptions = {
        upColor: elements.upColorInput.value,
        downColor: elements.downColorInput.value,
        borderDownColor: elements.downColorInput.value,
        borderUpColor: elements.upColorInput.value,
    };

    if (disableWicks) {
        baseOptions.wickUpColor = 'rgba(0,0,0,0)';
        baseOptions.wickDownColor = 'rgba(0,0,0,0)';
    } else {
        baseOptions.wickDownColor = elements.wickDownColorInput.value;
        baseOptions.wickUpColor = elements.wickUpColorInput.value;
    }

    return baseOptions;
}