// frontend/static/js/main.js
import { state } from './app/2-state.js';
import * as elements from './app/1-dom-elements.js';
import { getChartTheme } from './app/3-chart-options.js';
import { syncSettingsInputs, updateThemeToggleIcon, setAutomaticDateTime } from './app/4-ui-helpers.js';
import { recreateMainSeries } from './app/5-chart-drawing.js';
import { startSession } from './app/10-session-manager.js';
import { initializeAllEventListeners } from './app/7-event-listeners.js';
import { responsiveHandler } from './app/8-responsive-handler.js';

function initializeNewChartObject() {
    if (state.mainChart) state.mainChart.remove();
    
    state.mainChart = LightweightCharts.createChart(elements.chartContainer, getChartTheme(localStorage.getItem('chartTheme') || 'light'));
    state.mainSeries = null;
    state.volumeSeries = null;
    
    setTimeout(() => responsiveHandler.forceResize(), 100);
    
    syncSettingsInputs();
    recreateMainSeries(elements.chartTypeSelect.value);
    
    state.volumeSeries = state.mainChart.addHistogramSeries({ priceFormat: { type: 'volume' }, priceScaleId: '' });
    state.mainChart.priceScale('').applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } });
}

document.addEventListener('DOMContentLoaded', () => {
    // Basic UI setup
    const savedTheme = localStorage.getItem('chartTheme') || 'light';
    document.documentElement.setAttribute('data-theme', savedTheme);
    updateThemeToggleIcon();
    setAutomaticDateTime();

    // Initialize the chart object
    initializeNewChartObject();

    // Setup all event listeners from the new modules
    initializeAllEventListeners();
    
    // Start the session, which will trigger the initial data load
    startSession();
});