// frontend/static/js/main.js
import { state } from './app/2-state.js';
import * as elements from './app/1-dom-elements.js';
import { getChartTheme } from './app/3-chart-options.js';
import { syncSettingsInputs, updateThemeToggleIcon } from './app/4-ui-helpers.js';
import { recreateMainSeries } from './app/5-chart-drawing.js';
// --- MODIFIED: Import the new setAutomaticDateTime function ---
import { startSession, loadInitialChart, setAutomaticDateTime } from './app/6-api-service.js';
import { setupChartObjectListeners, setupControlListeners,setupCandleTypeListener, setupChartInfiniteScroll  } from './app/7-event-listeners.js';
import { responsiveHandler, onResize } from './app/8-responsive-handler.js';


function initializeNewChartObject() {
    if (state.mainChart) {
        state.mainChart.remove();
    }
    state.mainChart = null;
    state.mainSeries = null;
    state.volumeSeries = null;
    state.mainChart = LightweightCharts.createChart(elements.chartContainer, getChartTheme(localStorage.getItem('chartTheme') || 'light'));
    
    // Initialize responsive behavior
    setTimeout(() => {
        responsiveHandler.forceResize();
    }, 100);
    
    setupChartObjectListeners();
    syncSettingsInputs();
    recreateMainSeries(elements.chartTypeSelect.value);
    state.volumeSeries = state.mainChart.addHistogramSeries({ priceFormat: { type: 'volume' }, priceScaleId: '' });
    state.mainChart.priceScale('').applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } });
}

document.addEventListener('DOMContentLoaded', () => {
    const tabs = document.querySelectorAll('.modal-box .tabs .tab');
    const tabContents = document.querySelectorAll('.modal-box .tab-content');

    tabs.forEach(tab => {
        tab.addEventListener('click', (event) => {
            // Prevent the default anchor tag behavior
            event.preventDefault();

            // Deactivate all tabs
            tabs.forEach(t => t.classList.remove('tab-active'));
            // Activate the clicked tab
            tab.classList.add('tab-active');

            // Hide all tab content panels
            tabContents.forEach(content => {
                if (!content.classList.contains('hidden')) {
                    content.classList.add('hidden');
                }
            });

            // Get the ID of the content to show from the tab's data attribute
            const targetId = tab.getAttribute('data-tab');
            const targetContent = document.getElementById(targetId);

            // Show the corresponding content
            if (targetContent) {
                targetContent.classList.remove('hidden');
            }
        });
    });
});

export function reinitializeAndLoadChart() {
    console.log("Re-initializing chart from fresh...");
    initializeNewChartObject();
    loadInitialChart();
}

document.addEventListener('DOMContentLoaded', () => {
    const savedTheme = localStorage.getItem('chartTheme') || 'light';
    document.documentElement.setAttribute('data-theme', savedTheme);
    
    updateThemeToggleIcon();

    // --- MODIFIED: Call setAutomaticDateTime() on page load ---
    // This sets the default time range ONCE.
    setAutomaticDateTime();
    setupChartInfiniteScroll();
    setupCandleTypeListener(); // Add this line
    
    setupControlListeners(reinitializeAndLoadChart);
    initializeNewChartObject();
    startSession(); // This will call loadInitialChart, which now uses the pre-filled values
});