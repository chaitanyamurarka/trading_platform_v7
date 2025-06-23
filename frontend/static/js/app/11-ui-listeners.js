import * as elements from './1-dom-elements.js';
import { state } from './2-state.js';
import { applyTheme, setAutomaticDateTime } from './4-ui-helpers.js';
import { takeScreenshot, recreateMainSeries, applySeriesColors, applyVolumeColors } from './5-chart-drawing.js';
import { loadChartData } from './6-api-service.js';
import { disconnectFromAllLiveFeeds } from './9-websocket-service.js';

/**
 * Initializes all primary UI event listeners for the application.
 */
export function initializeAllEventListeners() {
    // --- Main Controls ---
    // Any change to these controls should trigger a full data reload.
    const reloadControls = [
        elements.exchangeSelect, 
        elements.symbolSelect, 
        elements.intervalSelect,
        elements.candleTypeSelect,
        elements.startTimeInput, 
        elements.endTimeInput,
        elements.timezoneSelect
    ];
    
    reloadControls.forEach(control => {
        control.addEventListener('change', loadChartData);
    });

    // --- Live Toggle ---
    elements.liveToggle.addEventListener('change', () => {
        if (elements.liveToggle.checked) {
            setAutomaticDateTime(); // Set time to now for live data
            loadChartData(); // Reload data, which will also connect the WebSocket
        } else {
            disconnectFromAllLiveFeeds(); // Manually disconnect if toggled off
        }
    });

    // --- Chart Display Controls ---
    elements.chartTypeSelect.addEventListener('change', () => {
        recreateMainSeries(elements.chartTypeSelect.value);
    });
    
    // --- Theme Toggle ---
    const themeToggleCheckbox = elements.themeToggle.querySelector('input[type="checkbox"]');
    themeToggleCheckbox.addEventListener('change', () => {
        applyTheme(themeToggleCheckbox.checked ? 'dark' : 'light');
    });

    // --- Other UI Actions ---
    elements.screenshotBtn.addEventListener('click', takeScreenshot);
    setupSettingsModalListeners();
    setupSidebarToggleListener();
    setupSettingsTabsListeners();
}


/**
 * Sets up listeners for the controls inside the settings modal.
 */
function setupSettingsModalListeners() {
    elements.gridColorInput.addEventListener('input', () => state.chart.applyOptions({ grid: { vertLines: { color: elements.gridColorInput.value }, horzLines: { color: elements.gridColorInput.value } } }));
    elements.watermarkInput.addEventListener('input', () => state.chart.applyOptions({ watermark: { text: elements.watermarkInput.value } }));
    
    [elements.upColorInput, elements.downColorInput, elements.wickUpColorInput, elements.wickDownColorInput, elements.disableWicksInput].forEach(input => {
        input.addEventListener('change', applySeriesColors);
    });

    [elements.volUpColorInput, elements.volDownColorInput].forEach(input => {
        input.addEventListener('change', applyVolumeColors);
    });

    elements.showOHLCLegendToggle.addEventListener('change', () => {
        state.showOHLCLegend = elements.showOHLCLegendToggle.checked;
        if (!state.showOHLCLegend) {
            elements.dataLegendElement.style.display = 'none';
        }
    });
}

/**
 * Sets up listeners for opening and closing the sidebar menu.
 */
function setupSidebarToggleListener() {
    if (elements.menuToggle && elements.sidebar && elements.sidebarOverlay) {
        const toggleSidebar = () => {
            elements.sidebar.classList.toggle('open');
            elements.sidebarOverlay.classList.toggle('hidden');
        };
        elements.menuToggle.addEventListener('click', toggleSidebar);
        elements.sidebarOverlay.addEventListener('click', toggleSidebar);
    }
}

/**
 * Sets up listeners for the tabs within the settings modal.
 */
function setupSettingsTabsListeners() {
    const tabsContainer = elements.settingsModal.querySelector('.tabs');
    if (!tabsContainer) return;

    tabsContainer.addEventListener('click', (event) => {
        const clickedTab = event.target.closest('.tab');
        if (!clickedTab) return;

        tabsContainer.querySelectorAll('.tab').forEach(tab => tab.classList.remove('tab-active'));
        clickedTab.classList.add('tab-active');

        elements.settingsModal.querySelectorAll('.tab-content').forEach(content => content.classList.add('hidden'));
        
        const targetContent = document.getElementById(clickedTab.dataset.tab);
        if (targetContent) {
            targetContent.classList.remove('hidden');
        }
    });
}