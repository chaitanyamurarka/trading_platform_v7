// frontend/static/js/app/11-ui-listeners.js
import * as elements from './1-dom-elements.js';
import { state } from './2-state.js';
import { applyTheme, syncSettingsInputs, showToast, setAutomaticDateTime } from './4-ui-helpers.js';
import { takeScreenshot, recreateMainSeries, applySeriesColors, applyVolumeColors } from './5-chart-drawing.js';
import { loadChartData } from './6-api-service.js';
import { connectToLiveDataFeed, connectToLiveHeikinAshiData, disconnectFromAllLiveFeeds } from './9-websocket-service.js';

export function setupUiListeners() {
    // --- MODIFICATION START ---
    // Controls that trigger a full data reload.
    // Interval, Candle Type, and Timezone now have dedicated listeners.
    [elements.exchangeSelect, elements.symbolSelect, elements.startTimeInput, elements.endTimeInput].forEach(control => {
        control.addEventListener('change', () => loadChartData(true));
    });

    /**
     * Handles changes to the timezone select dropdown.
     * Prevents changing the timezone if Live Mode is active and reverts to the previous state.
     */
    elements.timezoneSelect.addEventListener('change', (event) => {
        if (elements.liveToggle.checked) {
            showToast('Timezone cannot be changed while Live Mode is active.', 'warning');
            // Revert the dropdown's visible selection to the previous, valid timezone from the state
            event.target.value = state.timezone; 
            return;
        }
        // If not in live mode, it's a valid change.
        // Update the state with the new timezone, then reload the chart.
        state.timezone = event.target.value;
        loadChartData(true);
    });

    /**
     * Handles changes to the interval select dropdown.
     * Validates that the new interval is compatible with the selected candle type.
     */
    elements.intervalSelect.addEventListener('change', () => {
        const newInterval = elements.intervalSelect.value;
        const currentCandleType = elements.candleTypeSelect.value;

        // Validation: Prevent selecting a tick interval while on Heikin Ashi.
        if (newInterval.endsWith('tick') && currentCandleType === 'heikin_ashi') {
            showToast('Heikin Ashi is not compatible with Tick intervals.', 'error');
            elements.intervalSelect.value = state.interval; // Revert to last valid interval.
            return;
        }
        
        // Update state and reload chart
        state.interval = newInterval;
        if (state.interval.endsWith('tick')) {
            state.candleType = 'tick';
        } else {
            // If we switched from a tick interval, sync state.candleType with the UI.
            state.candleType = currentCandleType;
        }
        loadChartData(true);
    });

    /**
     * Handles changes to the candle type select dropdown.
     * Validates that the new candle type is compatible with the selected interval.
     */
    elements.candleTypeSelect.addEventListener('change', () => {
        const newCandleType = elements.candleTypeSelect.value;
        const currentInterval = elements.intervalSelect.value;

        // Validation: Prevent selecting Heikin Ashi while on a tick interval.
        if (newCandleType === 'heikin_ashi' && currentInterval.endsWith('tick')) {
            showToast('Heikin Ashi is not compatible with Tick intervals.', 'error');
            // Revert to last valid candle type. If it was 'tick', show 'regular' in the UI.
            elements.candleTypeSelect.value = state.candleType === 'tick' ? 'regular' : state.candleType;
            return;
        }

        // Update state and reload chart
        state.candleType = newCandleType;
        loadChartData(true);
    });
    // --- MODIFICATION END ---
    
    // Live Toggle
    elements.liveToggle.addEventListener('change', () => {
        const isLive = elements.liveToggle.checked;
        if (isLive) {
            setAutomaticDateTime();
            loadChartData(true);
        } else {
            disconnectFromAllLiveFeeds();
        }
    });

    // Chart Type (Candlestick, Bar, etc.)
    elements.chartTypeSelect.addEventListener('change', () => {
        recreateMainSeries(elements.chartTypeSelect.value);
    });

    // Theme Toggle
    const themeToggleCheckbox = elements.themeToggle.querySelector('input[type="checkbox"]');
    themeToggleCheckbox.addEventListener('change', () => {
        applyTheme(themeToggleCheckbox.checked ? 'dark' : 'light');
    });

    // Screenshot Button
    elements.screenshotBtn.addEventListener('click', takeScreenshot);

    // Settings Modal Listeners
    setupSettingsModalListeners();
    
    // Sidebar Toggle Listener
    setupSidebarToggleListener();

    // Settings Tabs Listeners
    setupSettingsTabsListeners();
}

// Unchanged functions below...
function setupSettingsModalListeners() {
    elements.gridColorInput.addEventListener('input', () => state.mainChart.applyOptions({ grid: { vertLines: { color: elements.gridColorInput.value }, horzLines: { color: elements.gridColorInput.value } } }));
    elements.watermarkInput.addEventListener('input', () => state.mainChart.applyOptions({ watermark: { text: elements.watermarkInput.value } }));
    
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

function setupSidebarToggleListener() {
    if (elements.menuToggle && elements.sidebar && elements.sidebarOverlay) {
        const toggleSidebar = (event) => {
            console.log('Sidebar toggle initiated by:', event.currentTarget);
            
            elements.sidebar.classList.toggle('open');
            elements.sidebarOverlay.classList.toggle('hidden');
        };

        elements.menuToggle.addEventListener('click', toggleSidebar);
        elements.sidebarOverlay.addEventListener('click', toggleSidebar);
    } else {
        console.error('Could not find all required elements for sidebar toggle functionality.');
    }
}

function setupSettingsTabsListeners() {
    const tabsContainer = elements.settingsModal.querySelector('.tabs');
    if (!tabsContainer) return;

    tabsContainer.addEventListener('click', (event) => {
        const clickedTab = event.target.closest('.tab');
        if (!clickedTab) return;

        tabsContainer.querySelectorAll('.tab').forEach(tab => tab.classList.remove('tab-active'));
        clickedTab.classList.add('tab-active');

        const tabContents = elements.settingsModal.querySelectorAll('.tab-content');
        tabContents.forEach(content => content.classList.add('hidden'));

        const targetTabId = clickedTab.dataset.tab;
        const targetContent = document.getElementById(targetTabId);
        if (targetContent) {
            targetContent.classList.remove('hidden');
        }
    });
}