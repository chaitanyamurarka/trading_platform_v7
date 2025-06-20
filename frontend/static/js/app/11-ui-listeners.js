// frontend/static/js/app/11-ui-listeners.js
import * as elements from './1-dom-elements.js';
import { state } from './2-state.js';
import { applyTheme, syncSettingsInputs, showToast, setAutomaticDateTime } from './4-ui-helpers.js';
import { takeScreenshot, recreateMainSeries, applySeriesColors, applyVolumeColors } from './5-chart-drawing.js';
import { loadChartData } from './6-api-service.js';
import { connectToLiveDataFeed, connectToLiveHeikinAshiData, disconnectFromAllLiveFeeds } from './9-websocket-service.js';

function isTickInterval(interval) {
    return interval.endsWith('t');
}

export function setupUiListeners() {
    // Main chart controls
    [elements.exchangeSelect, elements.symbolSelect, elements.intervalSelect, elements.startTimeInput, elements.endTimeInput, elements.timezoneSelect, elements.candleTypeSelect].forEach(control => {
        control.addEventListener('change', () => {
            const selectedInterval = elements.intervalSelect.value;
            // Disable live toggle if a tick-based interval is selected
            elements.liveToggle.disabled = isTickInterval(selectedInterval);
            if (isTickInterval(selectedInterval) && elements.liveToggle.checked) {
                elements.liveToggle.checked = false; // Turn off live mode if tick interval is selected
                disconnectFromAllLiveFeeds();
                showToast("Live mode is not supported for tick intervals.", 'warning');
            }
            loadChartData();
        });
    });

    // Live Toggle
    elements.liveToggle.addEventListener('change', () => {
        const isLive = elements.liveToggle.checked;
        const selectedInterval = elements.intervalSelect.value;
        if (isTickInterval(selectedInterval)) {
            // This should ideally not happen due to the disabled property, but as a safeguard
            elements.liveToggle.checked = false;
            disconnectFromAllLiveFeeds();
            showToast("Live mode is not supported for tick intervals.", 'warning');
            return;
        }

        if (isLive) {
            setAutomaticDateTime();
            loadChartData(); // This will also trigger the appropriate live connection
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

    // NEW: Add Sidebar Toggle Listener
    setupSidebarToggleListener();

    // NEW: Add Settings Tabs Listeners
    setupSettingsTabsListeners(); // Add this line
}

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

// NEW: Function to handle sidebar toggling
function setupSidebarToggleListener() {
    if (elements.menuToggle && elements.sidebar && elements.sidebarOverlay) {
        const toggleSidebar = (event) => {
            // Log to the console for debugging
            console.log('Sidebar toggle initiated by:', event.currentTarget);

            elements.sidebar.classList.toggle('open');
            elements.sidebarOverlay.classList.toggle('hidden');
        };

        elements.menuToggle.addEventListener('click', toggleSidebar);
        elements.sidebarOverlay.addEventListener('click', toggleSidebar);
    } else {
        // This will show an error in the console if the elements aren't found
        console.error('Could not find all required elements for sidebar toggle functionality.');
    }
}

// NEW: Function to handle settings tabs
function setupSettingsTabsListeners() {
    const tabsContainer = elements.settingsModal.querySelector('.tabs');
    if (!tabsContainer) return;

    tabsContainer.addEventListener('click', (event) => {
        const clickedTab = event.target.closest('.tab');
        if (!clickedTab) return;

        // Remove active class from all tabs and add to clicked tab
        tabsContainer.querySelectorAll('.tab').forEach(tab => tab.classList.remove('tab-active'));
        clickedTab.classList.add('tab-active');

        // Hide all tab contents and show the selected one
        const tabContents = elements.settingsModal.querySelectorAll('.tab-content');
        tabContents.forEach(content => content.classList.add('hidden'));

        const targetTabId = clickedTab.dataset.tab;
        const targetContent = document.getElementById(targetTabId);
        if (targetContent) {
            targetContent.classList.remove('hidden');
        }
    });
}