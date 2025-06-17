// app/4-ui-helpers.js
import * as elements from './1-dom-elements.js';
import { getChartTheme } from './3-chart-options.js';
import { state } from './2-state.js';


export function setAutomaticDateTime() {
    const selectedTimezone = elements.timezoneSelect.value || 'America/New_York';

    const now = new Date();
    const nyParts = getDatePartsInZone(now, 'America/New_York');

    // Create a Date object representing 8:00 PM New York time
    const eightPMNY = new Date(Date.UTC(nyParts.year, nyParts.month - 1, nyParts.day, 0, 0, 0));
    eightPMNY.setUTCHours(getUTCHourOffset('America/New_York', 20, now));

    // If NY current date is same but time < 20:00 → subtract a day
    const currentNY = new Date();
    const currentParts = getDatePartsInZone(currentNY, 'America/New_York');

    if (currentParts.year === nyParts.year && currentParts.month === nyParts.month && currentParts.day === nyParts.day) {
        const nowNY = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
        const endNY = new Date(nowNY);
        endNY.setHours(20, 0, 0, 0); // set 8 PM NY today

        if (nowNY < endNY) {
            endNY.setDate(endNY.getDate() - 1);
        }
    }

    const finalEndUTC = new Date(eightPMNY);
    const finalStartUTC = new Date(finalEndUTC);
    finalStartUTC.setUTCDate(finalEndUTC.getUTCDate() - 30);

    const startFormatted = formatDateInZone(finalStartUTC, selectedTimezone);
    const endFormatted = formatDateInZone(finalEndUTC, selectedTimezone);

    elements.startTimeInput.value = startFormatted;
    elements.endTimeInput.value = endFormatted;

    console.log(`[${selectedTimezone}] Start: ${startFormatted}, End: ${endFormatted}`);
}

function formatDateInZone(date, timeZone) {
    const parts = new Intl.DateTimeFormat('en-US', {
        timeZone,
        year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit',
        hour12: false
    }).formatToParts(date);

    const map = Object.fromEntries(parts.map(p => [p.type, p.value]));
    return `${map.year}-${map.month}-${map.day}T${map.hour}:${map.minute}`;
}

function getCurrentHourInTimezone(timeZone) {
    const now = new Date();
    const parts = new Intl.DateTimeFormat('en-US', {
        timeZone,
        hour: '2-digit',
        hour12: false
    }).formatToParts(now);
    return parseInt(parts.find(p => p.type === 'hour').value, 10);
}

function getDatePartsInZone(date, timeZone) {
    const parts = new Intl.DateTimeFormat('en-US', {
        timeZone,
        year: 'numeric', month: '2-digit', day: '2-digit'
    }).formatToParts(date);

    return Object.fromEntries(parts.map(p => [p.type, parseInt(p.value, 10)]));
}

function getUTCHourOffset(timeZone, targetHourInZone, referenceDate) {
    const testDate = new Date(referenceDate);
    testDate.setUTCHours(0, 0, 0, 0); // midnight UTC

    const zoneHour = new Intl.DateTimeFormat('en-US', {
        timeZone,
        hour: '2-digit',
        hour12: false
    }).formatToParts(testDate).find(p => p.type === 'hour').value;

    const offset = targetHourInZone - parseInt(zoneHour, 10);
    return 0 + offset;
}

export function showToast(message, type = 'info') {
    const toastContainer = document.getElementById('toast-container');
    if (!toastContainer) return;

    const toast = document.createElement('div');
    toast.className = `alert alert-${type} shadow-lg transition-opacity duration-300 opacity-0`;
    toast.innerHTML = `<span>${message}</span>`;

    toastContainer.appendChild(toast);

    // Animate in
    requestAnimationFrame(() => {
        toast.classList.remove('opacity-0');
        toast.classList.add('opacity-100');
    });

    // Auto-dismiss after 4s
    setTimeout(() => {
        toast.classList.remove('opacity-100');
        toast.classList.add('opacity-0');
        setTimeout(() => toast.remove(), 300);
    }, 4000);
}

// export function updateDataSummary(latestData) {
//     if (!elements.dataSummaryElement || !latestData) return;
//     const change = latestData.close - latestData.open;
//     const changePercent = (change / latestData.open) * 100;
//     elements.dataSummaryElement.innerHTML = `
//         <strong>${elements.symbolSelect.value} (${elements.exchangeSelect.value})</strong> | C: ${latestData.close.toFixed(2)} | H: ${latestData.high.toFixed(2)} | L: ${latestData.low.toFixed(2)} | O: ${latestData.open.toFixed(2)}
//         <span class="${change >= 0 ? 'text-success' : 'text-error'}">(${change.toFixed(2)} / ${changePercent.toFixed(2)}%)</span>`;
// }

export function applyTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('chartTheme', theme);
    if (state.mainChart) state.mainChart.applyOptions(getChartTheme(theme));
    syncSettingsInputs();
}

export function syncSettingsInputs() {
    const currentTheme = getChartTheme(localStorage.getItem('chartTheme') || 'light');
    elements.bgColorInput.value = currentTheme.layout.background.color;
    elements.gridColorInput.value = currentTheme.grid.vertLines.color;
    elements.upColorInput.value = '#10b981';
    elements.downColorInput.value = '#ef4444';
    elements.wickUpColorInput.value = '#10b981';
    elements.wickDownColorInput.value = '#ef4444';
    elements.volUpColorInput.value = '#10b981';
    elements.volDownColorInput.value = '#ef4444';
}

export function updateThemeToggleIcon() {
    const theme = document.documentElement.getAttribute('data-theme');
    const toggleCheckbox = elements.themeToggle.querySelector('input[type="checkbox"]');
    
    if (toggleCheckbox) {
        // The "swap-on" (sun icon) should be active when the theme is dark.
        // The checkbox being 'checked' activates "swap-on".
        toggleCheckbox.checked = theme === 'dark';
    }
}

window.showToast = showToast;
