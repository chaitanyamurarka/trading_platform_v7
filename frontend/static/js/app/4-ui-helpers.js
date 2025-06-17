// app/4-ui-helpers.js
import * as elements from './1-dom-elements.js';
import { getChartTheme } from './3-chart-options.js';
import { state } from './2-state.js';

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

export function updateDataSummary(latestData) {
    if (!elements.dataSummaryElement || !latestData) return;
    const change = latestData.close - latestData.open;
    const changePercent = (change / latestData.open) * 100;
    elements.dataSummaryElement.innerHTML = `
        <strong>${elements.symbolSelect.value} (${elements.exchangeSelect.value})</strong> | C: ${latestData.close.toFixed(2)} | H: ${latestData.high.toFixed(2)} | L: ${latestData.low.toFixed(2)} | O: ${latestData.open.toFixed(2)}
        <span class="${change >= 0 ? 'text-success' : 'text-error'}">(${change.toFixed(2)} / ${changePercent.toFixed(2)}%)</span>`;
}

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
