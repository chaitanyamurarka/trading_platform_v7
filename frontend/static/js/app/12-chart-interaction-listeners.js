// frontend/static/js/app/12-chart-interaction-listeners.js
import { state } from './2-state.js';
import * as elements from './1-dom-elements.js';
import { fetchAndPrependRegularCandleChunk, fetchAndPrependHeikinAshiChunk } from './6-api-service.js';

function formatPrice(price, decimals = 2) {
    if (price === null || price === undefined) return 'N/A';
    return parseFloat(price).toFixed(decimals);
}

function formatVolume(volume) {
    if (volume === null || volume === undefined) return 'N/A';
    const num = parseInt(volume);
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num.toString();
}

function updateOHLCLegend(priceData, volumeData) {
    if (!state.showOHLCLegend || !priceData) {
        elements.dataLegendElement.style.display = 'none';
        return;
    }

    const symbol = elements.symbolSelect.value;
    const candleTypeLabel = state.candleType === 'heikin_ashi' ? 'HA' : 'Regular';
    const isBullish = priceData.close >= priceData.open;
    const changeColor = isBullish ? '#26a69a' : '#ef5350';
    const change = priceData.close - priceData.open;
    const changePercent = priceData.open !== 0 ? (change / priceData.open) * 100 : 0;
    
    elements.dataLegendElement.innerHTML = `
        <div class="space-y-1">
            <div class="font-bold text-sm">${symbol} <span class="text-xs text-blue-400">${candleTypeLabel}</span></div>
            <div class="flex items-center gap-3 text-xs">
                <span>O: <span class="font-mono">${formatPrice(priceData.open)}</span></span>
                <span>H: <span class="font-mono">${formatPrice(priceData.high)}</span></span>
                <span>L: <span class="font-mono">${formatPrice(priceData.low)}</span></span>
                <span>C: <span class="font-mono">${formatPrice(priceData.close)}</span></span>
                <span>Vol: <span class="font-mono">${formatVolume(volumeData?.value)}</span></span>
                <span style="color: ${changeColor}">Δ: ${change.toFixed(2)} (${changePercent.toFixed(2)}%)</span>
            </div>
        </div>`;
    elements.dataLegendElement.style.display = 'block';
}

function showLatestOHLCValues() {
    const currentPriceData = state.getCurrentChartData();
    const currentVolumeData = state.getCurrentVolumeData();
    if (!currentPriceData || currentPriceData.length === 0) {
        elements.dataLegendElement.style.display = 'none';
        return;
    }
    updateOHLCLegend(currentPriceData[currentPriceData.length - 1], currentVolumeData[currentVolumeData.length - 1]);
}

export function setupChartInteractionListeners() {
    if (!state.mainChart) return;

    state.mainChart.subscribeCrosshairMove((param) => {
        if (!param.time || !param.seriesPrices) {
            showLatestOHLCValues();
            return;
        }
        const priceData = param.seriesPrices.get(state.mainSeries);
        const volumeData = state.volumeSeries ? param.seriesPrices.get(state.volumeSeries) : null;
        updateOHLCLegend(priceData, volumeData);
    });
    
    window.addEventListener('resize', () => {
        if (state.mainChart) {
            state.mainChart.resize(elements.chartContainer.clientWidth, elements.chartContainer.clientHeight);
        }
    });
}

export function setupChartInfiniteScroll() {
    if (!state.mainChart) return;

    state.mainChart.timeScale().subscribeVisibleLogicalRangeChange((newRange) => {
        if (newRange && newRange.from <= 10 && !state.isAllDataLoaded() && !state.currentlyFetching) {
            if (state.candleType === 'heikin_ashi') {
                fetchAndPrependHeikinAshiChunk();
            } else {
                fetchAndPrependRegularCandleChunk();
            }
        }
    });
}