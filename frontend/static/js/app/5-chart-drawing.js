// frontend/static/js/app/5-chart-drawing.js
import { state } from './2-state.js';
import * as elements from './1-dom-elements.js';
import { getSeriesOptions } from './3-chart-options.js';

export function recreateMainSeries(type) {
    if (state.mainSeries) {
        state.mainChart.removeSeries(state.mainSeries);
    }
    const seriesOptions = getSeriesOptions();
    switch (type) {
        case 'bar':
            state.mainSeries = state.mainChart.addBarSeries(seriesOptions);
            break;
        case 'line':
            state.mainSeries = state.mainChart.addLineSeries({ color: seriesOptions.upColor });
            break;
        case 'area':
            state.mainSeries = state.mainChart.addAreaSeries({ lineColor: seriesOptions.upColor, topColor: `${seriesOptions.upColor}66`, bottomColor: `${seriesOptions.upColor}00` });
            break;
        default:
            state.mainSeries = state.mainChart.addCandlestickSeries(seriesOptions);
            break;
    }
    if (state.allChartData.length > 0) {
        state.mainSeries.setData(state.allChartData);
    }
}

export function applySeriesColors() {
    if (!state.mainSeries) return;
    state.mainSeries.applyOptions(getSeriesOptions());
}

export function applyVolumeColors() {
    if (!state.volumeSeries || !state.allChartData.length || !state.allVolumeData.length) return;
    const priceActionMap = new Map();
    state.allChartData.forEach(priceData => {
        priceActionMap.set(priceData.time, priceData.close >= priceData.open);
    });
    const newVolumeData = state.allVolumeData.map(volumeData => ({
        ...volumeData,
        color: priceActionMap.get(volumeData.time) ? elements.volUpColorInput.value + '80' : elements.volDownColorInput.value + '80',
    }));
    state.allVolumeData = newVolumeData;
    state.volumeSeries.setData(state.allVolumeData);
}

// --- MODIFIED: Corrected takeScreenshot function ---
export function takeScreenshot() {
    if (!state.mainChart) return;
    // The takeScreenshot() method returns a canvas element directly, not a promise.
    const canvas = state.mainChart.takeScreenshot();
    const link = document.createElement('a');
    link.href = canvas.toDataURL();
    link.download = `chart-screenshot-${new Date().toISOString()}.png`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}