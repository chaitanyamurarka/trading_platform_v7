import { state } from './2-state.js';
import * as elements from './1-dom-elements.js';
import { getSeriesOptions } from './3-chart-options.js';

/**
 * Automatically adjusts the chart's visible range to fit all the data.
 * This function is now exported to be used by other modules like the api-service.
 */
export function applyAutoscaling() {
    if (!state.mainChart) return;
    state.mainChart.timeScale().fitContent();
}

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
            state.mainSeries = state.mainChart.addAreaSeries({ 
                lineColor: seriesOptions.upColor, 
                topColor: `${seriesOptions.upColor}66`, 
                bottomColor: `${seriesOptions.upColor}00` 
            });
            break;
        default:
            state.mainSeries = state.mainChart.addCandlestickSeries(seriesOptions);
            break;
    }
    
    // Get the correct data array for the currently active chart type.
    const currentData = state.candleType === 'tick' ? state.allTickData 
                      : state.candleType === 'heikin_ashi' ? state.allHeikinAshiData 
                      : state.allChartData;

    if (currentData && currentData.length > 0) {
        state.mainSeries.setData(currentData);
    }
}


export function applySeriesColors() {
    if (!state.mainSeries) return;
    
    // Always recreate the series to ensure wick changes take effect
    recreateMainSeries(elements.chartTypeSelect.value);
}

export function applyVolumeColors() {
    if (!state.volumeSeries) return;
    
    const currentData = state.candleType === 'tick' ? state.allTickData
                      : state.candleType === 'heikin_ashi' ? state.allHeikinAshiData
                      : state.allChartData;

    const currentVolume = state.candleType === 'tick' ? state.allTickVolumeData
                        : state.candleType === 'heikin_ashi' ? state.allHeikinAshiVolumeData
                        : state.allVolumeData;

    if (!currentData.length || !currentVolume.length) return;

    const priceActionMap = new Map();
    currentData.forEach(priceData => {
        priceActionMap.set(priceData.time, priceData.close >= priceData.open);
    });
    const newVolumeData = currentVolume.map(volumeData => ({
        ...volumeData,
        color: priceActionMap.get(volumeData.time) 
            ? elements.volUpColorInput.value + '80' 
            : elements.volDownColorInput.value + '80',
    }));

    if(state.candleType === 'tick') state.allTickVolumeData = newVolumeData;
    else if(state.candleType === 'heikin_ashi') state.allHeikinAshiVolumeData = newVolumeData;
    else state.allVolumeData = newVolumeData;

    state.volumeSeries.setData(newVolumeData);
}

export function takeScreenshot() {
    if (!state.mainChart) return;
    const canvas = state.mainChart.takeScreenshot();
    const link = document.createElement('a');
    link.href = canvas.toDataURL();
    link.download = `chart-screenshot-${new Date().toISOString()}.png`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}