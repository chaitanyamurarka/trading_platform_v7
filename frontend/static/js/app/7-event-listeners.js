// frontend/static/js/app/7-event-listeners.js
import * as elements from './1-dom-elements.js';
import { state } from './2-state.js';
import { applyTheme, updateThemeToggleIcon,showToast } from './4-ui-helpers.js';
import { takeScreenshot, recreateMainSeries, applySeriesColors, applyVolumeColors } from './5-chart-drawing.js';
import { loadInitialChart, fetchAndPrependDataChunk, connectToLiveDataFeed, disconnectFromLiveDataFeed, disconnectFromAllLiveFeeds, setAutomaticDateTime, loadChartData  } from './6-api-service.js';
import { reinitializeAndLoadChart } from '../main.js';

/**
 * Sets up infinite scroll for chart (works for both regular and Heikin Ashi)
 */
export function setupChartInfiniteScroll() {
    if (!state.mainChart) return;

    state.mainChart.timeScale().subscribeVisibleLogicalRangeChange((newRange) => {
        // Check if user scrolled to the left edge and we have more data
        if (newRange && newRange.from <= 10 && !state.isAllDataLoaded() && !state.currentlyFetching) {
            console.log(`Infinite scroll triggered for ${state.candleType} candles`);
            fetchAndPrependDataChunk();
        }
    });
}

// Get the data legend element
const dataLegend = document.getElementById('data-legend');

// Function to format price values
function formatPrice(price, decimals = 2) {
    if (price === null || price === undefined) return 'N/A';
    return parseFloat(price).toFixed(decimals);
}

// Function to format volume with appropriate suffixes
function formatVolume(volume) {
    if (volume === null || volume === undefined) return 'N/A';
    
    const num = parseInt(volume);
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    } else {
        return num.toString();
    }
}

function updateOHLCLegend(priceData, volumeData) {
    if (!state.showOHLCLegend) {
        dataLegend.style.display = 'none';
        return;
    }

    if (!priceData) {
        dataLegend.style.display = 'none';
        return;
    }

    const symbol = elements.symbolSelect.value || 'SYMBOL';
    
    // ADD THIS LINE - Define candleTypeLabel based on current candle type
    const candleTypeLabel = state.candleType === 'heikin_ashi' ? 'Heikin Ashi' : '';
    
    const open = formatPrice(priceData.open);
    const high = formatPrice(priceData.high);
    const low = formatPrice(priceData.low);
    const close = formatPrice(priceData.close);
    const volume = volumeData ? formatVolume(volumeData.value) : 'N/A';
    
    // Determine if the bar is bullish or bearish
    const isBullish = priceData.close >= priceData.open;
    const changeColor = isBullish ? '#26a69a' : '#ef5350';
    
    // Calculate price change and percentage
    const change = (parseFloat(priceData.close) - parseFloat(priceData.open));
    const changePercent = parseFloat(priceData.open) !== 0 ? (change / parseFloat(priceData.open)) * 100 : 0;
    const changeStr = (change >= 0 ? '+' : '') + formatPrice(change);
    const changePercentStr = (changePercent >= 0 ? '+' : '') + changePercent.toFixed(2) + '%';
    
    // Use horizontal layout (Option 2 from previous response)
    dataLegend.innerHTML = `
        <div class="space-y-1">
            <div class="font-bold text-sm">
                ${symbol} ${candleTypeLabel ? `<span class="text-xs text-blue-400">${candleTypeLabel}</span>` : ''}
            </div>
            <div class="flex items-center gap-3 text-xs">
                <span><span class="text-gray-400">O:</span> <span class="font-mono">${open}</span></span>
                <span><span class="text-gray-400">H:</span> <span class="font-mono">${high}</span></span>
                <span><span class="text-gray-400">L:</span> <span class="font-mono">${low}</span></span>
                <span><span class="text-gray-400">C:</span> <span class="font-mono">${close}</span></span>
                <span><span class="text-gray-400">Vol:</span> <span class="font-mono">${volume}</span></span>
                <span class="ml-2">
                    <span class="text-gray-400">Δ:</span>
                    <span class="font-mono" style="color: ${changeColor}">${changeStr} (${changePercentStr})</span>
                </span>
            </div>
        </div>
    `;
    
    dataLegend.style.display = 'block';
}

// Also update your showLatestOHLCValues function to handle both candle types:
function showLatestOHLCValues() {
    // Use the appropriate data array based on current candle type
    const currentData = state.candleType === 'heikin_ashi' ? state.allHeikinAshiData : state.allChartData;
    
    if (!currentData || currentData.length === 0) {
        dataLegend.style.display = 'none';
        return;
    }
    
    // Get the latest price data
    const latestPriceData = currentData[currentData.length - 1];
    
    // Get the latest volume data if available
    let latestVolumeData = null;
    if (state.allVolumeData && state.allVolumeData.length > 0) {
        latestVolumeData = state.allVolumeData[state.allVolumeData.length - 1];
    }
    
    updateOHLCLegend(latestPriceData, latestVolumeData);
}

export function setupChartObjectListeners() {
    if (!state.mainChart) return;

    // Subscribe to crosshair move events for OHLC tooltip
    state.mainChart.subscribeCrosshairMove((param) => {

        if (!state.showOHLCLegend) {
            dataLegend.style.display = 'none';
            return;
        }

        // Check if we have valid parameters and the necessary data structures
        if (!param || !param.time || !param.seriesPrices) {
            // Show latest values when not hovering over any specific bar
            showLatestOHLCValues();
            return;
        }

        // Ensure we have the main series initialized
        if (!state.mainSeries) {
            console.warn('Main series not initialized yet');
            showLatestOHLCValues();
            return;
        }

        // Safely get the price data for the hovered bar
        let priceData = null;
        try {
            priceData = param.seriesPrices.get(state.mainSeries);
        } catch (error) {
            console.error('Error getting price data from seriesPrices:', error);
            showLatestOHLCValues();
            return;
        }

        // If we don't have price data for this point, show latest values
        if (!priceData) {
            showLatestOHLCValues();
            return;
        }
        
        // --- MODIFICATION START ---
        // Find the corresponding volume data from the state array using the timestamp
        let volumeData = null;
        if (state.allVolumeData && state.allVolumeData.length > 0) {
            // Use a Map for efficient lookups
            const volumeMap = new Map(state.allVolumeData.map(item => [item.time, item]));
            volumeData = volumeMap.get(param.time);
        }
        // --- MODIFICATION END ---
        
        updateOHLCLegend(priceData, volumeData);
    });

    // Subscribe to visible logical range changes for pagination
    state.mainChart.timeScale().subscribeVisibleLogicalRangeChange(async (newVisibleRange) => {
        if (!newVisibleRange || state.currentlyFetching || state.allDataLoaded || !state.chartRequestId) return;
        if (newVisibleRange.from < 15) {
            await fetchAndPrependDataChunk();
        }
    });

    // Handle window resize
    window.addEventListener('resize', () => {
        if (state.mainChart) {
            state.mainChart.resize(elements.chartContainer.clientWidth, elements.chartContainer.clientHeight);
        }
    });

    // Show latest values when chart is first loaded
    setTimeout(() => {
        showLatestOHLCValues();
    }, 100);
}

// Helper function to safely add event listener to element
function safeAddEventListener(element, event, handler) {
    if (element && typeof element.addEventListener === 'function') {
        element.addEventListener(event, handler);
        return true;
    }
    console.warn(`Element not found or invalid for event listener:`, element);
    return false;
}

export function setupControlListeners(reloadChartCallback) {
    // Handle changes for controls that define the chart's main context
    [elements.exchangeSelect, elements.symbolSelect, elements.intervalSelect].forEach(control => {
        safeAddEventListener(control, 'change', () => {
            if (elements.liveToggle && elements.liveToggle.checked) {
                loadInitialChart().then(() => {
                    if (state.sessionToken) {
                        const symbol = elements.symbolSelect.value;
                        const interval = elements.intervalSelect.value;
                        connectToLiveDataFeed(symbol, interval);
                    }
                });
            } else {
                reloadChartCallback();
            }
        });
    });

    // --- FIX: Handle Start/End time changes separately ---
    // These controls should still disable live mode.
    [elements.startTimeInput, elements.endTimeInput].forEach(control => {
        safeAddEventListener(control, 'change', () => {
            if (elements.liveToggle && elements.liveToggle.checked) {
                elements.liveToggle.checked = false; // This will trigger the liveToggle's own 'change' listener to disconnect.
            }
            reloadChartCallback();
        });
    });

    let previousTimezoneValue = elements.timezoneSelect ? elements.timezoneSelect.value : '';

    safeAddEventListener(elements.timezoneSelect, 'focus', () => {
        previousTimezoneValue = elements.timezoneSelect.value;
    });

    // --- FIX: Create a dedicated listener for the timezone selector ---
    safeAddEventListener(elements.timezoneSelect, 'change', () => {
        if (elements.liveToggle && elements.liveToggle.checked) {
            showToast('Cannot change timezone while Live mode is ON. Please turn off Live mode first.', 'warning');
            elements.timezoneSelect.value = previousTimezoneValue;
            return;
        }
        setAutomaticDateTime();
        reloadChartCallback();
    });

    // Chart type change listener
    safeAddEventListener(elements.chartTypeSelect, 'change', () => {
        recreateMainSeries(elements.chartTypeSelect.value);
        // Update the legend after chart type change
        setTimeout(() => {
            showLatestOHLCValues();
        }, 100);
    });

    // Theme toggle listener
    const themeToggleCheckbox = elements.themeToggle ? elements.themeToggle.querySelector('input[type="checkbox"]') : null;
    if (themeToggleCheckbox) {
        safeAddEventListener(themeToggleCheckbox, 'change', () => {
            const newTheme = themeToggleCheckbox.checked ? 'dark' : 'light';
            applyTheme(newTheme);
            // The call to updateThemeToggleIcon() is no longer needed here because the
            // browser has already changed the checkbox state, which controls the icon.
            
            // Update legend after theme change
            setTimeout(() => {
                showLatestOHLCValues();
            }, 100);
        });
    }

    // Live toggle listener
    safeAddEventListener(elements.liveToggle, 'change', () => {
        const isLiveMode = elements.liveToggle.checked;
        
        if (isLiveMode) {
            // Switching to live mode
            setAutomaticDateTime();
            loadInitialChart().then(() => {
                if (state.sessionToken) {
                    const symbol = elements.symbolSelect.value;
                    const interval = elements.intervalSelect.value;
                    connectToLiveDataFeed(symbol, interval);
                }
            });
        } else {
            // Switching to historical mode
            disconnectFromLiveDataFeed();
        }
    });

    // Screenshot button listener
    safeAddEventListener(elements.screenshotBtn, 'click', takeScreenshot);

    // Settings modal listeners
    safeAddEventListener(elements.bgColorInput, 'change', () => {
        if (state.mainChart) {
            state.mainChart.applyOptions({
                layout: { background: { color: elements.bgColorInput.value } }
            });
        }
    });

    safeAddEventListener(elements.gridColorInput, 'change', () => {
        if (state.mainChart) {
            state.mainChart.applyOptions({
                grid: {
                    vertLines: { color: elements.gridColorInput.value },
                    horzLines: { color: elements.gridColorInput.value }
                }
            });
        }
    });

    safeAddEventListener(elements.watermarkInput, 'change', () => {
        if (state.mainChart) {
            state.mainChart.applyOptions({
                watermark: { text: elements.watermarkInput.value }
            });
        }
    });

    // Series color change listeners
    [elements.upColorInput, elements.downColorInput, elements.wickUpColorInput, elements.wickDownColorInput].forEach(input => {
        safeAddEventListener(input, 'change', applySeriesColors);
    });

    // Volume color change listeners
    [elements.volUpColorInput, elements.volDownColorInput].forEach(input => {
        safeAddEventListener(input, 'change', applyVolumeColors);
    });

    // Disable wicks listener
    safeAddEventListener(elements.disableWicksInput, 'change', applySeriesColors);

    // --- CORRECTED Drawing tools listeners ---
    safeAddEventListener(elements.toolTrendLineBtn, 'click', () => {
        if (state.mainChart) {
            state.mainChart.setActiveLineTool('TrendLine');
        }
    });

    safeAddEventListener(elements.toolHorizontalLineBtn, 'click', () => {
        if (state.mainChart) {
            state.mainChart.setActiveLineTool('HorizontalLine');
        }
    });

    safeAddEventListener(elements.toolFibRetracementBtn, 'click', () => {
        if (state.mainChart) {
            state.mainChart.setActiveLineTool('FibRetracement');
        }
    });

    safeAddEventListener(elements.toolRectangleBtn, 'click', () => {
        if (state.mainChart) {
            state.mainChart.setActiveLineTool('Rectangle');
        }
    });

   safeAddEventListener(elements.toolBrushBtn, 'click', () => {
        if (state.mainChart) {
            state.mainChart.setActiveLineTool('Brush');
        }
    });

    safeAddEventListener(elements.toolRemoveSelectedBtn, 'click', () => {
        if (state.mainChart) {
            state.mainChart.removeSelectedLineTools();
        }
    });

    safeAddEventListener(elements.toolRemoveAllBtn, 'click', () => {
        if (state.mainChart) {
            state.mainChart.removeAllLineTools();
        }
    });

    const autoScaleBtn = document.getElementById('scaling-auto-btn');
    const linearScaleBtn = document.getElementById('scaling-linear-btn');

    if (autoScaleBtn) {
        autoScaleBtn.addEventListener('click', () => {
            if (!state.mainChart) return;

            // 1. Apply autoscale to the price axis
            state.mainChart.applyOptions({
                rightPriceScale: { autoScale: true },
                leftPriceScale: { autoScale: true }
            });

            // 2. --- MODIFIED: Enable auto-scrolling to the latest bar ---
            // This will keep a small margin from the right edge, making new bars visible
            state.mainChart.timeScale().applyOptions({ rightOffset: 12 });

            if (state.allChartData.length > 0) {
            const dataSize = state.allChartData.length;
            state.mainChart.timeScale().setVisibleLogicalRange({
                from: Math.max(0, dataSize - 100),
                to: dataSize - 1,
            });
            } else {
                state.mainChart.timeScale().fitContent();
            }

            // 3. Update button styles
            autoScaleBtn.classList.add('btn-active');
            linearScaleBtn.classList.remove('btn-active');
        });
    }

    if (linearScaleBtn) {
        linearScaleBtn.addEventListener('click', () => {
            if (!state.mainChart) return;
            state.mainChart.applyOptions({
                rightPriceScale: { autoScale: false },
                leftPriceScale: { autoScale: false }
            });

            // --- NEW: Disable auto-scrolling when switching to linear scale ---
            state.mainChart.timeScale().applyOptions({ rightOffset: 0 });

            linearScaleBtn.classList.add('btn-active');
            autoScaleBtn.classList.remove('btn-active');
        });
    }

    safeAddEventListener(elements.showOHLCLegendToggle, 'change', () => {
        state.showOHLCLegend = elements.showOHLCLegendToggle.checked;
        if (!state.showOHLCLegend) {
            dataLegend.style.display = 'none'; // Immediately hide if unchecked
        }
    });
}

/**
 * Handle candle type selection change
 */
export function setupCandleTypeListener() {
        elements.candleTypeSelect.addEventListener('change', async (event) => {
        const newCandleType = event.target.value;
        
        if (newCandleType === state.candleType) return; // No change
        
        state.candleType = newCandleType;
        
        // Update UI to show loading state
        showToast(`Switching to ${newCandleType === 'heikin_ashi' ? 'Heikin Ashi' : 'Regular'} candles...`, 'info');
        
        try {
            await loadChartData();
            
            // Update legend to show candle type
            if (state.allData?.length > 0 || state.allHeikinAshiData?.length > 0) {
                const latestData = newCandleType === 'heikin_ashi' ? 
                    state.allHeikinAshiData?.at(-1) : 
                    state.allChartData?.at(-1);
                    
                // if (latestData) {
                //     updateDataSummary(latestData);
                // }
            }
            
            showToast(`Switched to ${newCandleType === 'heikin_ashi' ? 'Heikin Ashi' : 'Regular'} candles`, 'success');
        } catch (error) {
            console.error('Error switching candle type:', error);
            showToast('Error switching candle type', 'error');
            // Revert selection on error
            elements.candleTypeSelect.value = state.candleType;
        }
    });
}

/**
 * Enhanced function to update the OHLC legend with candle type indicator
 */
function updateOHLCLegendEnhanced(priceData, volumeData) {
    if (!state.showOHLCLegend) {
        dataLegend.style.display = 'none';
        return;
    }

    if (!priceData) {
        dataLegend.style.display = 'none';
        return;
    }

    const symbol = elements.symbolSelect.value || 'SYMBOL';
    const candleTypeLabel = state.candleType === 'heikin_ashi' ? 'HA' : '';
    const open = formatPrice(priceData.open);
    const high = formatPrice(priceData.high);
    const low = formatPrice(priceData.low);
    const close = formatPrice(priceData.close);
    const volume = volumeData ? formatVolume(volumeData.value) : 'N/A';
    
    // Determine if the bar is bullish or bearish
    const isBullish = priceData.close >= priceData.open;
    const changeColor = isBullish ? '#26a69a' : '#ef5350';
    
    // Calculate price change and percentage
    const change = (parseFloat(priceData.close) - parseFloat(priceData.open));
    const changePercent = parseFloat(priceData.open) !== 0 ? (change / parseFloat(priceData.open)) * 100 : 0;
    const changeStr = (change >= 0 ? '+' : '') + formatPrice(change);
    const changePercentStr = (changePercent >= 0 ? '+' : '') + changePercent.toFixed(2) + '%';
    
    dataLegend.innerHTML = `
        <div class="flex items-center text-xs bg-black bg-opacity-20 px-2 py-1 rounded">
            <span class="font-bold text-sm mr-2">
                ${symbol} ${candleTypeLabel ? `<span class="text-xs text-blue-400">${candleTypeLabel}</span>` : ''}
            </span>
            <span class="text-gray-400">O</span><span class="font-mono mx-1">${open}</span><span class="text-gray-500">|</span>
            <span class="text-gray-400 ml-1">H</span><span class="font-mono mx-1">${high}</span><span class="text-gray-500">|</span>
            <span class="text-gray-400 ml-1">L</span><span class="font-mono mx-1">${low}</span><span class="text-gray-500">|</span>
            <span class="text-gray-400 ml-1">C</span><span class="font-mono mx-1" style="color: ${changeColor}">${close}</span><span class="text-gray-500">|</span>
            <span class="text-gray-400 ml-1">V</span><span class="font-mono mx-1">${volume}</span><span class="text-gray-500">|</span>
            <span class="text-gray-400 ml-1">Δ</span><span class="font-mono ml-1" style="color: ${changeColor}">${changeStr} (${changePercentStr})</span>
        </div>
    `;
        
    dataLegend.style.display = 'block';
}

// ===== 7. Crosshair event handler update for both candle types =====

export function setupChartObjectListenersEnhanced() {
    if (!state.mainChart) return;

    // Subscribe to crosshair move events for OHLC tooltip
    state.mainChart.subscribeCrosshairMove(param => {
        if (!param.point || !param.time) {
            // Show latest values when not hovering
            showLatestOHLCValuesEnhanced();
            return;
        }

        // Get price data based on current candle type
        const currentData = state.candleType === 'heikin_ashi' ? state.allHeikinAshiData : state.allChartData;
        const priceData = param.seriesData.get(state.mainSeries);
        
        // Get volume data
        let volumeData = null;
        if (state.volumeSeries && state.allVolumeData) {
            volumeData = param.seriesData.get(state.volumeSeries);
        }

        updateOHLCLegendEnhanced(priceData, volumeData);
    });
}

function showLatestOHLCValuesEnhanced() {
    const currentData = state.candleType === 'heikin_ashi' ? state.allHeikinAshiData : state.allChartData;
    
    if (!currentData || currentData.length === 0) {
        dataLegend.style.display = 'none';
        return;
    }
    
    const latestPriceData = currentData[currentData.length - 1];
    
    let latestVolumeData = null;
    if (state.allVolumeData && state.allVolumeData.length > 0) {
        latestVolumeData = state.allVolumeData[state.allVolumeData.length - 1];
    }
    
    updateOHLCLegendEnhanced(latestPriceData, latestVolumeData);
}