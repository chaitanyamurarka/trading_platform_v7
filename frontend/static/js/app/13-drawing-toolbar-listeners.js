import * as elements from './1-dom-elements.js';
import { state } from './2-state.js';

export function setupDrawingToolbarListeners() {
    if (!elements.toolTrendLineBtn) return; // Exit if drawing tools are not in the DOM
    
    elements.toolTrendLineBtn.addEventListener('click', () => state.mainChart?.setActiveLineTool('TrendLine'));
    elements.toolHorizontalLineBtn.addEventListener('click', () => state.mainChart?.setActiveLineTool('HorizontalLine'));
    elements.toolFibRetracementBtn.addEventListener('click', () => state.mainChart?.setActiveLineTool('FibRetracement'));
    elements.toolRectangleBtn.addEventListener('click', () => state.mainChart?.setActiveLineTool('Rectangle'));
    elements.toolBrushBtn.addEventListener('click', () => state.mainChart?.setActiveLineTool('Brush'));
    elements.toolRemoveSelectedBtn.addEventListener('click', () => state.mainChart?.removeSelectedLineTools());
    elements.toolRemoveAllBtn.addEventListener('click', () => state.mainChart?.removeAllLineTools());

    // Scaling buttons
    const autoScaleBtn = document.getElementById('scaling-auto-btn');
    const linearScaleBtn = document.getElementById('scaling-linear-btn');

    autoScaleBtn?.addEventListener('click', () => {
        // Apply autoscale to the price and time axes
        state.mainChart?.priceScale().applyOptions({ autoScale: true });
        state.mainChart?.timeScale().applyOptions({ rightOffset: 12 });

        // --- NEW LOGIC ---
        // Set the visible range to the most recent 100 bars
        const currentData = state.getCurrentChartData();
        if (currentData && currentData.length > 0) {
            const dataSize = currentData.length;
            state.mainChart.timeScale().setVisibleLogicalRange({
                from: Math.max(0, dataSize - 100),
                to: dataSize - 1
            });
        }
        // --- END NEW LOGIC ---

        // --- FIX: This line forces the chart to scroll to the latest bar ---
        state.mainChart?.timeScale().scrollToRealTime();

        autoScaleBtn.classList.add('btn-active');
        linearScaleBtn.classList.remove('btn-active');
    });

    linearScaleBtn?.addEventListener('click', () => {
        state.mainChart?.priceScale().applyOptions({ autoScale: false });
        state.mainChart?.timeScale().applyOptions({ rightOffset: 0 });
        linearScaleBtn.classList.add('btn-active');
        autoScaleBtn.classList.remove('btn-active');
    });
}

export function applyAutoscaling() {
    const autoScaleBtn = document.getElementById('scaling-auto-btn');
    const linearScaleBtn = document.getElementById('scaling-linear-btn');

    if (!state.mainChart) return;


    // Apply autoscale to the price and time axes
    state.mainChart.priceScale().applyOptions({ autoScale: true });
    state.mainChart.timeScale().applyOptions({ rightOffset: 12 });

    // Set the visible range to the most recent 100 bars
    const currentData = state.getCurrentChartData();
    if (currentData && currentData.length > 0) {
        const dataSize = currentData.length;
        state.mainChart.timeScale().setVisibleLogicalRange({
            from: Math.max(0, dataSize - 100),
            to: dataSize - 1
        });
    }

    // Scroll to the far right of the chart to see the latest bar
    state.mainChart.timeScale().scrollToRealTime();

    if(autoScaleBtn && linearScaleBtn) {
        autoScaleBtn.classList.add('btn-active');
        linearScaleBtn.classList.remove('btn-active');
    }
}