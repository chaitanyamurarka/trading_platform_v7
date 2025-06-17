// frontend/static/js/app/7-event-listeners.js
import { setupUiListeners } from './11-ui-listeners.js';
import { setupChartInteractionListeners, setupChartInfiniteScroll } from './12-chart-interaction-listeners.js';
import { setupDrawingToolbarListeners } from './13-drawing-toolbar-listeners.js';

export function initializeAllEventListeners() {
    setupUiListeners();
    setupChartInteractionListeners();
    setupChartInfiniteScroll();
    setupDrawingToolbarListeners();
}