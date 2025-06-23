import * as api from '../api.js';
import { state } from './2-state.js';
import * as elements from './1-dom-elements.js';
import { showToast } from './4-ui-helpers.js';

let liveDataSocket = null;

/**
 * Connects to the unified live data WebSocket endpoint.
 */
export function connectToLiveFeed() {
    disconnectFromAllLiveFeeds(); // Ensure no old connection exists

    const params = {
        dataType: state.currentDataType,
        symbol: elements.symbolSelect.value,
        interval: elements.intervalSelect.value,
        timezone: elements.timezoneSelect.value,
    };

    const onOpen = () => showToast(`Live feed connected for ${params.symbol}!`, 'success');
    const onMessage = (data) => state.processLiveUpdate(data);
    const onClose = () => showToast('Live feed disconnected.', 'info');

    liveDataSocket = api.createLiveDataConnection(params, onMessage, onOpen, onClose);
}

/**
 * Disconnects from the current live data feed.
 */
export function disconnectFromAllLiveFeeds() {
    if (liveDataSocket) {
        liveDataSocket.onclose = null; // Prevent onclose handler from firing on manual close
        liveDataSocket.close();
        liveDataSocket = null;
    }
}