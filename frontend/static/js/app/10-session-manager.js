// frontend/static/js/app/10-session-manager.js
import { initiateSession, sendHeartbeat } from '../api.js';
import { state } from './2-state.js';
import { showToast } from './4-ui-helpers.js';
import { loadChartData } from './6-api-service.js';

export async function startSession() {
    try {
        const sessionData = await initiateSession();
        state.sessionToken = sessionData.session_token;
        showToast('Session started.', 'info');
        
        // Load initial data after getting a session token
        loadChartData();

        // Start heartbeat to keep the session alive
        if (state.heartbeatIntervalId) clearInterval(state.heartbeatIntervalId);
        state.heartbeatIntervalId = setInterval(() => {
            if (state.sessionToken) {
                sendHeartbeat(state.sessionToken).catch(e => console.error('Heartbeat failed', e));
            }
        }, 60000); // every minute

    } catch (error) {
        console.error('Failed to initiate session:', error);
        showToast('Could not start a session. Please reload.', 'error');
    }
}