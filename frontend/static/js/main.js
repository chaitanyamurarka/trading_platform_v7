import { initializeNewChartObject } from './app/3-chart-options.js';
import { initializeAllEventListeners } from './app/11-ui-listeners.js';
import { setAutomaticDateTime, updateThemeToggleIcon } from './app/4-ui-helpers.js';
import { startApplication } from './app/6-api-service.js'; 

/**
 * Main application entry point.
 * This event listener fires when the DOM is fully loaded.
 */
document.addEventListener('DOMContentLoaded', () => {
    // Basic UI setup
    const savedTheme = localStorage.getItem('chartTheme') || 'light';
    document.documentElement.setAttribute('data-theme', savedTheme);
    updateThemeToggleIcon();
    setAutomaticDateTime();

    // Initialize the chart objects and all UI event listeners
    initializeNewChartObject();
    initializeAllEventListeners();
    
    // --- Start the application ---
    // This single function now handles getting the session and loading the initial data.
    startApplication(); 
});