// frontend/static/js/app/8-responsive-handler.js

import { state } from './2-state.js';
import * as elements from './1-dom-elements.js';

/**
 * Advanced responsive handler for TradingView-style dynamic adaptation
 */
class ResponsiveHandler {
    constructor() {
        this.isInitialized = false;
        this.resizeTimeout = null;
        this.observerTimeout = null;
        this.lastKnownSize = { width: 0, height: 0 };
        this.mediaQueries = new Map();
        this.callbacks = new Set();
        
        this.init();
    }
    
    init() {
        if (this.isInitialized) return;
        
        this.setupMediaQueries();
        this.setupResizeObserver();
        this.setupEventListeners();
        this.initialResize();
        
        this.isInitialized = true;
        console.log('Responsive handler initialized');
    }
    
    setupMediaQueries() {
        const queries = [
            { name: 'mobile', query: '(max-width: 639px)' },
            { name: 'tablet-portrait', query: '(max-width: 767px) and (min-width: 640px)' },
            { name: 'tablet-landscape', query: '(max-width: 1023px) and (min-width: 768px)' },
            { name: 'desktop', query: '(max-width: 1439px) and (min-width: 1024px)' },
            { name: 'large-desktop', query: '(min-width: 1440px)' }
        ];
        
        queries.forEach(({ name, query }) => {
            const mq = window.matchMedia(query);
            this.mediaQueries.set(name, mq);
            
            mq.addEventListener('change', (e) => {
                if (e.matches) {
                    this.handleBreakpointChange(name);
                }
            });
        });
    }
    
    setupResizeObserver() {
        if (!window.ResizeObserver) {
            console.warn('ResizeObserver not supported, falling back to resize events');
            return;
        }
        
        this.resizeObserver = new ResizeObserver((entries) => {
            clearTimeout(this.observerTimeout);
            this.observerTimeout = setTimeout(() => {
                for (const entry of entries) {
                    if (entry.target === elements.chartContainer) {
                        this.handleChartResize(entry.contentRect);
                    }
                }
            }, 16); // ~60fps
        });
        
        if (elements.chartContainer) {
            this.resizeObserver.observe(elements.chartContainer);
        }
    }
    
    setupEventListeners() {
        // Enhanced window resize handler
        window.addEventListener('resize', this.debounce(() => {
            this.handleWindowResize();
        }, 100));
        
        // Orientation change handler
        window.addEventListener('orientationchange', () => {
            setTimeout(() => {
                this.handleOrientationChange();
            }, 250);
        });
        
        // Fullscreen change handler
        document.addEventListener('fullscreenchange', () => {
            setTimeout(() => {
                this.handleFullscreenChange();
            }, 100);
        });
        
        // Focus/blur handlers for performance optimization
        window.addEventListener('focus', () => {
            this.handleWindowFocus();
        });
        
        window.addEventListener('blur', () => {
            this.handleWindowBlur();
        });
    }
    
    handleChartResize(rect) {
        if (!state.mainChart || !rect) return;
        
        const { width, height } = rect;
        
        // Only resize if dimensions actually changed
        if (width === this.lastKnownSize.width && height === this.lastKnownSize.height) {
            return;
        }
        
        this.lastKnownSize = { width, height };
        
        // Ensure minimum dimensions
        const minWidth = this.getMinChartWidth();
        const minHeight = this.getMinChartHeight();
        
        const finalWidth = Math.max(width, minWidth);
        const finalHeight = Math.max(height, minHeight);
        
        try {
            state.mainChart.resize(finalWidth, finalHeight, true);
            this.notifyCallbacks('chartResize', { width: finalWidth, height: finalHeight });
        } catch (error) {
            console.error('Error resizing chart:', error);
        }
    }
    
    handleWindowResize() {
        if (!elements.chartContainer) return;
        
        const containerRect = elements.chartContainer.getBoundingClientRect();
        this.handleChartResize(containerRect);
        
        this.updateResponsiveClasses();
        this.notifyCallbacks('windowResize', {
            width: window.innerWidth,
            height: window.innerHeight
        });
    }
    
    handleBreakpointChange(breakpoint) {
        document.body.setAttribute('data-breakpoint', breakpoint);
        this.updateResponsiveClasses();
        
        // Trigger resize after breakpoint change
        setTimeout(() => {
            this.handleWindowResize();
        }, 50);
        
        this.notifyCallbacks('breakpointChange', { breakpoint });
    }
    
    handleOrientationChange() {
        this.updateResponsiveClasses();
        this.handleWindowResize();
        this.notifyCallbacks('orientationChange', {
            orientation: screen.orientation?.angle || window.orientation
        });
    }
    
    handleFullscreenChange() {
        const isFullscreen = !!document.fullscreenElement;
        document.body.classList.toggle('fullscreen-mode', isFullscreen);
        
        setTimeout(() => {
            this.handleWindowResize();
        }, 100);
        
        this.notifyCallbacks('fullscreenChange', { isFullscreen });
    }
    
    handleWindowFocus() {
        // Resume high-frequency updates when window is focused
        this.notifyCallbacks('windowFocus');
    }
    
    handleWindowBlur() {
        // Reduce update frequency when window is blurred for performance
        this.notifyCallbacks('windowBlur');
    }
    
    updateResponsiveClasses() {
        const width = window.innerWidth;
        const height = window.innerHeight;
        
        // Update responsive classes on body
        document.body.classList.toggle('mobile-view', width < 640);
        document.body.classList.toggle('tablet-view', width >= 640 && width < 1024);
        document.body.classList.toggle('desktop-view', width >= 1024);
        document.body.classList.toggle('landscape-view', width > height);
        document.body.classList.toggle('portrait-view', width <= height);

        // --- NEW: Add classes based on vertical height for more granular control ---
        document.body.classList.toggle('height-short', height < 600);
        document.body.classList.toggle('height-medium', height >= 600 && height < 800);
        document.body.classList.toggle('height-tall', height >= 800);
        
        // Update CSS custom properties
        document.documentElement.style.setProperty('--viewport-width', `${width}px`);
        document.documentElement.style.setProperty('--viewport-height', `${height}px`);
        document.documentElement.style.setProperty('--aspect-ratio', `${width / height}`);
    }
    
    getMinChartWidth() {
        const width = window.innerWidth;
        if (width < 640) return 250;
        if (width < 768) return 300;
        if (width < 1024) return 400;
        return 500;
    }
    
    getMinChartHeight() {
        const height = window.innerHeight;
        if (height < 500) return 150;
        if (height < 600) return 200;
        if (height < 800) return 250;
        return 300;
    }
    
    initialResize() {
        this.updateResponsiveClasses();
        
        // Set initial breakpoint
        for (const [name, mq] of this.mediaQueries) {
            if (mq.matches) {
                this.handleBreakpointChange(name);
                break;
            }
        }
        
        setTimeout(() => {
            this.handleWindowResize();
        }, 100);
    }
    
    // Public API
    onResize(callback) {
        this.callbacks.add(callback);
        return () => this.callbacks.delete(callback);
    }
    
    forceResize() {
        this.handleWindowResize();
    }
    
    getCurrentBreakpoint() {
        for (const [name, mq] of this.mediaQueries) {
            if (mq.matches) {
                return name;
            }
        }
        return 'unknown';
    }
    
    getViewportInfo() {
        return {
            width: window.innerWidth,
            height: window.innerHeight,
            breakpoint: this.getCurrentBreakpoint(),
            aspectRatio: window.innerWidth / window.innerHeight,
            orientation: window.innerWidth > window.innerHeight ? 'landscape' : 'portrait',
            pixelRatio: window.devicePixelRatio || 1
        };
    }
    
    // Utility methods
    debounce(func, wait) {
        return (...args) => {
            clearTimeout(this.resizeTimeout);
            this.resizeTimeout = setTimeout(() => func.apply(this, args), wait);
        };
    }
    
    notifyCallbacks(event, data = {}) {
        this.callbacks.forEach(callback => {
            try {
                callback(event, data);
            } catch (error) {
                console.error('Error in responsive callback:', error);
            }
        });
    }
    
    destroy() {
        if (this.resizeObserver) {
            this.resizeObserver.disconnect();
        }
        
        clearTimeout(this.resizeTimeout);
        clearTimeout(this.observerTimeout);
        
        this.callbacks.clear();
        this.mediaQueries.clear();
        
        this.isInitialized = false;
    }
}

// Create singleton instance
export const responsiveHandler = new ResponsiveHandler();

// Export helper functions
export function getViewportInfo() {
    return responsiveHandler.getViewportInfo();
}

export function onResize(callback) {
    return responsiveHandler.onResize(callback);
}

export function forceResize() {
    responsiveHandler.forceResize();
}