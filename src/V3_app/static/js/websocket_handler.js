// Common WebSocket Handler for the Financial App

document.addEventListener('DOMContentLoaded', function() {
    console.log("!!! WebSocket Handler DOMContentLoaded Fired !!!");
    console.log("WebSocket Handler Initializing...");

    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${window.location.host}/ws/ibkr_status`;
    let socket;
    let reconnectInterval = 1000; // Start with 1 second
    const maxReconnectInterval = 30000; // Cap at 30 seconds

    // --- Banner Elements ---
    const banner = document.getElementById('ibkr-warning-banner');
    const bannerTitle = document.getElementById('ibkr-warning-title');
    const bannerMessage = document.getElementById('ibkr-warning-message');

    const BANNER_DISMISSED_KEY = 'ibkrBannerDismissed';

    function isBannerDismissed() {
        return sessionStorage.getItem(BANNER_DISMISSED_KEY) === 'true';
    }

    function setBannerDismissed(dismissed) {
        if (dismissed) {
            sessionStorage.setItem(BANNER_DISMISSED_KEY, 'true');
        } else {
            sessionStorage.removeItem(BANNER_DISMISSED_KEY);
        }
    }

    function updateBanner(show, title = 'Connection Error:', message = '', isError = true) {
        if (!banner || !bannerTitle || !bannerMessage) {
            console.warn('IBKR warning banner elements not found.');
            return;
        }

        // --- Check if dismissed --- 
        if (show && isBannerDismissed()) {
            console.log("Banner update suppressed because it was dismissed in this session.");
            return; // Don't show if dismissed
        }
        // --- End check --- 

        if (show) {
            bannerTitle.textContent = title;
            bannerMessage.textContent = message;
            banner.classList.remove('d-none');
            // Optionally change class based on error status
            if (isError) {
                banner.classList.remove('alert-success'); // Ensure success class is removed
                banner.classList.add('alert-danger');
            } else {
                banner.classList.remove('alert-danger');
                banner.classList.add('alert-success');
            }
             // --- Undismiss if banner is explicitly hidden (e.g., by connected message) --- 
            setBannerDismissed(false);
             // --- End undismiss --- 
        } else {
            banner.classList.add('d-none');
        }
    }

    // --- Add listener for banner dismissal --- 
    if (banner) {
        // Listen for the Bootstrap 'close.bs.alert' event which fires *before* the element is removed
        banner.addEventListener('close.bs.alert', function () {
            console.log("IBKR Warning Banner dismissed by user.");
            setBannerDismissed(true); // Set the flag when user closes it
        });
    }
    // --- End listener --- 

    function connectWebSocket() {
        console.log(`Attempting WebSocket connection to ${wsUrl}...`);
        socket = new WebSocket(wsUrl);

        socket.onopen = function(event) {
            console.log('WebSocket connection established.');
            reconnectInterval = 1000; // Reset reconnect interval on successful connection
            // Hide banner on successful connection (assuming initial state might be error)
            // However, wait for a 'connected' message from the server for confirmation.
            // updateBanner(false); // Let the server confirm connection status
            
            // --- Request initial status --- 
            if (socket.readyState === WebSocket.OPEN) {
                console.log('WebSocket open, sending get_ibkr_status request.');
                try {
                    socket.send(JSON.stringify({ type: "get_ibkr_status" }));
                } catch (e) {
                    console.error("Error sending get_ibkr_status:", e);
                }
            } else {
                console.warn('WebSocket not open when trying to send get_ibkr_status.');
            }
            // --- End request initial status --- 
        };

        socket.onmessage = function(event) {
            console.log('WebSocket message received:', event.data);
            try {
                const message = JSON.parse(event.data);

                // --- Handle 'data_updated' event (from original files) ---
                if (message.event === 'data_updated') {
                    console.log('Data update event received, reloading page...');
                    // Use the global showAlert if available, otherwise console log
                    if (typeof showAlert === 'function') {
                        showAlert('Data updated from server. Reloading...', 'info', 2000);
                    } else {
                        console.info('Data updated from server. Reloading...');
                    }
                    // Add a slight delay before reloading
                    setTimeout(() => { location.reload(); }, 1500);
                }
                // --- Handle 'connection_status' event (NEW) ---
                else if (message.type === 'connection_status' && message.service === 'ibkr') {
                    console.log('IBKR Connection Status Update:', message);
                    if (message.status === 'error') {
                        const title = message.code ? `IBKR Error (${message.code}):` : 'IBKR Connection Error:';
                        updateBanner(true, title, message.message || 'An unknown error occurred.', true);
                    } else if (message.status === 'connected') {
                        // Optionally show a temporary success message or just hide the error banner
                        // updateBanner(true, 'IBKR Status:', 'Connected successfully.', false); // Optional success flash
                        // setTimeout(() => updateBanner(false), 3000); // Hide after 3 seconds
                        updateBanner(false); // Hide any previous error banner
                    }
                }
                // --- Add handlers for other message types if needed ---

            } catch (e) {
                console.error('Error parsing WebSocket message:', e, event.data);
            }
        };

        socket.onclose = function(event) {
            console.warn(`WebSocket connection closed (Code: ${event.code}, Reason: ${event.reason}). Attempting to reconnect in ${reconnectInterval / 1000}s...`);
            // Show connection error banner when WS closes unexpectedly
            // Avoid showing banner if the close was clean (e.g., server shutdown, code 1000 or 1001 typically)
            if (event.code !== 1000 && event.code !== 1001) {
                 updateBanner(true, 'WebSocket Error:', 'Connection lost. Attempting to reconnect...', true);
            }
            // Exponential backoff for reconnection attempts
            setTimeout(connectWebSocket, reconnectInterval);
            reconnectInterval = Math.min(reconnectInterval * 2, maxReconnectInterval);
        };

        socket.onerror = function(error) {
            console.error('WebSocket error:', error);
            // Show a generic connection error banner
            updateBanner(true, 'WebSocket Error:', 'Could not establish connection.', true);
            // onclose will handle the reconnect attempt after an error leads to closure
        };
    }

    // Initial connection attempt
    connectWebSocket();

}); // End DOMContentLoaded 