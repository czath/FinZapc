document.addEventListener('DOMContentLoaded', function() {
    const LOG_PREFIX = "TimeseriesModule:";
    console.log(LOG_PREFIX, "DOMContentLoaded event fired.");

    // --- Constants ---
    const TIMESERIES_TAB_PANE_ID = 'timeseries-tab-pane';

    // --- State Variables ---
    let timeseriesChartInstance = null;
    // Add other state variables as needed:
    // e.g., selected tickers, date ranges, data series
    let financialChartLibraryPromise = null; // NEW: For dynamic library loading
    let dateAdapterLibraryPromise = null; // NEW: For dynamic date adapter loading

    // --- DOM Elements ---
    // Example: const timeSeriesChartCanvas = document.getElementById('timeseries-chart-canvas'); 
    //          (assuming you add a canvas with this ID in the HTML)
    // Add references to controls (dropdowns for tickers, date pickers, etc.)

    // --- NEW: DOM Elements for Price History Study ---
    const priceHistoryRunButton = document.getElementById('ts-ph-run-study-btn');
    const priceHistoryTickerInput = document.getElementById('ts-ph-ticker-input'); // Actual input field
    const priceHistoryStartDateInput = document.getElementById('ts-ph-start-date');
    const priceHistoryEndDateInput = document.getElementById('ts-ph-end-date');
    const priceHistoryIntervalSelect = document.getElementById('ts-ph-interval');
    const priceHistoryPeriodSelector = document.getElementById('ts-ph-period-selector');
    const priceHistoryTickerSourceRadios = document.querySelectorAll('input[name="tsPhTickerSource"]');
    const priceHistoryLoadedTickerSelect = document.getElementById('ts-ph-ticker-select-loaded');
    const priceHistoryLoadedTickerContainer = document.getElementById('ts-ph-ticker-select-loaded-container');
    const priceHistoryManualTickerContainer = document.getElementById('ts-ph-ticker-input-manual-container'); // Container for manual input
    const priceHistoryStartDateContainer = document.getElementById('ts-ph-start-date-container');
    const priceHistoryEndDateContainer = document.getElementById('ts-ph-end-date-container');
    const tsPhChartTypeSelect = document.getElementById('ts-ph-chart-type'); // NEW: Chart Type Selector
    const timeseriesTabPane = document.getElementById(TIMESERIES_TAB_PANE_ID); // Used to mark as initialized
    const tsResetZoomBtn = document.getElementById('ts-reset-zoom-btn'); // NEW: Reset Zoom Button

    // --- NEW: DOM Elements for Price Performance Comparison Study (PPC) ---
    const ppcRunButton = document.getElementById('ts-ppc-run-study-btn');
    const ppcTickerSourceRadios = document.querySelectorAll('input[name="tsPpcTickerSource"]');
    const ppcLoadedTickerSelect = document.getElementById('ts-ppc-ticker-select-loaded');
    const ppcLoadedTickerContainer = document.getElementById('ts-ppc-ticker-select-loaded-container');
    const ppcManualTickerTextarea = document.getElementById('ts-ppc-ticker-input-manual');
    const ppcManualTickerContainer = document.getElementById('ts-ppc-ticker-input-manual-container');
    // --- NEW: Unique DOM Elements for PPC Study Date/Period/Interval Controls ---
    const ppcPeriodSelector = document.getElementById('ts-ppc-period-selector');
    const ppcStartDateInput = document.getElementById('ts-ppc-start-date');
    const ppcEndDateInput = document.getElementById('ts-ppc-end-date');
    const ppcStartDateContainer = document.getElementById('ts-ppc-start-date-container');
    const ppcEndDateContainer = document.getElementById('ts-ppc-end-date-container');
    const ppcIntervalSelect = document.getElementById('ts-ppc-interval');

    // --- Initialization ---
    function initializeTimeseriesModule() {
        console.log(LOG_PREFIX, "Initializing base UI and event listeners (pre-data)...");
        if (!timeseriesTabPane) {
            console.error(LOG_PREFIX, "Timeseries tab pane not found!");
            return;
        }
        setupEventListeners();
        // Initial call to handleStudySelectionChange to set initial pane visibility based on HTML
        if (document.getElementById('ts-study-selector')) {
            console.log(LOG_PREFIX, "Initial call to handleStudySelectionChange from initializeTimeseriesModule.");
            handleStudySelectionChange({ target: document.getElementById('ts-study-selector') });
        }
        console.log(LOG_PREFIX, "Base UI and event listeners initialized.");
    }

    // --- Event Listener Setup (for elements within the Timeseries tab) ---
    function setupEventListeners() {
        console.log(LOG_PREFIX, "Setting up event listeners...");

        if (priceHistoryRunButton) {
            priceHistoryRunButton.addEventListener('click', handleRunPriceHistory);
        } else {
            console.warn(LOG_PREFIX, "Run button (ts-ph-run-study-btn) not found.");
        }

        // --- Event Listeners for Price Performance Comparison (PPC) ---
        if (ppcRunButton) {
            ppcRunButton.addEventListener('click', handleRunPricePerformanceComparison);
        } else {
            console.warn(LOG_PREFIX, "Run Comparison button (ts-ppc-run-study-btn) not found.");
        }

        if (priceHistoryPeriodSelector) {
            priceHistoryPeriodSelector.addEventListener('change', function() {
                const isCustom = this.value === 'custom';
                if (priceHistoryStartDateContainer) priceHistoryStartDateContainer.style.display = isCustom ? 'block' : 'none';
                if (priceHistoryEndDateContainer) priceHistoryEndDateContainer.style.display = isCustom ? 'block' : 'none';
            });
            // Trigger change on load to set initial state for PH date inputs
            if (priceHistoryStartDateContainer && priceHistoryEndDateContainer) {
                 const initialIsCustomPeriod = priceHistoryPeriodSelector.value === 'custom';
                 priceHistoryStartDateContainer.style.display = initialIsCustomPeriod ? 'block' : 'none';
                 priceHistoryEndDateContainer.style.display = initialIsCustomPeriod ? 'block' : 'none';
            }
        } else {
            console.warn(LOG_PREFIX, "Period selector (ts-ph-period-selector) for Price History not found.");
        }

        if (priceHistoryTickerSourceRadios && priceHistoryLoadedTickerContainer && priceHistoryManualTickerContainer && priceHistoryTickerInput) {
            console.log(LOG_PREFIX, "Setting up Ticker Source Radio listeners.");
            priceHistoryTickerSourceRadios.forEach(radio => {
                radio.addEventListener('change', function() {
                    console.log(LOG_PREFIX, "Ticker source radio changed to:", this.value);
                    const isLoaded = this.value === 'loaded';
                    priceHistoryLoadedTickerContainer.style.display = isLoaded ? 'block' : 'none';
                    priceHistoryManualTickerContainer.style.display = !isLoaded ? 'block' : 'none';
                });
            });
            // Set initial state based on checked radio
            const initialTickerSourceChecked = document.querySelector('input[name="tsPhTickerSource"]:checked');
            if (initialTickerSourceChecked) {
                const initialIsLoaded = initialTickerSourceChecked.value === 'loaded';
                priceHistoryLoadedTickerContainer.style.display = initialIsLoaded ? 'block' : 'none';
                priceHistoryManualTickerContainer.style.display = !initialIsLoaded ? 'block' : 'none';
            } else if (priceHistoryTickerSourceRadios.length > 0) { // Default if nothing checked
                priceHistoryTickerSourceRadios[0].checked = true; // Check the first one (usually 'loaded')
                priceHistoryLoadedTickerContainer.style.display = 'block';
                priceHistoryManualTickerContainer.style.display = 'none';
            }
        } else {
            console.error(LOG_PREFIX, "Ticker Source Radio elements or containers not found.");
        }

        const studySelector = document.getElementById('ts-study-selector');
        if (studySelector) {
            studySelector.addEventListener('change', handleStudySelectionChange);
        } else {
            console.warn(LOG_PREFIX, "Study selector (ts-study-selector) not found.");
        }

        if (tsResetZoomBtn) { // NEW: Add event listener for reset zoom button
            tsResetZoomBtn.addEventListener('click', () => {
                if (timeseriesChartInstance && typeof timeseriesChartInstance.resetZoom === 'function') {
                    timeseriesChartInstance.resetZoom();
                    console.log(LOG_PREFIX, "Chart zoom reset.");
                } else {
                    console.warn(LOG_PREFIX, "Reset zoom clicked, but no chart instance found or resetZoom is not a function.");
                }
            });
        } else {
            console.warn(LOG_PREFIX, "Reset zoom button (ts-reset-zoom-btn) not found.");
        }

        // --- Event Listeners for Price Performance Comparison (PPC) ---
        if (ppcTickerSourceRadios && ppcLoadedTickerContainer && ppcManualTickerContainer) {
            console.log(LOG_PREFIX, "Setting up PPC Ticker Source Radio listeners.");
            ppcTickerSourceRadios.forEach(radio => {
                radio.addEventListener('change', function() {
                    console.log(LOG_PREFIX, "PPC Ticker source radio changed to:", this.value);
                    const isLoaded = this.value === 'loaded';
                    ppcLoadedTickerContainer.style.display = isLoaded ? 'block' : 'none';
                    if (ppcManualTickerContainer) ppcManualTickerContainer.style.display = !isLoaded ? 'block' : 'none';
                });
            });
            // Set initial state for PPC ticker source based on checked radio
            const initialPpcTickerSourceChecked = document.querySelector('input[name="tsPpcTickerSource"]:checked');
            if (initialPpcTickerSourceChecked) {
                const initialIsLoaded = initialPpcTickerSourceChecked.value === 'loaded';
                ppcLoadedTickerContainer.style.display = initialIsLoaded ? 'block' : 'none';
                if (ppcManualTickerContainer) ppcManualTickerContainer.style.display = !initialIsLoaded ? 'block' : 'none';
            } else if (ppcTickerSourceRadios.length > 0) { // Default if nothing checked
                ppcTickerSourceRadios[0].checked = true; // Check the first one (usually 'loaded')
                ppcLoadedTickerContainer.style.display = 'block';
                if (ppcManualTickerContainer) ppcManualTickerContainer.style.display = 'none';
            }
        } else {
            console.warn(LOG_PREFIX, "PPC Ticker Source Radio elements or containers not found for event setup.");
        }
        // --- End PPC Event Listeners ---

        // --- Event Listener for Price History (PH) Period Selector ---
        if (priceHistoryPeriodSelector) {
            priceHistoryPeriodSelector.addEventListener('change', function() {
                const isCustom = this.value === 'custom';
                if (priceHistoryStartDateContainer) priceHistoryStartDateContainer.style.display = isCustom ? 'block' : 'none';
                if (priceHistoryEndDateContainer) priceHistoryEndDateContainer.style.display = isCustom ? 'block' : 'none';
            });
            // Trigger change on load to set initial state for PH date inputs
            if (priceHistoryStartDateContainer && priceHistoryEndDateContainer) {
                 const initialIsCustomPeriod = priceHistoryPeriodSelector.value === 'custom';
                 priceHistoryStartDateContainer.style.display = initialIsCustomPeriod ? 'block' : 'none';
                 priceHistoryEndDateContainer.style.display = initialIsCustomPeriod ? 'block' : 'none';
            }
        } else {
            console.warn(LOG_PREFIX, "Period selector (ts-ph-period-selector) for Price History not found.");
        }

        // --- NEW: Event Listener for Price Performance Comparison (PPC) Period Selector ---
        if (ppcPeriodSelector) {
            ppcPeriodSelector.addEventListener('change', function() {
                const isCustom = this.value === 'custom';
                if (ppcStartDateContainer) ppcStartDateContainer.style.display = isCustom ? 'block' : 'none';
                if (ppcEndDateContainer) ppcEndDateContainer.style.display = isCustom ? 'block' : 'none';
            });
            // Trigger change on load/init for PPC date inputs (or handle via handleStudySelectionChange)
            // For now, ensure correct initial state when pane becomes visible in handleStudySelectionChange
             if (ppcStartDateContainer && ppcEndDateContainer) { // Initial setup for PPC date fields
                 const initialIsCustomPpcPeriod = ppcPeriodSelector.value === 'custom';
                 ppcStartDateContainer.style.display = initialIsCustomPpcPeriod ? 'block' : 'none';
                 ppcEndDateContainer.style.display = initialIsCustomPpcPeriod ? 'block' : 'none';
            }
        } else {
            console.warn(LOG_PREFIX, "Period selector (ts-ppc-period-selector) for PPC not found.");
        }

        if (priceHistoryTickerSourceRadios && priceHistoryLoadedTickerContainer && priceHistoryManualTickerContainer && priceHistoryTickerInput) {
            console.log(LOG_PREFIX, "Setting up Ticker Source Radio listeners.");
            priceHistoryTickerSourceRadios.forEach(radio => {
                radio.addEventListener('change', function() {
                    console.log(LOG_PREFIX, "Ticker source radio changed to:", this.value);
                    const isLoaded = this.value === 'loaded';
                    priceHistoryLoadedTickerContainer.style.display = isLoaded ? 'block' : 'none';
                    priceHistoryManualTickerContainer.style.display = !isLoaded ? 'block' : 'none';
                });
            });
            // Set initial state based on checked radio
            const initialTickerSourceChecked = document.querySelector('input[name="tsPhTickerSource"]:checked');
            if (initialTickerSourceChecked) {
                const initialIsLoaded = initialTickerSourceChecked.value === 'loaded';
                priceHistoryLoadedTickerContainer.style.display = initialIsLoaded ? 'block' : 'none';
                priceHistoryManualTickerContainer.style.display = !initialIsLoaded ? 'block' : 'none';
            } else if (priceHistoryTickerSourceRadios.length > 0) { // Default if nothing checked
                priceHistoryTickerSourceRadios[0].checked = true; // Check the first one (usually 'loaded')
                priceHistoryLoadedTickerContainer.style.display = 'block';
                priceHistoryManualTickerContainer.style.display = 'none';
            }
        } else {
            console.error(LOG_PREFIX, "Ticker Source Radio elements or containers not found.");
        }

        console.log(LOG_PREFIX, "Event listeners setup complete.");
    }

    // NEW: Function to dynamically load a script and return a promise
    function loadScript(url, libraryName, globalCheck) {
        return new Promise((resolve, reject) => {
            if (globalCheck && globalCheck()) { // Check if already available
                console.log(LOG_PREFIX, `${libraryName} already available.`);
                resolve();
                return;
            }
            console.log(LOG_PREFIX, `Loading ${libraryName} from ${url}...`);
            const script = document.createElement('script');
            script.src = url;
            script.async = true;
            script.onload = () => {
                console.log(LOG_PREFIX, `${libraryName} loaded successfully.`);
                if (globalCheck && !globalCheck()) {
                    console.warn(LOG_PREFIX, `${libraryName} did not make expected global components available after loading. Subsequent operations might fail.`);
                }
                resolve();
            };
            script.onerror = (error) => {
                console.error(LOG_PREFIX, `Failed to load ${libraryName} from ${url}.`, error);
                // NEW: Reset the main promise variable for the specific library to allow retry
                if (libraryName === "Date Adapter Library") {
                    dateAdapterLibraryPromise = null;
                }
                if (libraryName === "Financial Chart Library") {
                    financialChartLibraryPromise = null;
                }
                reject(new Error(`Failed to load ${libraryName}.`));
            };
            document.head.appendChild(script);
        });
    }

    // NEW: Function to dynamically load the date adapter library
    function loadDateAdapterLibrary() {
        if (!dateAdapterLibraryPromise) {
            // Using date-fns adapter. The bundle includes date-fns itself.
            const adapterURL = 'https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js';
            // For date-fns adapter, there isn't a simple single global. 
            // Chart.js will pick it up if loaded. We rely on Chart.js time scale working after this.
            // MODIFIED: Pass null for globalCheck to always attempt script append if dateAdapterLibraryPromise is null
            dateAdapterLibraryPromise = loadScript(adapterURL, "Date Adapter Library", null);
        }
        return dateAdapterLibraryPromise;
    }

    // NEW: Function to dynamically load the financial chart library
    function loadFinancialChartLibrary() {
        if (!financialChartLibraryPromise) {
            const financialLibURL = 'https://cdn.jsdelivr.net/npm/chartjs-chart-financial@0.2.0/dist/chartjs-chart-financial.min.js';
            financialChartLibraryPromise = loadScript(financialLibURL, "Financial Chart Library", () => window.Chart && window.Chart.controllers && window.Chart.controllers.candlestick);
        }
        return financialChartLibraryPromise;
    }

    // NEW: Helper to show placeholder message and clean up chart
    function showPlaceholderWithMessage(message) {
        console.info(LOG_PREFIX, "Showing placeholder: ", message);
        const chartCanvas = document.getElementById('ts-chart-canvas');
        const chartPlaceholder = document.getElementById('ts-chart-placeholder');

        if (timeseriesChartInstance) {
            timeseriesChartInstance.destroy();
            timeseriesChartInstance = null;
        }

        if (chartCanvas) chartCanvas.style.display = 'none';
        if (chartPlaceholder) {
            chartPlaceholder.style.display = 'block';
            chartPlaceholder.textContent = message;
        }
    }

    // --- Event Handlers ---
    // --- NEW: Handler for Price History Study ---
    async function handleRunPriceHistory() {
        console.log(LOG_PREFIX, "handleRunPriceHistory called.");
        if (!priceHistoryIntervalSelect || !priceHistoryPeriodSelector || !priceHistoryTickerSourceRadios || !tsPhChartTypeSelect) {
            console.error(LOG_PREFIX, "Essential UI elements for Price History not found!");
            alert("Error: Essential UI components for Price History are missing.");
            return;
        }

        let userStartDateStr = null; // MODIFIED: Declare here for wider scope
        let userEndDateStr = null; // MODIFIED: Declare here for wider scope
        let rangeDetails; // NEW: To hold range info for renderTimeseriesChart

        const tickerSourceChecked = document.querySelector('input[name="tsPhTickerSource"]:checked');
        if (!tickerSourceChecked) {
            console.error(LOG_PREFIX, "No ticker source selected.");
            alert("Please select a ticker source (Loaded or Manual).");
            return;
        }
        const tickerSource = tickerSourceChecked.value;
        let ticker = '';

        if (tickerSource === 'loaded') {
            if (!priceHistoryLoadedTickerSelect || priceHistoryLoadedTickerSelect.value === "") {
                alert("Please select a ticker from the loaded list."); 
                return;
            }
            ticker = priceHistoryLoadedTickerSelect.value;
        } else { // manual
            if (!priceHistoryTickerInput) {
                alert("Ticker manual input field not found."); return;
            }
            ticker = priceHistoryTickerInput.value.trim().toUpperCase();
            if (!ticker) {
                alert("Please enter a ticker symbol manually.");
                return;
            }
        }
        
        const interval = priceHistoryIntervalSelect.value;
        const selectedPeriod = priceHistoryPeriodSelector.value;
        const chartType = tsPhChartTypeSelect.value;

        if (!interval) {
            alert("Please select an interval.");
            return;
        }
        
        let queryParams = `ticker=${encodeURIComponent(ticker)}&interval=${encodeURIComponent(interval)}`; // MODIFIED: Initialize queryParams here

        if (selectedPeriod === 'custom') {
            if (!priceHistoryStartDateInput || !priceHistoryEndDateInput) {
                console.error(LOG_PREFIX, "Price History Start/End Date inputs not found!");
                alert("Error: Date input components are missing for custom range.");
                return;
            }
            userStartDateStr = priceHistoryStartDateInput.value;
            userEndDateStr = priceHistoryEndDateInput.value;

            if (!userStartDateStr || !userEndDateStr) {
                alert("Please select a start and end date for custom range.");
                return;
            }

            const startDateObj = new Date(userStartDateStr);
            const endDateObj = new Date(userEndDateStr);

            // MODIFIED: Allow start date to be the same as end date for single-day queries.
            if (startDateObj > endDateObj) {
                alert("Start date must be before or the same as end date.");
                return;
            }

            // Adjust end date for API to make it inclusive
            const apiEndDateObj = new Date(endDateObj);
            apiEndDateObj.setDate(apiEndDateObj.getDate() + 1);
            const apiEndDateStr = apiEndDateObj.toISOString().split('T')[0];

            queryParams += `&start_date=${encodeURIComponent(userStartDateStr)}&end_date=${encodeURIComponent(apiEndDateStr)}`;
            rangeDetails = { start: userStartDateStr, end: userEndDateStr }; // NEW: Populate for custom range
        } else {
            queryParams += `&period=${encodeURIComponent(selectedPeriod)}`;
            rangeDetails = { period: selectedPeriod }; // NEW: Populate for predefined period
        }

        const apiUrl = `/api/v3/timeseries/price_history?${queryParams}`;
        console.log(LOG_PREFIX, `Fetching Price History from: ${apiUrl}`);
        showLoadingIndicator(true);

        try {
            const response = await fetch(apiUrl);
            // No need to await showLoadingIndicator(false) here, it's synchronous UI update

            if (!response.ok) {
                showLoadingIndicator(false); // Hide on error before alert
                const errorData = await response.json().catch(() => ({ detail: response.statusText }));
                console.error(LOG_PREFIX, "Error fetching price history:", response.status, errorData);
                alert(`Error fetching price history: ${errorData.detail || response.statusText}`);
                return;
            }

            const data = await response.json();
            showLoadingIndicator(false); // Hide on success after getting data
            console.log(LOG_PREFIX, "Price History Data Received:", data);

            if (data && data.length > 0) {
                renderTimeseriesChart(data, ticker, interval, rangeDetails, chartType); // MODIFIED: Use rangeDetails
            } else {
                renderTimeseriesChart([], ticker, interval, rangeDetails, chartType); // MODIFIED: Use rangeDetails
            }

        } catch (error) {
            showLoadingIndicator(false);
            console.error(LOG_PREFIX, "Network or other error fetching price history:", error);
            alert(`Failed to fetch price history: ${error.message}`);
        }
    }

    // --- NEW: Handler for Price Performance Comparison Study ---
    async function handleRunPricePerformanceComparison() {
        console.log(LOG_PREFIX, "handleRunPricePerformanceComparison called.");
        // TODO: Implement full logic: get tickers, period, interval, fetch data for each, normalize, then render.

        let selectedTickers = [];
        const tickerSourceChecked = document.querySelector('input[name="tsPpcTickerSource"]:checked');
        if (!tickerSourceChecked) {
            alert("Please select a ticker source for comparison."); return;
        }
        const tickerSource = tickerSourceChecked.value;

        if (tickerSource === 'loaded') {
            if (!ppcLoadedTickerSelect) { alert("PPC Loaded ticker select not found."); return; }
            selectedTickers = Array.from(ppcLoadedTickerSelect.selectedOptions).map(option => option.value);
            if (selectedTickers.length === 0) { alert("Please select at least one ticker from the loaded list."); return; }
        } else { // manual
            if (!ppcManualTickerTextarea) { alert("PPC Manual ticker textarea not found."); return; }
            const manualTickersStr = ppcManualTickerTextarea.value.trim();
            if (!manualTickersStr) { alert("Please enter ticker symbols manually."); return; }
            selectedTickers = manualTickersStr.split(',').map(t => t.trim().toUpperCase()).filter(t => t);
            if (selectedTickers.length === 0) { alert("Please enter valid ticker symbols manually."); return; }
        }

        if (selectedTickers.length === 0) {
            alert("No tickers selected for comparison."); return;
        }

        // Reuse existing selectors for period, date, interval
        const interval = ppcIntervalSelect ? ppcIntervalSelect.value : null;
        const selectedPeriod = ppcPeriodSelector ? ppcPeriodSelector.value : null;
        
        if (!interval || !selectedPeriod) {
            alert("Interval or Period selector not found or value missing for PPC study."); return;
        }

        let userStartDateStr = null;
        let userEndDateStr = null;
        let queryDateParams = {}; // Use this to build API params for dates/period

        if (selectedPeriod === 'custom') {
            if (!ppcStartDateInput || !ppcEndDateInput) {
                alert("Date input components are missing for PPC custom range."); return;
            }
            userStartDateStr = ppcStartDateInput.value;
            userEndDateStr = ppcEndDateInput.value;
            if (!userStartDateStr || !userEndDateStr) {
                alert("Please select a start and end date for PPC custom range."); return;
            }
            const startDateObj = new Date(userStartDateStr);
            const endDateObj = new Date(userEndDateStr);
            if (startDateObj > endDateObj) {
                alert("Start date must be before or the same as end date."); return;
            }
            const apiEndDateObj = new Date(endDateObj);
            apiEndDateObj.setDate(apiEndDateObj.getDate() + 1);
            
            // For API call
            queryDateParams.start_date = userStartDateStr;
            queryDateParams.end_date = apiEndDateObj.toISOString().split('T')[0];
        } else {
            // For API call
            queryDateParams.period = selectedPeriod;
            // userStartDateStr and userEndDateStr remain null for title purposes if period is not custom
        }

        console.log(LOG_PREFIX, "PPC Run with Tickers:", selectedTickers, 
                        "Interval:", interval, 
                        "API Date Params:", queryDateParams, 
                        "User Dates (for title if custom):", {start: userStartDateStr, end: userEndDateStr });
        
        // alert(`Comparison study run triggered for tickers: ${selectedTickers.join(', ')}. Data fetching & normalization next.`);
        showLoadingIndicator(true);
        showPlaceholderWithMessage('Fetching and processing data for comparison...');

        const fetchPromises = selectedTickers.map(ticker => {
            let apiParams = `ticker=${encodeURIComponent(ticker)}&interval=${encodeURIComponent(interval)}`;
            if (queryDateParams.period) {
                apiParams += `&period=${encodeURIComponent(queryDateParams.period)}`;
            } else if (queryDateParams.start_date && queryDateParams.end_date) {
                apiParams += `&start_date=${encodeURIComponent(queryDateParams.start_date)}&end_date=${encodeURIComponent(queryDateParams.end_date)}`;
            }
            const apiUrl = `/api/v3/timeseries/price_history?${apiParams}`;
            console.log(LOG_PREFIX, `Fetching for PPC: ${apiUrl}`);
            return fetch(apiUrl)
                .then(response => {
                    if (!response.ok) {
                        return response.json().catch(() => ({})).then(errData => {
                            throw new Error(`HTTP error ${response.status} for ${ticker}: ${errData.detail || response.statusText}`);
                        });
                    }
                    return response.json();
                })
                .then(data => ({ ticker, data })) // Tag data with its ticker
                .catch(error => ({ ticker, error: error.message || "Failed to fetch" })); // Tag error with ticker
        });

        Promise.allSettled(fetchPromises)
            .then(results => {
                showLoadingIndicator(false);
                const successfullyFetchedData = [];
                const errors = [];

                results.forEach(result => {
                    if (result.status === 'fulfilled') {
                        if (result.value.error) { // Our custom error tagging
                            errors.push(`${result.value.ticker}: ${result.value.error}`);
                        } else if (result.value.data && result.value.data.length > 0) {
                            successfullyFetchedData.push(result.value);
                        } else {
                            errors.push(`${result.value.ticker}: No data returned or empty dataset.`);
                        }
                    } else { // status === 'rejected' (network error, etc.)
                        // For Promise.allSettled, error usually in result.reason
                        // but our structure above puts it in result.value.error if HTTP error was caught by .catch()
                        // This part might need adjustment based on exact error structure if fetch itself fails fundamentally.
                        errors.push(`A ticker: Request failed - ${result.reason?.message || 'Unknown fetch error'}`); 
                    }
                });

                if (errors.length > 0) {
                    alert("Errors occurred during data fetching for comparison:\n" + errors.join("\n"));
                }

                if (successfullyFetchedData.length === 0) {
                    showPlaceholderWithMessage("No data successfully fetched for any selected tickers for comparison.");
                    return;
                }

                // Normalize data
                const normalizedSeries = successfullyFetchedData.map(tickerDataObj => {
                    const { ticker, data } = tickerDataObj;
                    // Find the first valid data point to get the base price
                    let basePrice = null;
                    let firstValidDataPointIndex = -1;

                    for (let i = 0; i < data.length; i++) {
                        if (data[i] && typeof data[i].Close === 'number') {
                            basePrice = data[i].Close;
                            firstValidDataPointIndex = i;
                            break;
                        }
                    }

                    if (basePrice === null || firstValidDataPointIndex === -1) {
                        console.warn(LOG_PREFIX, `Could not find a valid base price for ticker: ${ticker}`);
                        errors.push(`${ticker}: Could not determine base price for normalization.`);
                        return null; // Skip this ticker if no base price
                    }

                    const normalizedPoints = data.slice(firstValidDataPointIndex).map(point => {
                        const currentClose = point.Close;
                        let performance = 0;
                        if (typeof currentClose === 'number' && basePrice !== 0) {
                            performance = ((currentClose - basePrice) / basePrice) * 100;
                        }
                        return {
                            x: new Date(point.Datetime || point.Date).valueOf(),
                            y: performance
                        };
                    }).filter(p => !isNaN(p.x) && !isNaN(p.y)); // Ensure valid points

                    return {
                        ticker: ticker,
                        data: normalizedPoints
                    };
                }).filter(series => series && series.data.length > 0); // Filter out nulls or empty series

                if (normalizedSeries.length === 0) {
                    showPlaceholderWithMessage("No data could be normalized for comparison. Check console for details.");
                     if (errors.length > 0 && !alertAlreadyShown) { // Avoid double alert if previous one was sufficient
                        // alert("Additionally, errors occurred during data fetching/normalization:\n" + errors.join("\n"));
                    } // It might be better to consolidate error display
                    return;
                }

                // Pass data to a chart rendering function
                // The chart type for comparison is always 'line'
                // The 'range' for the title will be based on userStartDateStr, userEndDateStr or selectedPeriod
                const titleRange = selectedPeriod === 'custom' && userStartDateStr && userEndDateStr ? 
                                   { start: userStartDateStr, end: userEndDateStr } : 
                                   { period: selectedPeriod };

                console.log(LOG_PREFIX, "Normalized series for chart:", normalizedSeries);
                // Call renderTimeseriesChart, but it needs to be adapted for multi-series and different y-axis
                // For now, let's assume renderTimeseriesChart can handle this new data structure if we pass a special chartType
                renderTimeseriesChart(normalizedSeries, `Comparison: ${normalizedSeries.map(s=>s.ticker).join(', ')}`, interval, titleRange, 'performance_comparison_line');

            }).catch(overallError => {
                showLoadingIndicator(false);
                console.error(LOG_PREFIX, "Generic error in Promise.allSettled for PPC:", overallError);
                showPlaceholderWithMessage("An unexpected error occurred while processing comparison data.");
            });
    }

    // --- NEW: Handler for Study Selection Change (Basic for now) ---
    function handleStudySelectionChange(event) {
        const selectedStudy = event.target.value;
        console.log(LOG_PREFIX, `Study changed to: ${selectedStudy}`);

        document.querySelectorAll('.study-config-pane').forEach(pane => {
            pane.style.display = 'none';
        });

        const selectedPaneId = `config-pane-${selectedStudy}`;
        const selectedPane = document.getElementById(selectedPaneId);
        if (selectedPane) {
            selectedPane.style.display = 'block';
            console.log(LOG_PREFIX, `Displayed config pane: ${selectedPaneId}`);

            // NEW: Ensure correct date input visibility for the newly shown pane based on ITS OWN period selector
            if (selectedStudy === 'price_history') {
                if (priceHistoryPeriodSelector && priceHistoryStartDateContainer && priceHistoryEndDateContainer) {
                    const isCustom = priceHistoryPeriodSelector.value === 'custom';
                    priceHistoryStartDateContainer.style.display = isCustom ? 'block' : 'none';
                    priceHistoryEndDateContainer.style.display = isCustom ? 'block' : 'none';
                }
            } else if (selectedStudy === 'price_performance_comparison') {
                if (ppcPeriodSelector && ppcStartDateContainer && ppcEndDateContainer) {
                    const isCustom = ppcPeriodSelector.value === 'custom';
                    ppcStartDateContainer.style.display = isCustom ? 'block' : 'none';
                    ppcEndDateContainer.style.display = isCustom ? 'block' : 'none';
                }
            }
        } else {
            console.warn(LOG_PREFIX, `Config pane for study '${selectedStudy}' (ID: ${selectedPaneId}) not found.`);
        }
    }

    // --- NEW: Handler for when main analytics transformation is complete ---
    function handleAnalyticsTransformationComplete() {
        console.log(LOG_PREFIX, "'AnalyticsTransformComplete' event received. Populating tickers and finalizing setup.");
        if (timeseriesTabPane && !timeseriesTabPane.classList.contains('data-initialized')) {
            populateLoadedTickerSelect();
            // handleStudySelectionChange might be re-run if necessary, or ensure initial setup is sufficient
            // For now, populateLoadedTickerSelect is the key data-dependent step here.
            timeseriesTabPane.classList.add('data-initialized'); // Mark that data-dependent init has run
            console.log(LOG_PREFIX, "Timeseries module data-dependent initialization complete.");
        } else if (timeseriesTabPane && timeseriesTabPane.classList.contains('data-initialized')){
            console.log(LOG_PREFIX, "'AnalyticsTransformComplete' received, but data already initialized. Repopulating tickers.");
            populateLoadedTickerSelect(); // Repopulate if transform runs again
        }
    }

    // --- Data Fetching & Processing ---
    function getFinalAnalyticsData() {
        console.log(LOG_PREFIX, "getFinalAnalyticsData called");
        if (window.AnalyticsMainModule && typeof window.AnalyticsMainModule.getFinalDataForAnalysis === 'function') {
            const finalDataPackage = window.AnalyticsMainModule.getFinalDataForAnalysis();
            console.log(LOG_PREFIX, "finalDataPackage from AnalyticsMainModule:", finalDataPackage); // Can be verbose

            if (Array.isArray(finalDataPackage) && finalDataPackage.length > 0) {
                console.log(LOG_PREFIX, "getFinalAnalyticsData - Returning direct array data.");
                return finalDataPackage;
            } else if (finalDataPackage && finalDataPackage.originalData && Array.isArray(finalDataPackage.originalData)) {
                console.log(LOG_PREFIX, "getFinalAnalyticsData - Returning data from originalData property.");
                return finalDataPackage.originalData;
            }
            console.warn(LOG_PREFIX, "getFinalAnalyticsData - Data is not a direct array and originalData property is missing/not an array or empty.");
            return [];
        } else {
            console.warn(LOG_PREFIX, "getFinalAnalyticsData - AnalyticsMainModule or getFinalDataForAnalysis function not available.");
            return [];
        }
    }

    function populateLoadedTickerSelect() {
        console.log(LOG_PREFIX, "populateLoadedTickerSelect called");
        if (!priceHistoryLoadedTickerSelect && !ppcLoadedTickerSelect) {
            console.warn(LOG_PREFIX, "Price history and PPC loaded ticker select elements not found.");
            return;
        }

        const analyticsOriginalData = getFinalAnalyticsData();
        console.log(LOG_PREFIX, "populateLoadedTickerSelect - analyticsOriginalData count:", analyticsOriginalData.length);
        const uniqueTickers = new Set();
        
        if (analyticsOriginalData && Array.isArray(analyticsOriginalData)) {
            analyticsOriginalData.forEach((item, index) => {
                if (item && item.ticker) {
                    uniqueTickers.add(item.ticker);
                } else {
                    // console.debug(LOG_PREFIX, `populateLoadedTickerSelect - Item at index ${index} is missing ticker. Item:`, item); // Can be verbose
                }
            });
        } else {
            console.warn(LOG_PREFIX, "populateLoadedTickerSelect - analyticsOriginalData is not an array or is null.");
        }

        const sortedTickers = Array.from(uniqueTickers).sort();

        // Populate Price History single select
        if (priceHistoryLoadedTickerSelect) {
            priceHistoryLoadedTickerSelect.innerHTML = ''; // Clear existing options
            if (sortedTickers.length > 0) {
                const placeholderOption = document.createElement('option');
                placeholderOption.value = "";
                placeholderOption.textContent = "Select a Ticker...";
                placeholderOption.disabled = true;
                placeholderOption.selected = true;
                priceHistoryLoadedTickerSelect.appendChild(placeholderOption);
                sortedTickers.forEach(ticker => {
                    const option = document.createElement('option');
                    option.value = ticker;
                    option.textContent = ticker;
                    priceHistoryLoadedTickerSelect.appendChild(option);
                });
            } else {
                const noTickerOption = document.createElement('option');
                noTickerOption.value = "";
                noTickerOption.textContent = "No tickers in loaded data";
                priceHistoryLoadedTickerSelect.appendChild(noTickerOption);
            }
            console.log(LOG_PREFIX, "populateLoadedTickerSelect - Populated Price History select with tickers:", sortedTickers);
        }

        // Populate Price Performance Comparison multi-select
        if (ppcLoadedTickerSelect) {
            ppcLoadedTickerSelect.innerHTML = ''; // Clear existing options
            if (sortedTickers.length > 0) {
                // No placeholder needed for multi-select usually, or make it non-selectable if desired
                sortedTickers.forEach(ticker => {
                    const option = document.createElement('option');
                    option.value = ticker;
                    option.textContent = ticker;
                    ppcLoadedTickerSelect.appendChild(option);
                });
            } else {
                const noTickerOption = document.createElement('option');
                noTickerOption.value = "";
                noTickerOption.textContent = "No tickers for comparison";
                noTickerOption.disabled = true; // Make it unselectable
                ppcLoadedTickerSelect.appendChild(noTickerOption);
            }
            console.log(LOG_PREFIX, "populateLoadedTickerSelect - Populated PPC multi-select with tickers:", sortedTickers);
        }
    }

    async function fetchTickerItemData(tickers, dateRange /*, other_params */) {
        // Placeholder for fetching detailed historical data for specific tickers
        // from the ticker_data_item table via a backend endpoint.
        console.log(LOG_PREFIX, `TODO - Fetch ticker_data_item for ${tickers} in range ${dateRange}`);
        // const response = await fetch('/api/analytics/timeseries/ticker-data', {
        //     method: 'POST',
        //     headers: { 'Content-Type': 'application/json' },
        //     body: JSON.stringify({ tickers, dateRange /*, ... */ })
        // });
        // if (!response.ok) {
        //     console.error("AnalyticsTimeseriesModule: Error fetching ticker item data.");
        //     return null;
        // }
        // return await response.json();
        return Promise.resolve(null); // Placeholder
    }

    async function fetchExternalWebData(tickers, dateRange /*, source, params */) {
        // Placeholder for fetching data from external web sources (e.g., Yahoo Finance)
        // This might be direct or via a backend proxy.
        console.log(LOG_PREFIX, `TODO - Fetch external web data for ${tickers} from source XXX`);
        // Example for Yahoo (client-side or via backend):
        // const response = await fetch(`/api/analytics/timeseries/external-data?source=yahoo&tickers=${tickers.join(',')}&startDate=${dateRange.start}&endDate=${dateRange.end}`);
        // ...
        return Promise.resolve(null); // Placeholder
    }

    function synthesizeData(baseData, tickerItemDetails, externalDetails) {
        // Placeholder for combining:
        // 1. baseData (from finalDataForAnalysis)
        // 2. tickerItemDetails (from ticker_data_item table)
        // 3. externalDetails (from web sources)
        // This will involve aligning data points by date, handling missing data, etc.
        console.log(LOG_PREFIX, "TODO - Synthesize all data sources.");
        return []; // Placeholder for synthesized timeseries data
    }

    // --- Charting ---
    // MODIFIED: renderTimeseriesChart to handle different chart types and dynamic library loading
    function renderTimeseriesChart(apiData, ticker, interval, range, chartType) {
        console.log(LOG_PREFIX, "renderTimeseriesChart called for ticker:", ticker, "Data points:", apiData?.length, "Chart Type:", chartType);

        const chartCanvas = document.getElementById('ts-chart-canvas'); 
        const chartPlaceholder = document.getElementById('ts-chart-placeholder');

        if (!chartCanvas || !chartPlaceholder) {
            console.error(LOG_PREFIX, "Chart canvas (ts-chart-canvas) or placeholder (ts-chart-placeholder) not found!");
            return;
        }

        // Always destroy the old instance if it exists
        if (timeseriesChartInstance) {
            timeseriesChartInstance.destroy();
            timeseriesChartInstance = null;
        }
        // Ensure reset button is hidden initially or when chart is cleared
        if (tsResetZoomBtn) tsResetZoomBtn.style.display = 'none'; 

        if (!apiData || (chartType !== 'performance_comparison_line' && apiData.length === 0)) {
            showPlaceholderWithMessage(`No data available for ${ticker} with the selected parameters.`);
            return; 
        }
        
        const createChartLogic = () => {
            chartCanvas.style.display = 'block';
            chartPlaceholder.style.display = 'none';
            if (tsResetZoomBtn) tsResetZoomBtn.style.display = 'inline-block'; 

            const ctx = chartCanvas.getContext('2d');
            let datasets;
            let chartJsType; 
            let chartOptions = { 
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'top' },
                    tooltip: { mode: 'index', intersect: false },
                    zoom: {
                        pan: {
                            enabled: true,
                            mode: 'xy',
                            threshold: 5
                        },
                        zoom: {
                            wheel: {
                                enabled: true,
                            },
                            pinch: {
                                enabled: true
                            },
                            drag: {
                                enabled: true,
                                backgroundColor: 'rgba(0,123,255,0.2)'
                            },
                            mode: 'xy'
                        }
                    }
                },
                scales: {
                    y: {
                        title: { display: true, text: 'Price' },
                        beginAtZero: false 
                    }
                }
            };

            let specificTitlePart = "Price History";

            if (chartType === 'candlestick' || chartType === 'ohlc') {
                // Check for financial library components (candlestick or ohlc controller)
                if (!window.Chart || !window.Chart.controllers || 
                    !(window.Chart.controllers.candlestick || window.Chart.controllers.ohlc)) {
                    const errorMsg = `${chartType === 'candlestick' ? 'Candlestick' : 'OHLC'} chart components not available. Cannot render chart.`;
                    console.error(LOG_PREFIX, errorMsg);
                    alert(errorMsg + " Check if the financial library was loaded and registered correctly with Chart.js.");
                    showPlaceholderWithMessage(errorMsg);
                    return;
                }
                if (!apiData.every(d => d.hasOwnProperty('Open') && d.hasOwnProperty('High') && d.hasOwnProperty('Low') && d.hasOwnProperty('Close'))) {
                    const errorMsg = `Data for ${ticker} is missing required OHLC fields for a candlestick chart.`;
                    console.error(LOG_PREFIX, errorMsg);
                    alert("Error: Candlestick chart requires Open, High, Low, and Close data. Please ensure the API provides this.");
                    showPlaceholderWithMessage(errorMsg);
                    return;
                }

                datasets = [{
                    label: `${ticker} OHLC (${interval})`,
                    data: apiData.map(d => ({
                        x: new Date(d.Datetime || d.Date).valueOf(),
                        o: d.Open, h: d.High, l: d.Low, c: d.Close
                    })),
                    color: {
                        up: 'rgba(80, 160, 115, 0.8)', // Green for up
                        down: 'rgba(215, 85, 65, 0.8)', // Red for down
                        unchanged: 'rgba(150, 150, 150, 0.8)' // Grey for unchanged
                    },
                    borderColor: {
                        up: 'rgba(80, 160, 115, 1)',
                        down: 'rgba(215, 85, 65, 1)',
                        unchanged: 'rgba(150, 150, 150, 1)'
                    }
                }];
                chartJsType = chartType;
                specificTitlePart = chartType === 'candlestick' ? "Candlestick Chart" : "OHLC Chart";
                chartOptions.scales.x = {
                    type: 'time',
                    time: { 
                        tooltipFormat: 'MMM d, yyyy, HH:mm' 
                    },
                    title: { display: true, text: 'Date' },
                    ticks: { source: 'auto', maxRotation: 45, minRotation: 0 }
                };
                // Add custom tooltip callbacks for OHLC data
                chartOptions.plugins.tooltip.callbacks = {
                    label: function(tooltipItem) {
                        const raw = tooltipItem.raw;
                        if (raw && typeof raw.o === 'number' && typeof raw.h === 'number' && 
                            typeof raw.l === 'number' && typeof raw.c === 'number') {
                            return [
                                `Open:  ${raw.o.toFixed(2)}`,
                                `High:  ${raw.h.toFixed(2)}`,
                                `Low:   ${raw.l.toFixed(2)}`,
                                `Close: ${raw.c.toFixed(2)}`
                            ];
                        }
                        // Fallback (should not be hit often for these chart types if data is correct)
                        let label = tooltipItem.dataset.label || '';
                        if (label) {
                            label += ': ';
                        }
                        if (tooltipItem.formattedValue) {
                            label += tooltipItem.formattedValue;
                        }
                        return label;
                    }
                };
                 // Might want to disable main legend for candlestick if OHLC label is enough
                // chartOptions.plugins.legend = { display: false };
            } else if (chartType === 'performance_comparison_line') {
                console.log(LOG_PREFIX, "Configuring for performance_comparison_line chart type.");
                // apiData here is the normalizedSeries: [{ ticker: 'X', data: [{x,y},...] }, ...]
                
                chartJsType = 'line';
                specificTitlePart = "Price Performance Comparison";

                // Define an array of distinct colors for the lines
                const lineColors = [
                    'rgb(75, 192, 192)', 'rgb(255, 99, 132)', 'rgb(54, 162, 235)',
                    'rgb(255, 206, 86)', 'rgb(153, 102, 255)', 'rgb(255, 159, 64)',
                    'rgb(199, 199, 199)', 'rgb(83, 102, 255)', 'rgb(100, 255, 100)'
                ];

                datasets = apiData.map((series, index) => ({
                    label: series.ticker + " Performance",
                    data: series.data, // Already in {x, y} format
                    borderColor: lineColors[index % lineColors.length], // Cycle through colors
                    backgroundColor: lineColors[index % lineColors.length].replace('rgb', 'rgba').replace(')', ',0.1)'), // Slight fill for visibility
                    borderWidth: 2, // Slightly thicker lines for comparison
                    fill: false,
                    pointRadius: 0, // No points by default, lines are clearer
                    tension: 0.1
                }));

                chartOptions.scales.x = {
                    type: 'time',
                    time: {
                        tooltipFormat: 'MMM d, yyyy' + (['15m', '30m', '1h'].includes(interval) ? ', HH:mm' : '')
                    },
                    title: { display: true, text: 'Date' + (['15m', '30m', '1h'].includes(interval) ? '/Time' : '') },
                    ticks: { source: 'auto', maxRotation: 45, minRotation: 0 }
                };
                chartOptions.scales.y = {
                    title: { display: true, text: 'Performance (%)' },
                    ticks: {
                        callback: function(value) {
                            return value + '%';
                        }
                    },
                    // beginAtZero is not strictly needed as data starts at 0% but doesn't hurt
                };

                chartOptions.plugins.tooltip.callbacks = {
                    label: function(tooltipItem) {
                        let label = tooltipItem.dataset.label || '';
                        if (label) {
                            label += ': ';
                        }
                        if (tooltipItem.parsed.y !== null) {
                            label += tooltipItem.parsed.y.toFixed(2) + '%';
                        }
                        return label;
                    }
                };

            } else { // Default to line chart (for single ticker price history)
                const isIntraday = ['15m', '30m', '1h'].includes(interval); // Add other intraday intervals if supported

                if (isIntraday) {
                    // Configure for time axis (intraday)
                    chartOptions.scales.x = {
                        type: 'time',
                        time: {
                            tooltipFormat: 'MMM d, yyyy, HH:mm'
                        },
                        title: { display: true, text: 'Date/Time' }, 
                        ticks: { source: 'auto', maxRotation: 45, minRotation: 0 }
                    };
                    // Data format for time axis: {x: timestamp, y: value}
                    const lineData = apiData.map(d => ({
                        x: new Date(d.Datetime || d.Date).valueOf(),
                        y: d.Close
                    }));
                    datasets = [{
                        label: `Close Price (${interval})`,
                        data: lineData,
                        borderColor: 'rgb(75, 192, 192)',
                        backgroundColor: 'rgba(75, 192, 192, 0.1)',
                        borderWidth: 1,
                        fill: false,
                        pointRadius: 0
                    }];
                } else {
                    // Configure for category axis (daily or longer)
                    const labels = apiData.map(d => new Date(d.Datetime || d.Date).toLocaleDateString());
                    chartOptions.scales.x = {
                        type: 'category',
                        labels: labels, // Pass labels for category axis
                        title: { display: true, text: 'Date' },
                        ticks: { maxRotation: 45, minRotation: 0 }
                    };
                    // Data format for category axis: array of values
                    const closePrices = apiData.map(d => d.Close);
                    datasets = [{
                        label: `Close Price (${interval})`,
                        data: closePrices,
                        borderColor: 'rgb(75, 192, 192)',
                        backgroundColor: 'rgba(75, 192, 192, 0.1)',
                        borderWidth: 1,
                        fill: false,
                        pointRadius: 0
                    }];
                }
                chartJsType = 'line';
                specificTitlePart = "Line Chart";
            }

            let titleRangePart = "";
            if (range && range.period && range.period !== 'custom') {
                titleRangePart = `Period: ${range.period.toUpperCase()}`;
            } else if (range && range.start && range.end) {
                titleRangePart = `Range: ${new Date(range.start).toLocaleDateString()} - ${new Date(range.end).toLocaleDateString()}`;
            }
            
            chartOptions.plugins.title = {
                display: true,
                // For performance comparison, 'ticker' var is a comma-separated string of tickers
                text: `${chartType === 'performance_comparison_line' ? '' : ticker + " - "}${specificTitlePart} ${titleRangePart ? '('+titleRangePart+')' : ''}`.trim(),
                font: { size: 16 }
            };

            timeseriesChartInstance = new Chart(ctx, {
                type: chartJsType,
                data: { datasets: datasets }, 
                options: chartOptions
            });
            console.log(LOG_PREFIX, "Chart rendered successfully for", ticker, "as", chartJsType);
        };

        if (chartType === 'candlestick' || chartType === 'ohlc') {
            showLoadingIndicator(true); // Show loading indicator
            loadDateAdapterLibrary() // First, load the date adapter
                .then(() => {
                    // Then, load the financial chart library
                    return loadFinancialChartLibrary(); 
                })
                .then(() => {
                    // Both libraries loaded, now create the chart
                    showLoadingIndicator(false);
                    createChartLogic();
                })
                .catch(error => {
                    showLoadingIndicator(false);
                    const errorMsg = `Failed to load required libraries for ${chartType === 'candlestick' ? 'candlestick' : 'OHLC'} charts: ${error.message}`;
                    console.error(LOG_PREFIX, errorMsg, error);
                    alert(errorMsg + " Please check the console for details.");
                    showPlaceholderWithMessage(errorMsg);
                    if (tsResetZoomBtn) tsResetZoomBtn.style.display = 'none'; // Also hide on error
                });
        } else {
            createChartLogic(); // For line chart, no special library needed beyond core Chart.js
        }
    }

    // --- Main Execution Logic ---
    // Example of how a user interaction might trigger the process:
    // async function handleGenerateChart() {
    //     const baseData = getFinalAnalyticsData();
    //     const selectedTickers = /* get from UI */;
    //     const selectedDateRange = /* get from UI */;

    //     const [tickerDetails, externalData] = await Promise.all([
    //         fetchTickerItemData(selectedTickers, selectedDateRange),
    //         fetchExternalWebData(selectedTickers, selectedDateRange)
    //     ]);

    //     const synthesizedSeries = synthesizeData(baseData, tickerDetails, externalData);
    //     renderTimeseriesChart(synthesizedSeries);
    // }

    // --- Initialize ---
    // Ensure the main analytics module is loaded if it exposes necessary data/functions
    // REMOVED attemptInitialization function and its direct call

    // Initial non-data-dependent setup
    initializeTimeseriesModule(); 

    // --- Expose functions if needed by other modules (less common for a tab-specific module) ---
    // window.AnalyticsTimeseriesModule = {
    //     // functionsToExpose
    // };

    // Example for a loading indicator (you would need to implement the UI for this)
    function showLoadingIndicator(show) {
        const indicator = document.getElementById('timeseries-loading-indicator'); 
        if (indicator) {
            indicator.style.display = show ? 'flex' : 'none'; // Use flex for better alignment of spinner and text
        }
        if (priceHistoryRunButton) {
            priceHistoryRunButton.disabled = show;
            priceHistoryRunButton.innerHTML = show ? '<span class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>Loading...' : 'Run';
        }
    }

    // --- Call initial setup for study config visibility AFTER all functions are defined ---
    // This call was already inside initializeTimeseriesModule and also at the end.
    // We keep the one in initializeTimeseriesModule called on DOMContentLoaded for early UI setup.
    // The one at the end is removed as initializeTimeseriesModule handles it now.

    // Listen for the main analytics module to signal data readiness
    console.log(LOG_PREFIX, "Adding event listener for 'AnalyticsTransformComplete' to WINDOW.");
    window.addEventListener('AnalyticsTransformComplete', handleAnalyticsTransformationComplete);
}); 