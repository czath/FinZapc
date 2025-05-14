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

        if (priceHistoryPeriodSelector) {
            priceHistoryPeriodSelector.addEventListener('change', function() {
                const isCustom = this.value === 'custom';
                if (priceHistoryStartDateContainer) priceHistoryStartDateContainer.style.display = isCustom ? 'block' : 'none';
                if (priceHistoryEndDateContainer) priceHistoryEndDateContainer.style.display = isCustom ? 'block' : 'none';
            });
            // Trigger change on load to set initial state of date inputs
            if (priceHistoryStartDateContainer && priceHistoryEndDateContainer) { // Ensure containers exist
                 const initialIsCustomPeriod = priceHistoryPeriodSelector.value === 'custom';
                 priceHistoryStartDateContainer.style.display = initialIsCustomPeriod ? 'block' : 'none';
                 priceHistoryEndDateContainer.style.display = initialIsCustomPeriod ? 'block' : 'none';
            }
        } else {
            console.warn(LOG_PREFIX, "Period selector (ts-ph-period-selector) not found.");
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

        let queryParams = `ticker=${encodeURIComponent(ticker)}&interval=${encodeURIComponent(interval)}`;

        if (selectedPeriod === 'custom') {
            if (!priceHistoryStartDateInput || !priceHistoryEndDateInput) {
                console.error(LOG_PREFIX, "Price History Start/End Date inputs not found!");
                alert("Error: Date input components are missing for custom range.");
                return;
            }
            const startDate = priceHistoryStartDateInput.value;
            const endDate = priceHistoryEndDateInput.value;
            if (!startDate || !endDate) {
                alert("Please select a start and end date for custom range.");
                return;
            }
            if (new Date(startDate) >= new Date(endDate)) {
                alert("Start date must be before end date.");
                return;
            }
            queryParams += `&start_date=${encodeURIComponent(startDate)}&end_date=${encodeURIComponent(endDate)}`;
        } else {
            queryParams += `&period=${encodeURIComponent(selectedPeriod)}`;
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
                // alert(`Successfully fetched ${data.length} records for ${ticker}. Charting next.`);
                renderTimeseriesChart(data, ticker, interval, selectedPeriod === 'custom' ? {start: priceHistoryStartDateInput.value, end: priceHistoryEndDateInput.value} : {period: selectedPeriod}, chartType);
            } else {
                // alert(`No price history data found for ${ticker} with the selected parameters.`); // Now handled by renderTimeseriesChart
                renderTimeseriesChart([], ticker, interval, selectedPeriod === 'custom' ? {start: priceHistoryStartDateInput.value, end: priceHistoryEndDateInput.value} : {period: selectedPeriod}, chartType); // MODIFIED: Pass chartType, Clear chart
            }

        } catch (error) {
            showLoadingIndicator(false);
            console.error(LOG_PREFIX, "Network or other error fetching price history:", error);
            alert(`Failed to fetch price history: ${error.message}`);
        }
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
        if (!priceHistoryLoadedTickerSelect) {
            console.warn(LOG_PREFIX, "priceHistoryLoadedTickerSelect element not found.");
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

        priceHistoryLoadedTickerSelect.innerHTML = ''; // Clear existing options more cleanly
        if (uniqueTickers.size > 0) {
            // Add a default placeholder option
            const placeholderOption = document.createElement('option');
            placeholderOption.value = "";
            placeholderOption.textContent = "Select a Ticker...";
            placeholderOption.disabled = true;
            placeholderOption.selected = true;
            priceHistoryLoadedTickerSelect.appendChild(placeholderOption);

            Array.from(uniqueTickers).sort().forEach(ticker => {
                const option = document.createElement('option');
                option.value = ticker;
                option.textContent = ticker;
                priceHistoryLoadedTickerSelect.appendChild(option);
            });
            console.log(LOG_PREFIX, "populateLoadedTickerSelect - Populated with tickers:", Array.from(uniqueTickers).sort());
        } else {
            const noTickerOption = document.createElement('option');
            noTickerOption.value = "";
            noTickerOption.textContent = "No tickers in loaded data";
            priceHistoryLoadedTickerSelect.appendChild(noTickerOption);
            console.warn(LOG_PREFIX, "populateLoadedTickerSelect - No unique tickers found.");
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

        if (!apiData || apiData.length === 0) {
            showPlaceholderWithMessage(`No data available for ${ticker} with the selected parameters.`);
            return;
        }

        const createChartLogic = () => {
            chartCanvas.style.display = 'block';
            chartPlaceholder.style.display = 'none';

            const ctx = chartCanvas.getContext('2d');
            let datasets;
            let chartJsType; // This will be 'line', 'candlestick'
            let chartOptions = { // Base options
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'top' },
                    tooltip: { mode: 'index', intersect: false }
                },
                scales: {
                    y: {
                        title: { display: true, text: 'Price' },
                        beginAtZero: false
                    }
                }
            };

            let specificTitlePart = "Price History";

            if (chartType === 'candlestick') {
                if (!window.Chart || !window.Chart.controllers || !window.Chart.controllers.candlestick) {
                    const errorMsg = "Candlestick chart components (e.g., Chart.controllers.candlestick) not available. Cannot render chart.";
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
                        x: new Date(d.Date).valueOf(),
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
                chartJsType = 'candlestick';
                specificTitlePart = "Candlestick Chart";
                chartOptions.scales.x = {
                    type: 'time',
                    time: { unit: 'day' /* auto-detect or make configurable */ },
                    title: { display: true, text: 'Date' },
                    ticks: { source: 'auto', maxRotation: 45, minRotation: 0 }
                };
                 // Might want to disable main legend for candlestick if OHLC label is enough
                // chartOptions.plugins.legend = { display: false };
            } else { // Default to line chart
                const labels = apiData.map(d => new Date(d.Date).toLocaleDateString());
                const closePrices = apiData.map(d => d.Close);
                datasets = [{
                    label: `Close Price (${interval})`,
                    data: closePrices, // Line chart data is simpler
                    borderColor: 'rgb(75, 192, 192)',
                    backgroundColor: 'rgba(75, 192, 192, 0.1)',
                    tension: 0.1,
                    fill: true
                }];
                chartJsType = 'line';
                specificTitlePart = "Line Chart";
                chartOptions.scales.x = {
                    type: 'category',
                    labels: labels, // Pass labels for category axis
                    title: { display: true, text: 'Date' },
                    ticks: { maxRotation: 45, minRotation: 0 }
                };
            }

            let titleRangePart = "";
            if (range.period && range.period !== 'custom') {
                titleRangePart = `Period: ${range.period.toUpperCase()}`;
            } else if (range.start && range.end) {
                titleRangePart = `Range: ${new Date(range.start).toLocaleDateString()} - ${new Date(range.end).toLocaleDateString()}`;
            }
            
            chartOptions.plugins.title = {
                display: true,
                text: `${ticker} - ${specificTitlePart} ${titleRangePart ? '('+titleRangePart+')' : ''}`.trim(),
                font: { size: 16 }
            };

            timeseriesChartInstance = new Chart(ctx, {
                type: chartJsType,
                data: { datasets: datasets }, // For line chart with category x-axis, labels are in options.scales.x.labels
                options: chartOptions
            });
            console.log(LOG_PREFIX, "Chart rendered successfully for", ticker, "as", chartJsType);
        }; // End of createChartLogic

        if (chartType === 'candlestick') {
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
                    const errorMsg = `Failed to load required libraries for candlestick charts: ${error.message}`;
                    console.error(LOG_PREFIX, errorMsg, error);
                    alert(errorMsg + " Please check the console for details.");
                    showPlaceholderWithMessage(errorMsg);
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