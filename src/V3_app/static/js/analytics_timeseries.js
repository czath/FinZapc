document.addEventListener('DOMContentLoaded', function() {
    const LOG_PREFIX = "TimeseriesModule:";
    console.log(LOG_PREFIX, "DOMContentLoaded event fired.");

    // --- Custom Chart.js Plugin for Last Value Indicator ---
    const lastValueIndicatorPlugin = {
        id: 'lastValueIndicator',
        afterDatasetsDraw(chart, args, pluginOptions) {
            const { ctx, chartArea: { right, top, bottom, left, width, height }, scales: { x: xScale, y: yScale } } = chart;
            console.log(LOG_PREFIX, '[IndicatorPlugin] afterDatasetsDraw triggered. chartArea.right:', right);

            pluginOptions = pluginOptions || {}; 
            const defaultFont = '10px Arial';
            const defaultTextColor = '#333'; // Fallback, should be overridden by theme
            const defaultXPadding = 5;

            chart.data.datasets.forEach((dataset, i) => {
                console.log(LOG_PREFIX, `[IndicatorPlugin] Processing dataset ${i}: ${dataset.label}, Visible: ${chart.isDatasetVisible(i)}`);
                if (!chart.isDatasetVisible(i) || !dataset.data || dataset.data.length === 0) {
                    console.log(LOG_PREFIX, `[IndicatorPlugin] Dataset ${i} skipped (not visible or no data).`);
                    return; 
                }

                const meta = chart.getDatasetMeta(i);
                console.log(LOG_PREFIX, `[IndicatorPlugin] Dataset ${i} meta.data length: ${meta.data ? meta.data.length : 'undefined'}`);
                if (!meta.data || meta.data.length === 0) {
                    console.log(LOG_PREFIX, `[IndicatorPlugin] Dataset ${i} skipped (no meta.data).`);
                    return; 
                }

                const lastElement = meta.data[meta.data.length - 1];
                const yPosition = lastElement ? lastElement.y : NaN; // Guard against lastElement being undefined
                console.log(LOG_PREFIX, `[IndicatorPlugin] Dataset ${i} lastElement exists: ${!!lastElement}, yPosition: ${yPosition}`);


                if (yPosition < top || yPosition > bottom) {
                     // console.log(LOG_PREFIX, `[IndicatorPlugin] Dataset ${i} yPosition ${yPosition} is outside chartArea (top: ${top}, bottom: ${bottom}). Skipping.`);
                    // return; // Still keeping this commented to see if text appears anywhere if yPosition is odd.
                }

                let textToDisplay = '';
                let valueToFormat = NaN;
                const appChartType = pluginOptions.appChartType || chart.config.type; 
                const rawLastDataPoint = dataset.data[dataset.data.length - 1];
                console.log(LOG_PREFIX, `[IndicatorPlugin] Dataset ${i} appChartType: ${appChartType}, rawLastDataPoint:`, rawLastDataPoint);


                if (!rawLastDataPoint) {
                    console.log(LOG_PREFIX, `[IndicatorPlugin] Dataset ${i} skipped (no rawLastDataPoint).`);
                    return;
                }

                try { 
                    if (appChartType === 'performance_comparison_line') {
                        valueToFormat = rawLastDataPoint.y;
                        textToDisplay = 'N/A';

                        const isPercentChange = chart.data.datasets[i].appDataType === 'percent_change';
                        const isRawOrDefault = chart.data.datasets[i].appDataType === 'raw' || typeof chart.data.datasets[i].appDataType === 'undefined';

                        if (valueToFormat !== null && typeof valueToFormat !== 'undefined') {
                            if (typeof valueToFormat === 'number') {
                                let decimals = 2;
                                const isRawAndSmall = isRawOrDefault &&
                                                      Math.abs(valueToFormat) > 0 && 
                                                      Math.abs(valueToFormat) < 0.01;

                                if (isRawAndSmall) {
                                     decimals = Math.max(2, -Math.floor(Math.log10(Math.abs(valueToFormat))) + 1); 
                                }
                                
                                textToDisplay = valueToFormat.toFixed(decimals);

                                if (isPercentChange) {
                                    textToDisplay += '%';
                                }
                            } else {
                                textToDisplay = String(valueToFormat); 
                                if (isPercentChange) {
                                    textToDisplay += '%';
                                }
                            }
                        } else {
                            textToDisplay = 'N/A';
                        }
                    } else if (appChartType === 'pair_relative_price_line') {
                        valueToFormat = rawLastDataPoint.y;
                        textToDisplay = typeof valueToFormat === 'number' && isFinite(valueToFormat) ? valueToFormat.toFixed(4) : '';
                    } else if (appChartType === 'candlestick' || appChartType === 'ohlc') {
                        valueToFormat = rawLastDataPoint.c; // Close price for OHLC
                        textToDisplay = typeof valueToFormat === 'number' && isFinite(valueToFormat) ? valueToFormat.toFixed(2) : '';
                    } else if (appChartType === 'line') {
                        if (typeof rawLastDataPoint === 'object' && rawLastDataPoint !== null && typeof rawLastDataPoint.y === 'number') {
                            valueToFormat = rawLastDataPoint.y; // {x,y} format
                        } else if (typeof rawLastDataPoint === 'number') { // THIS CASE IS UNLIKELY FOR LINE CHARTS NOW
                            valueToFormat = rawLastDataPoint; 
                        }
                        // For line charts, data is now expected as {x, y} or array of numbers for non-time series.
                        // For price history line (non-intraday), it's an array of Close prices.
                        // For intraday line, it's {x, y} with y as Close.
                        // The existing logic assumes rawLastDataPoint.y or rawLastDataPoint itself.
                        // Let's ensure this covers the structure from renderTimeseriesChart's line chart processing.
                        // If data for line chart is `apiData.map(d => d.Close)` (non-intraday)
                        // then `rawLastDataPoint` is a number.
                        // If data is `apiData.map(d => ({x: ..., y: d.Close}))` (intraday)
                        // then `rawLastDataPoint` is an object with `y`.
                         if (typeof rawLastDataPoint === 'object' && rawLastDataPoint !== null && rawLastDataPoint.hasOwnProperty('Close')) {
                            // This case might be from an older configuration or if data structure is just {Close: val}
                            // but current line chart prep uses 'y' or direct numbers.
                            // Let's ensure we're robust. The plugin is getting raw data from dataset.data
                            valueToFormat = rawLastDataPoint.Close;
                         } else if (typeof rawLastDataPoint === 'object' && rawLastDataPoint !== null && typeof rawLastDataPoint.y === 'number') {
                            valueToFormat = rawLastDataPoint.y;
                         } else if (typeof rawLastDataPoint === 'number') {
                            valueToFormat = rawLastDataPoint;
                         }

                        textToDisplay = typeof valueToFormat === 'number' && isFinite(valueToFormat) ? valueToFormat.toFixed(2) : '';
                    } else {
                        console.log(LOG_PREFIX, `[IndicatorPlugin] Dataset ${i} unknown appChartType: ${appChartType}`);
                        return; 
                    }
                    console.log(LOG_PREFIX, `[IndicatorPlugin] Dataset ${i} valueToFormat: ${valueToFormat}, textToDisplay: '${textToDisplay}'`);
                } catch (e) {
                    console.warn(LOG_PREFIX, '[IndicatorPlugin] Error processing data point for last value indicator:', e, rawLastDataPoint);
                    return;
                }
                
                if (textToDisplay === '' || isNaN(yPosition)) {
                    console.log(LOG_PREFIX, `[IndicatorPlugin] Dataset ${i} skipped (textToDisplay is empty or yPosition is NaN). text: '${textToDisplay}', y: ${yPosition}`);
                    return;
                }

                ctx.save();
                
                ctx.font = pluginOptions.font || defaultFont;

                let finalTextColor = pluginOptions.textColor || defaultTextColor; 
                if (!pluginOptions.textColor && 
                    typeof dataset.borderColor === 'string' && 
                    !(appChartType === 'candlestick' || appChartType === 'ohlc')) {
                    finalTextColor = dataset.borderColor;
                }
                ctx.fillStyle = finalTextColor;
                console.log(LOG_PREFIX, `[IndicatorPlugin] Dataset ${i} finalTextColor: ${finalTextColor}`);
                
                ctx.textAlign = 'left';
                ctx.textBaseline = 'middle';

                const xPadding = pluginOptions.xPadding || defaultXPadding;
                const xPosition = right + xPadding; // REVERTED: Draw to the right of the (new) chartArea.right
                // const xPosition = right - 30; // TEMP: Draw further left for testing
                console.log(LOG_PREFIX, `[IndicatorPlugin] Dataset ${i} Drawing at x: ${xPosition}, y: ${yPosition} (chartArea.right was ${right})`);

                // Pill background
                // Use pluginOptions.pillBackgroundColor if provided, otherwise use dataset.borderColor, else default.
                let actualPillBackgroundColor = pluginOptions.pillBackgroundColor;
                if (!actualPillBackgroundColor) {
                    if (typeof dataset.borderColor === 'string') {
                        actualPillBackgroundColor = dataset.borderColor;
                    } else {
                        // Fallback for complex borderColor (like OHLC) or if not a string
                        actualPillBackgroundColor = 'rgba(200, 200, 200, 0.7)'; // Neutral grey
                    }
                }

                if (actualPillBackgroundColor) { // Check if we have a color to draw the pill
                    const textMetrics = ctx.measureText(textToDisplay);
                    const pillHPadding = pluginOptions.pillHPadding || 4;
                    const pillVPadding = pluginOptions.pillVPadding || 2;
                    const pillWidth = textMetrics.width + (pillHPadding * 2);
                    const pillHeight = (textMetrics.actualBoundingBoxAscent || parseInt(ctx.font)) + (pillVPadding * 2);
                    const pillRadius = pluginOptions.pillBorderRadius || 3;

                    ctx.fillStyle = actualPillBackgroundColor;
                    ctx.beginPath();
                    ctx.roundRect(xPosition - pillHPadding, yPosition - pillHeight / 2, pillWidth, pillHeight, pillRadius);
                    ctx.fill();
                    
                    // ctx.fillStyle = finalTextColor; // OLD: Reset fillStyle for text to theme-aware color
                    ctx.fillStyle = '#000000'; // NEW: Set text color inside pill to black
                    console.log(LOG_PREFIX, `[IndicatorPlugin] Dataset ${i} Pill drawn with color: ${actualPillBackgroundColor}. Text color set to #000000.`);
                } else {
                    // If no pill is drawn, ensure the finalTextColor (theme-aware) is used for the text
                    ctx.fillStyle = finalTextColor;
                }

                console.log(LOG_PREFIX, `[IndicatorPlugin] Dataset ${i} Attempting ctx.fillText with text: '${textToDisplay}' using color ${ctx.fillStyle}`);
                ctx.fillText(textToDisplay, xPosition, yPosition);
                console.log(LOG_PREFIX, `[IndicatorPlugin] Dataset ${i} ctx.fillText DONE.`);
                ctx.restore();
            });
            console.log(LOG_PREFIX, '[IndicatorPlugin] afterDatasetsDraw finished.');
        }
    };

    Chart.register(lastValueIndicatorPlugin);

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

    // --- NEW: DOM Elements for Pair Relative Price Study (PRP) ---
    const prpTicker1SourceRadios = document.querySelectorAll('input[name="tsPrpTicker1Source"]');
    const prpTicker1LoadedSelect = document.getElementById('ts-prp-ticker1-select-loaded');
    const prpTicker1LoadedContainer = document.getElementById('ts-prp-ticker1-select-loaded-container');
    const prpTicker1ManualInput = document.getElementById('ts-prp-ticker1-input-manual');
    const prpTicker1ManualContainer = document.getElementById('ts-prp-ticker1-input-manual-container');

    const prpTicker2SourceRadios = document.querySelectorAll('input[name="tsPrpTicker2Source"]');
    const prpTicker2LoadedSelect = document.getElementById('ts-prp-ticker2-select-loaded');
    const prpTicker2LoadedContainer = document.getElementById('ts-prp-ticker2-select-loaded-container');
    const prpTicker2ManualInput = document.getElementById('ts-prp-ticker2-input-manual');
    const prpTicker2ManualContainer = document.getElementById('ts-prp-ticker2-input-manual-container');

    const prpPeriodSelector = document.getElementById('ts-prp-period-selector');
    const prpStartDateInput = document.getElementById('ts-prp-start-date');
    const prpEndDateInput = document.getElementById('ts-prp-end-date');
    const prpStartDateContainer = document.getElementById('ts-prp-start-date-container');
    const prpEndDateContainer = document.getElementById('ts-prp-end-date-container');
    const prpIntervalSelect = document.getElementById('ts-prp-interval');
    const prpRunButton = document.getElementById('ts-prp-run-study-btn');

    // --- NEW: DOM Elements for Price-Fundamental Ratios (PFR) ---
    const pfrTickerSelect = document.getElementById('ts-pfr-ticker-select');
    const pfrFieldSelect = document.getElementById('ts-pfr-field-select'); // Will be handled in fund.js
    const pfrPeriodSelector = document.getElementById('ts-pfr-period-selector');
    const pfrStartDateInput = document.getElementById('ts-pfr-start-date');
    const pfrEndDateInput = document.getElementById('ts-pfr-end-date');
    const pfrStartDateContainer = document.getElementById('ts-pfr-start-date-container');
    const pfrEndDateContainer = document.getElementById('ts-pfr-end-date-container');
    const pfrRunButton = document.getElementById('ts-pfr-run-study-btn'); // Will be handled in fund.js

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
        console.log(LOG_PREFIX, "Setting up event listeners for Timeseries module...");
        const studySelector = document.getElementById('ts-study-selector');

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

        if (studySelector) {
            studySelector.addEventListener('change', function(event) {
                console.log(LOG_PREFIX, "Study selector changed to:", event.target.value);
                const selectedStudy = event.target.value;
                document.querySelectorAll('.study-config-pane').forEach(pane => {
                    pane.style.display = 'none';
                });
                const activePane = document.getElementById(`config-pane-${selectedStudy}`);
                if (activePane) {
                    activePane.style.display = 'block';
                    console.log(LOG_PREFIX, "Displayed config pane:", `config-pane-${selectedStudy}`);

                    // Initialize specific study controls if needed
                    if (selectedStudy === 'price_performance_comparison') {
                        if (typeof initializePricePerformanceControls === 'function') {
                            initializePricePerformanceControls();
                        }
                    } else if (selectedStudy === 'pair_relative_price') {
                        if (typeof initializePairRelativePriceControls === 'function') {
                            initializePairRelativePriceControls();
                        }
                    } else if (selectedStudy === 'fundamentals_history') { // ADDED ELSE IF
                        if (typeof window.TimeseriesFundamentalsModule !== 'undefined' && 
                            typeof window.TimeseriesFundamentalsModule.initializeFundamentalsHistoryStudyControls === 'function') {
                            window.TimeseriesFundamentalsModule.initializeFundamentalsHistoryStudyControls();
                        } else {
                            console.warn(LOG_PREFIX, "TimeseriesFundamentalsModule or initializeFundamentalsHistoryStudyControls not found.");
                        }
                    } else if (selectedStudy === 'price_fundamental_comparison') { // NEW: Price-Fundamental Comparison
                        if (typeof window.TimeseriesFundamentalsModule !== 'undefined' &&
                            typeof window.TimeseriesFundamentalsModule.initializePriceFundamentalComparisonControls === 'function') {
                            window.TimeseriesFundamentalsModule.initializePriceFundamentalComparisonControls();
                        } else {
                            console.warn(LOG_PREFIX, "TimeseriesFundamentalsModule or initializePriceFundamentalComparisonControls not found.");
                        }
                    } else if (selectedStudy === 'price_fundamental_ratios') { // NEW: Price-Fundamental Ratios
                        if (typeof window.TimeseriesFundamentalsModule !== 'undefined' &&
                            typeof window.TimeseriesFundamentalsModule.initializePriceFundamentalRatiosControls === 'function') {
                            window.TimeseriesFundamentalsModule.initializePriceFundamentalRatiosControls();
                        } else {
                            console.warn(LOG_PREFIX, "TimeseriesFundamentalsModule or initializePriceFundamentalRatiosControls not found.");
                        }
                    } else if (selectedStudy === 'synthetic_fundamentals') { // NEW: Synthetic Fundamentals
                        if (typeof window.TimeseriesFundamentalsModule !== 'undefined' &&
                            typeof window.TimeseriesFundamentalsModule.initializeSyntheticFundamentalsControls === 'function') {
                            window.TimeseriesFundamentalsModule.initializeSyntheticFundamentalsControls();
                        } else {
                            console.warn(LOG_PREFIX, "TimeseriesFundamentalsModule or initializeSyntheticFundamentalsControls not found.");
                        }
                    }
                }
                // Always show placeholder if no study implies direct chart rendering
                // or if the selected study requires setup before plotting.
                showPlaceholderWithMessage("Configure study and click 'Run' to generate chart.");
            });
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

        // --- NEW: Event Listeners for PRP Ticker Source Radios ---
        if (prpTicker1SourceRadios && prpTicker1LoadedContainer && prpTicker1ManualContainer) {
            prpTicker1SourceRadios.forEach(radio => {
                radio.addEventListener('change', function() {
                    const isLoaded = this.value === 'loaded';
                    prpTicker1LoadedContainer.style.display = isLoaded ? 'block' : 'none';
                    prpTicker1ManualContainer.style.display = !isLoaded ? 'block' : 'none';
                });
            });
            // Set initial state for Ticker 1 based on checked radio (manual is default in HTML)
            const initialT1Source = document.querySelector('input[name="tsPrpTicker1Source"]:checked');
            if (initialT1Source) {
                const isLoaded = initialT1Source.value === 'loaded';
                prpTicker1LoadedContainer.style.display = isLoaded ? 'block' : 'none';
                prpTicker1ManualContainer.style.display = !isLoaded ? 'block' : 'none';
            } // else default HTML state (manual visible, loaded hidden)
        }

        if (prpTicker2SourceRadios && prpTicker2LoadedContainer && prpTicker2ManualContainer) {
            prpTicker2SourceRadios.forEach(radio => {
                radio.addEventListener('change', function() {
                    const isLoaded = this.value === 'loaded';
                    prpTicker2LoadedContainer.style.display = isLoaded ? 'block' : 'none';
                    prpTicker2ManualContainer.style.display = !isLoaded ? 'block' : 'none';
                });
            });
            // Set initial state for Ticker 2 based on checked radio (manual is default in HTML)
            const initialT2Source = document.querySelector('input[name="tsPrpTicker2Source"]:checked');
            if (initialT2Source) {
                const isLoaded = initialT2Source.value === 'loaded';
                prpTicker2LoadedContainer.style.display = isLoaded ? 'block' : 'none';
                prpTicker2ManualContainer.style.display = !isLoaded ? 'block' : 'none';
            } // else default HTML state
        }

        // --- NEW: Event Listener for Pair Relative Price (PRP) Period Selector ---
        if (prpPeriodSelector) {
            prpPeriodSelector.addEventListener('change', function() {
                const isCustom = this.value === 'custom';
                if (prpStartDateContainer) prpStartDateContainer.style.display = isCustom ? 'block' : 'none';
                if (prpEndDateContainer) prpEndDateContainer.style.display = isCustom ? 'block' : 'none';
            });
            // Initial setup for PRP date fields (will also be handled by handleStudySelectionChange when pane becomes active)
            if (prpStartDateContainer && prpEndDateContainer) {
                const initialIsCustomPrpPeriod = prpPeriodSelector.value === 'custom';
                prpStartDateContainer.style.display = initialIsCustomPrpPeriod ? 'block' : 'none';
                prpEndDateContainer.style.display = initialIsCustomPrpPeriod ? 'block' : 'none';
           }
        } else {
            console.warn(LOG_PREFIX, "Period selector (ts-prp-period-selector) for PRP not found.");
        }

        // --- NEW: Event Listener for Pair Relative Price (PRP) Run Button ---
        if (prpRunButton) {
            prpRunButton.addEventListener('click', handleRunPairRelativePrice);
        } else {
            console.warn(LOG_PREFIX, "Run Relative Price button (ts-prp-run-study-btn) not found.");
        }

        // NEW: Event listener for Fundamentals History run button
        const runFundamentalsHistoryBtn = document.getElementById('ts-fh-run-study-btn');
        if (runFundamentalsHistoryBtn) {
            runFundamentalsHistoryBtn.addEventListener('click', function() {
                if (typeof window.TimeseriesFundamentalsModule !== 'undefined' && 
                    typeof window.TimeseriesFundamentalsModule.handleRunFundamentalsHistory === 'function') {
                    window.TimeseriesFundamentalsModule.handleRunFundamentalsHistory();
                } else {
                    console.warn(LOG_PREFIX, "TimeseriesFundamentalsModule or handleRunFundamentalsHistory not found.");
                    showPlaceholderWithMessage("Error: Fundamentals History study logic is not available.");
                }
            });
        }

        // --- NEW: Event Listener for Price-Fundamental Ratios (PFR) Period Selector ---
        if (pfrPeriodSelector) {
            pfrPeriodSelector.addEventListener('change', function() {
                const isCustom = this.value === 'custom';
                if (pfrStartDateContainer) pfrStartDateContainer.style.display = isCustom ? 'block' : 'none';
                if (pfrEndDateContainer) pfrEndDateContainer.style.display = isCustom ? 'block' : 'none';
            });
            // Initial setup for PFR date fields (will also be handled by handleStudySelectionChange)
            if (pfrStartDateContainer && pfrEndDateContainer) {
                const initialIsCustomPfrPeriod = pfrPeriodSelector.value === 'custom';
                pfrStartDateContainer.style.display = initialIsCustomPfrPeriod ? 'block' : 'none';
                pfrEndDateContainer.style.display = initialIsCustomPfrPeriod ? 'block' : 'none';
            }
        } else {
            console.warn(LOG_PREFIX, "Period selector (ts-pfr-period-selector) for PFR not found.");
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
        console.log(LOG_PREFIX, "Run Price History clicked.");
        if (!priceHistoryRunButton || !priceHistoryIntervalSelect || !priceHistoryPeriodSelector || !priceHistoryTickerSourceRadios) {
            alert("Essential Price History UI elements are missing. Please refresh the page.");
            console.error(LOG_PREFIX, "Missing Price History UI elements.");
            return;
        }

        showLoadingIndicator(true);
        if (priceHistoryRunButton) priceHistoryRunButton.disabled = true;

        let ticker = '';
        const selectedSource = Array.from(priceHistoryTickerSourceRadios).find(radio => radio.checked).value;

        if (selectedSource === 'loaded') {
            if (priceHistoryLoadedTickerSelect) {
                ticker = priceHistoryLoadedTickerSelect.value;
            }
            if (!ticker) {
                alert("Please select a ticker from the loaded list.");
                showLoadingIndicator(false);
                if (priceHistoryRunButton) priceHistoryRunButton.disabled = false;
                return;
            }
        } else { // manual
            if (priceHistoryTickerInput) {
                ticker = priceHistoryTickerInput.value.trim().toUpperCase();
            }
            if (!ticker) {
                alert("Please enter a ticker symbol.");
                showLoadingIndicator(false);
                if (priceHistoryRunButton) priceHistoryRunButton.disabled = false;
                return;
            }
        }

        const interval = priceHistoryIntervalSelect.value;
        const period = priceHistoryPeriodSelector.value;
        let startDate = priceHistoryStartDateInput.value;
        let endDate = priceHistoryEndDateInput.value;
        const chartType = tsPhChartTypeSelect ? tsPhChartTypeSelect.value : 'line'; // Default to line if not found

        console.log(LOG_PREFIX, `Price History Params: Ticker=${ticker}, Interval=${interval}, Period=${period}, Start=${startDate}, End=${endDate}, ChartType=${chartType}`);


        if (period === 'custom') {
            if (!startDate || !endDate) {
                alert("For custom range, please select both Start and End dates.");
                showLoadingIndicator(false);
                if (priceHistoryRunButton) priceHistoryRunButton.disabled = false;
                return;
            }
            if (new Date(startDate) >= new Date(endDate)) {
                alert("Start Date must be before End Date for custom range.");
                showLoadingIndicator(false);
                if (priceHistoryRunButton) priceHistoryRunButton.disabled = false;
                return;
            }
        } else {
            // For non-custom periods, start/end dates are derived or ignored by API if period is 'max'
            // No specific client-side validation needed here for start/end if period is not 'custom'
        }
        
        // MODIFICATION START: Try to get data from cache first
        let apiData = null;
        if (window.AnalyticsPriceCache) {
            console.log(LOG_PREFIX, `Attempting to fetch ${ticker} ${interval} for period '${period}' (${startDate}-${endDate}) from cache.`);
            apiData = window.AnalyticsPriceCache.getPriceData(ticker, interval, period, startDate, endDate);
            if (apiData) {
                console.log(LOG_PREFIX, `Cache HIT for ${ticker} ${interval}. Data points: ${apiData.length}`);
            } else {
                console.log(LOG_PREFIX, `Cache MISS for ${ticker} ${interval}. Will fetch from API.`);
            }
        } else {
            console.warn(LOG_PREFIX, "AnalyticsPriceCache module not found. Fetching directly from API.");
        }

        if (apiData) { // If cache hit
            renderTimeseriesChart(apiData, ticker, interval, { period, start: startDate, end: endDate }, chartType);
            showLoadingIndicator(false);
            if (priceHistoryRunButton) priceHistoryRunButton.disabled = false;
        } else { // Cache miss or cache module not available, fetch from API
            let apiUrl = '/api/v3/timeseries/price_history';
            const params = new URLSearchParams();
            params.append('ticker', ticker);
            params.append('interval', interval);

            if (period === 'custom' && startDate && endDate) {
                params.append('start_date', startDate);
                params.append('end_date', endDate);
            } else if (period !== 'custom') {
                params.append('period', period);
                // For non-custom periods, API infers start/end. Do not send empty date strings.
            }
            apiUrl += `?${params.toString()}`;
            console.log(LOG_PREFIX, "Fetching Price History from API:", apiUrl);

            try {
                const response = await fetch(apiUrl);
                if (!response.ok) {
                    const errorData = await response.json().catch(() => ({ detail: "Unknown error during price history fetch." }));
                    console.error(LOG_PREFIX, "API Error:", response.status, errorData);
                    throw new Error(errorData.detail || `HTTP error ${response.status}`);
                }
                apiData = await response.json();
                console.log(LOG_PREFIX, "API Response Data:", apiData ? apiData.length : 0, "points");

                if (apiData && apiData.length > 0) {
                    // MODIFICATION: Store fetched data in cache
                    if (window.AnalyticsPriceCache) {
                        console.log(LOG_PREFIX, `Storing ${ticker} ${interval} (Period: '${period}', Start: '${startDate}', End: '${endDate}') in cache. Points: ${apiData.length}`);
                        window.AnalyticsPriceCache.storePriceData(ticker, interval, apiData, period, startDate, endDate);
                    }
                    renderTimeseriesChart(apiData, ticker, interval, { period, start: startDate, end: endDate }, chartType);
                } else {
                    showPlaceholderWithMessage("No price data returned for the selected criteria.");
                }
            } catch (error) {
                console.error(LOG_PREFIX, "Error fetching price history:", error);
                showPlaceholderWithMessage(`Error: ${error.message}`);
            } finally {
                showLoadingIndicator(false);
                if (priceHistoryRunButton) priceHistoryRunButton.disabled = false;
            }
        } // END Cache miss logic
    }

    // --- NEW: Handler for Price Performance Comparison Study ---
    async function handleRunPricePerformanceComparison() {
        console.log(LOG_PREFIX, "handleRunPricePerformanceComparison called.");

        let loadedTickers = [];
        if (ppcLoadedTickerSelect) {
            loadedTickers = Array.from(ppcLoadedTickerSelect.selectedOptions).map(option => option.value.trim().toUpperCase()).filter(t => t);
        } else {
            console.warn(LOG_PREFIX, "PPC Loaded ticker select not found.");
        }

        let manualTickers = [];
        if (ppcManualTickerTextarea) {
            const manualTickersStr = ppcManualTickerTextarea.value.trim();
            if (manualTickersStr) {
                manualTickers = manualTickersStr.split(',').map(t => t.trim().toUpperCase()).filter(t => t);
            }
        } else {
            console.warn(LOG_PREFIX, "PPC Manual ticker textarea not found.");
        }

        const combinedTickers = [...new Set([...loadedTickers, ...manualTickers])];

        if (combinedTickers.length === 0) {
            alert("Please select at least one ticker from the list or enter tickers manually for comparison.");
            return;
        }

        const selectedTickers = combinedTickers;
        console.log(LOG_PREFIX, "Combined and deduplicated tickers for PPC:", selectedTickers);

        const interval = ppcIntervalSelect ? ppcIntervalSelect.value : null;
        const selectedPeriod = ppcPeriodSelector ? ppcPeriodSelector.value : null;
        
        if (!interval || !selectedPeriod) {
            alert("Interval or Period selector not found or value missing for PPC study."); return;
        }

        let userStartDateStr = null; // User's direct input for custom start
        let userEndDateStr = null;   // User's direct input for custom end
        
        // Parameters for API query construction
        let apiQueryStartDate = null;
        let apiQueryEndDate = null;
        let apiQueryPeriod = null;

        if (selectedPeriod === 'custom') {
            if (!ppcStartDateInput || !ppcEndDateInput) {
                alert("Date input components are missing for PPC custom range."); return;
            }
            userStartDateStr = ppcStartDateInput.value;
            userEndDateStr = ppcEndDateInput.value;

            if (!userStartDateStr || !userEndDateStr) {
                alert("Please select a start and end date for PPC custom range."); return;
            }
            if (new Date(userStartDateStr) >= new Date(userEndDateStr)) {
                alert("Start date must be before end date for custom range."); return;
            }
            
            apiQueryStartDate = userStartDateStr;
            // API expects end_date to be inclusive, or rather, data up to the start of that day.
            // If user selects 2023-12-31, they want data for that day.
            // The existing logic adds 1 day for API, which is fine for API.
            // For cache parameters, we'll use userStartDateStr and userEndDateStr directly.
            const apiEndDateObj = new Date(userEndDateStr); // Use user's end date
            apiEndDateObj.setDate(apiEndDateObj.getDate() + 1); // Add 1 day for API query to include user's end date
            apiQueryEndDate = apiEndDateObj.toISOString().split('T')[0];

        } else {
            apiQueryPeriod = selectedPeriod;
            // userStartDateStr and userEndDateStr remain null if not 'custom'
        }

        console.log(LOG_PREFIX, "PPC Run with Tickers:", selectedTickers, 
                        "Interval:", interval, 
                        "Selected Period:", selectedPeriod,
                        "User Custom Dates:", {start: userStartDateStr, end: userEndDateStr},
                        "API Query Dates/Period:", {start: apiQueryStartDate, end: apiQueryEndDate, period: apiQueryPeriod });
        
        showLoadingIndicator(true);
        if (ppcRunButton) ppcRunButton.disabled = true;
        showPlaceholderWithMessage('Fetching and processing data for comparison...');

        const dataFetchPromises = selectedTickers.map(async (ticker) => {
            let cachedData = null;
            // Determine parameters for cache lookup and storage
            const cacheLookupPeriod = selectedPeriod; // This is 'custom' or 'ytd', '1mo' etc.
            const cacheLookupStartDate = userStartDateStr; // This is the user's specified start for 'custom'
            const cacheLookupEndDate = userEndDateStr;   // This is the user's specified end for 'custom'

            if (window.AnalyticsPriceCache) {
                console.log(LOG_PREFIX, `[PPC] Attempting to fetch ${ticker} ${interval} (Period: '${cacheLookupPeriod}', Start: '${cacheLookupStartDate}', End: '${cacheLookupEndDate}') from cache.`);
                cachedData = window.AnalyticsPriceCache.getPriceData(ticker, interval, cacheLookupPeriod, cacheLookupStartDate, cacheLookupEndDate);
                if (cachedData) {
                    console.log(LOG_PREFIX, `[PPC] Cache HIT for ${ticker} ${interval}. Data points: ${cachedData.length}`);
                    return { ticker, data: cachedData };
                } else {
                    console.log(LOG_PREFIX, `[PPC] Cache MISS for ${ticker} ${interval}. Will fetch from API.`);
                }
            } else {
                console.warn(LOG_PREFIX, "[PPC] AnalyticsPriceCache module not found. Fetching directly from API.");
            }

            // Cache miss or module not found, proceed to fetch from API
            const params = new URLSearchParams();
            params.append('ticker', ticker);
            params.append('interval', interval);

            if (apiQueryPeriod) { // For non-custom periods like 'ytd', '1mo'
                params.append('period', apiQueryPeriod);
            } else if (apiQueryStartDate && apiQueryEndDate) { // For 'custom' period
                params.append('start_date', apiQueryStartDate);
                params.append('end_date', apiQueryEndDate);
            }
            
            const apiUrl = `/api/v3/timeseries/price_history?${params.toString()}`;
            console.log(LOG_PREFIX, `[PPC] Fetching for ${ticker} from API: ${apiUrl}`);

            try {
                const response = await fetch(apiUrl);
                if (!response.ok) {
                    const errorData = await response.json().catch(() => ({}));
                    throw new Error(`HTTP error ${response.status} for ${ticker}: ${errorData.detail || response.statusText}`);
                }
                const apiData = await response.json();
                console.log(LOG_PREFIX, `[PPC] API Response for ${ticker}: ${apiData ? apiData.length : 0} points`);

                if (apiData && apiData.length > 0) {
                    if (window.AnalyticsPriceCache) {
                        console.log(LOG_PREFIX, `[PPC] Storing ${ticker} ${interval} (Period: '${cacheLookupPeriod}', Start: '${cacheLookupStartDate}', End: '${cacheLookupEndDate}') in cache. Points: ${apiData.length}`);
                        // Use cacheLookupPeriod, cacheLookupStartDate, cacheLookupEndDate for storing
                        window.AnalyticsPriceCache.storePriceData(ticker, interval, apiData, cacheLookupPeriod, cacheLookupStartDate, cacheLookupEndDate);
                    }
                    return { ticker, data: apiData };
                } else {
                    // Return error structure if no data, to be consistent with fetch failure
                    return { ticker, error: "No data returned or empty dataset from API." };
                }
            } catch (error) {
                console.error(LOG_PREFIX, `[PPC] Error fetching data for ${ticker}:`, error);
                return { ticker, error: error.message || "Failed to fetch due to network or other error" };
            }
        });

        Promise.allSettled(dataFetchPromises)
            .then(results => {
                showLoadingIndicator(false);
                if (ppcRunButton) ppcRunButton.disabled = false;
                const successfullyFetchedData = [];
                const errors = [];

                results.forEach(result => {
                    if (result.status === 'fulfilled') {
                        if (result.value.error) { 
                            errors.push(`${result.value.ticker}: ${result.value.error}`);
                        } else if (result.value.data && result.value.data.length > 0) {
                            successfullyFetchedData.push(result.value);
                        } else { 
                            errors.push(`${result.value.ticker}: No data after fetch (unexpected state).`);
                        }
                    } else { 
                        errors.push(`A ticker (unknown): Request promise rejected - ${result.reason?.message || 'Unknown critical error'}`); 
                    }
                });

                if (errors.length > 0) {
                    // Consider a more user-friendly way to display multiple errors if this becomes common
                    alert("Errors occurred during data fetching for comparison (some tickers might be missing):\n" + errors.join("\n"));
                }

                if (successfullyFetchedData.length === 0) {
                    showPlaceholderWithMessage("No data successfully obtained for any selected tickers for comparison.");
                    return;
                }

                // Normalize data (this part remains the same)
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

            }).catch(overallError => { // Catch for Promise.allSettled itself, though unlikely with individual catches
                showLoadingIndicator(false);
                if (ppcRunButton) ppcRunButton.disabled = false;
                console.error(LOG_PREFIX, "Critical error in Promise.allSettled for PPC:", overallError);
                showPlaceholderWithMessage("An unexpected critical error occurred while processing comparison data.");
            });
    }

    // --- NEW: Handler for Pair Relative Price Study (PRP) ---
    async function handleRunPairRelativePrice() {
        console.log(LOG_PREFIX, "handleRunPairRelativePrice called.");

        if (!prpTicker1ManualInput || !prpTicker1LoadedSelect || 
            !prpTicker2ManualInput || !prpTicker2LoadedSelect || 
            !prpPeriodSelector || !prpIntervalSelect) {
            console.error(LOG_PREFIX, "Essential UI elements for Pair Relative Price not found!");
            alert("Error: Essential UI components for Pair Relative Price are missing.");
            return;
        }

        let ticker1 = '';
        const ticker1Source = document.querySelector('input[name="tsPrpTicker1Source"]:checked');
        if (ticker1Source) {
            if (ticker1Source.value === 'loaded') {
                ticker1 = prpTicker1LoadedSelect.value;
            } else {
                ticker1 = prpTicker1ManualInput.value.trim().toUpperCase();
            }
        } else {
            alert("Please select a source for Ticker 1."); return;
        }

        let ticker2 = '';
        const ticker2Source = document.querySelector('input[name="tsPrpTicker2Source"]:checked');
        if (ticker2Source) {
            if (ticker2Source.value === 'loaded') {
                ticker2 = prpTicker2LoadedSelect.value;
            } else {
                ticker2 = prpTicker2ManualInput.value.trim().toUpperCase();
            }
        } else {
            alert("Please select a source for Ticker 2."); return;
        }

        if (!ticker1 || !ticker2) {
            alert("Please enter both Ticker 1 and Ticker 2 symbols.");
            return;
        }
        if (ticker1 === ticker2) {
            alert("Ticker 1 and Ticker 2 cannot be the same.");
            return;
        }

        const interval = prpIntervalSelect.value;
        const selectedPeriod = prpPeriodSelector.value;

        if (!interval || !selectedPeriod) {
            alert("Interval or Period selector not found or value missing for PRP study."); return;
        }

        let userStartDateStr = null; // User's direct input for custom start
        let userEndDateStr = null;   // User's direct input for custom end
        
        // Parameters for API query construction (and cache lookup)
        let apiQueryStartDate = null;
        let apiQueryEndDate = null;
        let apiQueryPeriod = null;
        let rangeDetails; // For chart title and cache parameters

        if (selectedPeriod === 'custom') {
            if (!prpStartDateInput || !prpEndDateInput) {
                alert("Date input components are missing for PRP custom range."); return;
            }
            userStartDateStr = prpStartDateInput.value;
            userEndDateStr = prpEndDateInput.value;
            if (!userStartDateStr || !userEndDateStr) {
                alert("Please select a start and end date for PRP custom range."); return;
            }
            const startDateObj = new Date(userStartDateStr);
            const endDateObj = new Date(userEndDateStr);
            if (startDateObj >= endDateObj) { // Corrected: Should be >= for empty or invalid range
                alert("Start date must be strictly before end date."); return;
            }
            
            apiQueryStartDate = userStartDateStr;
            // API expects end_date to be inclusive for the day.
            // If user selects 2023-12-31, they want data up to and including that day.
            // The API /price_history endpoint interprets end_date as exclusive if it's just a date (data up to start of that day).
            // So, to include the user's selected end_date, we typically add 1 day for the API query.
            // However, for cache consistency and user expectation, we'll use userStartDateStr and userEndDateStr directly for cache.
            const apiEndDateObjForQuery = new Date(userEndDateStr);
            apiEndDateObjForQuery.setDate(apiEndDateObjForQuery.getDate() + 1); // Add 1 day for API query to include the user's end date
            apiQueryEndDate = apiEndDateObjForQuery.toISOString().split('T')[0];

            rangeDetails = { start: userStartDateStr, end: userEndDateStr }; // Use user-provided dates for cache and title
            // apiQueryPeriod remains null for custom
        } else {
            apiQueryPeriod = selectedPeriod; // e.g., 'ytd', '1mo'
            // userStartDateStr and userEndDateStr remain null for non-custom
            rangeDetails = { period: selectedPeriod }; // Use period for cache and title
            // apiQueryStartDate and apiQueryEndDate remain null
        }

        console.log(LOG_PREFIX, `PRP Run for ${ticker1}/${ticker2}, Interval: ${interval}, Selected Period: ${selectedPeriod}, User Dates: ${userStartDateStr}-${userEndDateStr}, API Query: period=${apiQueryPeriod}, start=${apiQueryStartDate}, end=${apiQueryEndDate}`);
        showLoadingIndicator(true);
        if (prpRunButton) prpRunButton.disabled = true;
        showPlaceholderWithMessage(`Fetching data for ${ticker1} and ${ticker2}...`);

        const tickersToFetch = [ticker1, ticker2];
        
        // Use cacheLookupPeriod, cacheLookupStartDate, cacheLookupEndDate for cache operations.
        // These will be 'selectedPeriod' (e.g. 'ytd') OR userStartDateStr/userEndDateStr for 'custom'.
        const cacheLookupPeriod = selectedPeriod;
        const cacheLookupStartDate = userStartDateStr; // Null if not 'custom'
        const cacheLookupEndDate = userEndDateStr;     // Null if not 'custom'

        const dataFetchPromises = tickersToFetch.map(async (ticker) => {
            let cachedData = null;
            if (window.AnalyticsPriceCache) {
                console.log(LOG_PREFIX, `[PRP] Attempting to fetch ${ticker} ${interval} (Period: '${cacheLookupPeriod}', Start: '${cacheLookupStartDate}', End: '${cacheLookupEndDate}') from cache.`);
                cachedData = window.AnalyticsPriceCache.getPriceData(ticker, interval, cacheLookupPeriod, cacheLookupStartDate, cacheLookupEndDate);
                if (cachedData) {
                    console.log(LOG_PREFIX, `[PRP] Cache HIT for ${ticker} ${interval}. Data points: ${cachedData.length}`);
                    return { ticker, data: cachedData };
                } else {
                    console.log(LOG_PREFIX, `[PRP] Cache MISS for ${ticker} ${interval}. Will fetch from API.`);
                }
            } else {
                console.warn(LOG_PREFIX, "[PRP] AnalyticsPriceCache module not found. Fetching directly from API.");
            }

            // Cache miss or module not found, proceed to fetch from API
            const params = new URLSearchParams();
            params.append('ticker', ticker);
            params.append('interval', interval);

            if (apiQueryPeriod) { // For non-custom periods like 'ytd', '1mo'
                params.append('period', apiQueryPeriod);
            } else if (apiQueryStartDate && apiQueryEndDate) { // For 'custom' period
                params.append('start_date', apiQueryStartDate);
                params.append('end_date', apiQueryEndDate); // Use the API-adjusted end date
            }
            
            const apiUrl = `/api/v3/timeseries/price_history?${params.toString()}`;
            console.log(LOG_PREFIX, `[PRP] Fetching for ${ticker} from API: ${apiUrl}`);

            try {
                const response = await fetch(apiUrl);
                if (!response.ok) {
                    const errorData = await response.json().catch(() => ({}));
                    throw new Error(`HTTP error ${response.status} for ${ticker}: ${errorData.detail || response.statusText}`);
                }
                const apiData = await response.json();
                console.log(LOG_PREFIX, `[PRP] API Response for ${ticker}: ${apiData ? apiData.length : 0} points`);

                if (apiData && apiData.length > 0) {
                    if (window.AnalyticsPriceCache) {
                        console.log(LOG_PREFIX, `[PRP] Storing ${ticker} ${interval} (Period: '${cacheLookupPeriod}', Start: '${cacheLookupStartDate}', End: '${cacheLookupEndDate}') in cache. Points: ${apiData.length}`);
                        window.AnalyticsPriceCache.storePriceData(ticker, interval, apiData, cacheLookupPeriod, cacheLookupStartDate, cacheLookupEndDate);
                    }
                    return { ticker, data: apiData };
                } else {
                    return { ticker, error: "No data returned or empty dataset from API." };
                }
            } catch (error) {
                console.error(LOG_PREFIX, `[PRP] Error fetching data for ${ticker}:`, error);
                return { ticker, error: error.message || "Failed to fetch due to network or other error" };
            }
        });


        Promise.allSettled(dataFetchPromises)
            .then(results => {
                showLoadingIndicator(false);
                if (prpRunButton) prpRunButton.disabled = false;
                const fetchedDataMap = new Map();
                const errors = [];

                results.forEach(result => {
                    if (result.status === 'fulfilled') {
                        if (result.value.error) {
                            errors.push(`${result.value.ticker}: ${result.value.error}`);
                        } else if (result.value.data && result.value.data.length > 0) {
                            fetchedDataMap.set(result.value.ticker, result.value.data);
                        } else {
                            errors.push(`${result.value.ticker}: No data returned or empty dataset (after fetch/cache).`);
                        }
                    } else { // status === 'rejected'
                        // This path should ideally not be hit if individual fetches handle their errors and return a structured error object.
                        // But as a fallback:
                        errors.push(`A ticker request failed unexpectedly: ${result.reason?.message || 'Unknown critical fetch error'}`);
                    }
                });

                if (errors.length > 0) {
                    alert("Errors occurred during data fetching for relative price:\n" + errors.join("\n"));
                }

                if (fetchedDataMap.size !== 2) {
                    showPlaceholderWithMessage("Failed to fetch data for one or both tickers. Cannot calculate relative price.");
                    return;
                }

                const data1 = fetchedDataMap.get(ticker1);
                const data2 = fetchedDataMap.get(ticker2);

                // Align data and calculate ratio
                const alignedRatios = [];
                const data2Map = new Map(data2.map(item => [ (item.Datetime || item.Date), item.Close ]));

                for (const p1 of data1) {
                    const dateKey = p1.Datetime || p1.Date;
                    const close1 = p1.Close;
                    const close2 = data2Map.get(dateKey);

                    if (typeof close1 === 'number' && typeof close2 === 'number' && close2 !== 0) {
                        alignedRatios.push({
                            x: new Date(dateKey).valueOf(),
                            y: close1 / close2
                        });
                    } else if (typeof close1 === 'number' && typeof close2 === 'number' && close2 === 0) {
                         console.warn(LOG_PREFIX, `Cannot calculate ratio for ${dateKey}: Ticker 2 price is 0.`);
                    }
                }

                if (alignedRatios.length === 0) {
                    showPlaceholderWithMessage(`No common data points found or Ticker 2 price was zero for ${ticker1}/${ticker2}. Cannot calculate relative price.`);
                    return;
                }

                console.log(LOG_PREFIX, "Calculated relative price ratios:", alignedRatios.length);
                // Pass the originally determined rangeDetails (based on user input 'custom' or selectedPeriod) to the chart title
                renderTimeseriesChart(alignedRatios, `${ticker1}/${ticker2}`, interval, rangeDetails, 'pair_relative_price_line');

            }).catch(overallError => { // Catch for Promise.allSettled itself, though unlikely with robust individual catches
                showLoadingIndicator(false);
                if (prpRunButton) prpRunButton.disabled = false;
                console.error(LOG_PREFIX, "Critical error in Promise.allSettled for PRP:", overallError);
                showPlaceholderWithMessage("An unexpected critical error occurred while processing relative price data.");
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
            } else if (selectedStudy === 'pair_relative_price') { // NEW: Handle PRP pane
                if (prpPeriodSelector && prpStartDateContainer && prpEndDateContainer) {
                    const isCustom = prpPeriodSelector.value === 'custom';
                    prpStartDateContainer.style.display = isCustom ? 'block' : 'none';
                    prpEndDateContainer.style.display = isCustom ? 'block' : 'none';
                }
            } else if (selectedStudy === 'price_fundamental_ratios') { // NEW: Handle PFR pane
                 if (pfrPeriodSelector && pfrStartDateContainer && pfrEndDateContainer) {
                    const isCustom = pfrPeriodSelector.value === 'custom';
                    pfrStartDateContainer.style.display = isCustom ? 'block' : 'none';
                    pfrEndDateContainer.style.display = isCustom ? 'block' : 'none';
                }
            } else if (selectedStudy === 'synthetic_fundamentals') { // NEW: Handle SF pane date visibility
                // For Synthetic Fundamentals, date pickers are always visible as they are optional (default to YTD)
                // No specific period selector that hides/shows them, but ensure they are visible if the pane is active.
                const sfStartDateContainer = document.getElementById('ts-sf-start-date'); // Assuming the input itself is the container for visibility purposes
                const sfEndDateContainer = document.getElementById('ts-sf-end-date');
                // These are direct input elements, not containers like others. Their visibility is part of the pane.
                // No explicit show/hide needed here beyond the pane itself becoming visible.
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
        // Check if crucial select elements exist for logging, but proceed regardless for partial UI functionality
        if (!priceHistoryLoadedTickerSelect) console.warn(LOG_PREFIX, "Price History loaded ticker select element not found at init.");
        if (!ppcLoadedTickerSelect) console.warn(LOG_PREFIX, "PPC loaded ticker select element not found at init.");
        if (!prpTicker1LoadedSelect) console.warn(LOG_PREFIX, "PRP Ticker 1 loaded select element not found at init.");
        if (!prpTicker2LoadedSelect) console.warn(LOG_PREFIX, "PRP Ticker 2 loaded select element not found at init.");

        let analyticsOriginalData;
        try {
            analyticsOriginalData = getFinalAnalyticsData();
        } catch (e) {
            console.error(LOG_PREFIX, "Error calling getFinalAnalyticsData:", e);
            analyticsOriginalData = []; // Default to empty on error to prevent further crashes
        }
        
        // Safer logging for the received data package or its length
        if (analyticsOriginalData && typeof analyticsOriginalData.length === 'number') {
        console.log(LOG_PREFIX, "populateLoadedTickerSelect - analyticsOriginalData count:", analyticsOriginalData.length);
        } else {
            console.log(LOG_PREFIX, "populateLoadedTickerSelect - analyticsOriginalData is not an array or has no length. Data received:", analyticsOriginalData);
        }
        
        const uniqueTickers = new Set();
        if (Array.isArray(analyticsOriginalData)) {
            analyticsOriginalData.forEach((item, index) => {
                if (item && item.ticker) {
                    uniqueTickers.add(item.ticker);
                } else {
                    // console.debug(LOG_PREFIX, `populateLoadedTickerSelect - Item at index ${index} is missing ticker. Item:`, item);
                }
            });
        } else {
            console.warn(LOG_PREFIX, "populateLoadedTickerSelect - analyticsOriginalData is not an array (or is null/undefined). Cannot extract tickers. Data value:", analyticsOriginalData);
        }

        const sortedTickers = Array.from(uniqueTickers).sort();
        if (sortedTickers.length === 0) {
            console.warn(LOG_PREFIX, "populateLoadedTickerSelect - No unique tickers found after processing analyticsOriginalData. Dropdowns will indicate no data.");
        }

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
                const noneOption = document.createElement('option');
                noneOption.value = ""; // Empty value will be filtered out by current logic
                noneOption.textContent = "-- None (ignore list below) --";
                ppcLoadedTickerSelect.appendChild(noneOption);

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
                noTickerOption.disabled = true; 
                ppcLoadedTickerSelect.appendChild(noTickerOption);
            }
            console.log(LOG_PREFIX, "populateLoadedTickerSelect - Populated PPC multi-select with tickers:", sortedTickers);
        }

        // --- NEW: Populate Pair Relative Price Ticker 1 select ---
        if (prpTicker1LoadedSelect) {
            prpTicker1LoadedSelect.innerHTML = ''; // Clear existing options
            if (sortedTickers.length > 0) {
                const placeholderOption = document.createElement('option');
                placeholderOption.value = "";
                placeholderOption.textContent = "Select Ticker 1...";
                placeholderOption.disabled = true;
                placeholderOption.selected = true;
                prpTicker1LoadedSelect.appendChild(placeholderOption);
                sortedTickers.forEach(ticker => {
                    const option = document.createElement('option');
                    option.value = ticker;
                    option.textContent = ticker;
                    prpTicker1LoadedSelect.appendChild(option);
                });
            } else {
                const noTickerOption = document.createElement('option');
                noTickerOption.value = "";
                noTickerOption.textContent = "No loaded tickers";
                prpTicker1LoadedSelect.appendChild(noTickerOption);
            }
            console.log(LOG_PREFIX, "populateLoadedTickerSelect - Populated PRP Ticker 1 select.");
        }

        // --- NEW: Populate Pair Relative Price Ticker 2 select ---
        if (prpTicker2LoadedSelect) {
            prpTicker2LoadedSelect.innerHTML = ''; // Clear existing options
            if (sortedTickers.length > 0) {
                const placeholderOption = document.createElement('option');
                placeholderOption.value = "";
                placeholderOption.textContent = "Select Ticker 2...";
                placeholderOption.disabled = true;
                placeholderOption.selected = true;
                prpTicker2LoadedSelect.appendChild(placeholderOption);
                sortedTickers.forEach(ticker => {
                    const option = document.createElement('option');
                    option.value = ticker;
                    option.textContent = ticker;
                    prpTicker2LoadedSelect.appendChild(option);
                });
            } else {
                const noTickerOption = document.createElement('option');
                noTickerOption.value = "";
                noTickerOption.textContent = "No loaded tickers";
                prpTicker2LoadedSelect.appendChild(noTickerOption);
            }
            console.log(LOG_PREFIX, "populateLoadedTickerSelect - Populated PRP Ticker 2 select.");
        }

        // NEW: Get reference to the PFC ticker select
        const pfcTickerSelect = document.getElementById('ts-pfc-ticker-select');
        if (!pfcTickerSelect) console.warn(LOG_PREFIX, "PFC ticker select (ts-pfc-ticker-select) not found at init.");

        // let analyticsOriginalData; // REMOVE REDECLARATION
        try {
            analyticsOriginalData = getFinalAnalyticsData();
        } catch (e) {
            console.error(LOG_PREFIX, "Error calling getFinalAnalyticsData:", e);
            analyticsOriginalData = []; // Default to empty on error to prevent further crashes
        }
        
        // Safer logging for the received data package or its length
        if (analyticsOriginalData && typeof analyticsOriginalData.length === 'number') {
        console.log(LOG_PREFIX, "populateLoadedTickerSelect - analyticsOriginalData count:", analyticsOriginalData.length);
        } else {
            console.log(LOG_PREFIX, "populateLoadedTickerSelect - analyticsOriginalData is not an array or has no length. Data received:", analyticsOriginalData);
        }
        
        // const uniqueTickers = new Set(); // REMOVE REDECLARATION
        if (Array.isArray(analyticsOriginalData)) {
            analyticsOriginalData.forEach((item, index) => {
                if (item && item.ticker) {
                    uniqueTickers.add(item.ticker);
                } else {
                    // console.debug(LOG_PREFIX, `populateLoadedTickerSelect - Item at index ${index} is missing ticker. Item:`, item);
                }
            });
        } else {
            console.warn(LOG_PREFIX, "populateLoadedTickerSelect - analyticsOriginalData is not an array (or is null/undefined). Cannot extract tickers. Data value:", analyticsOriginalData);
        }

        // const sortedTickers = Array.from(uniqueTickers).sort(); // REMOVE REDECLARATION
        if (sortedTickers.length === 0) {
            console.warn(LOG_PREFIX, "populateLoadedTickerSelect - No unique tickers found after processing analyticsOriginalData. Dropdowns will indicate no data.");
        }

        // NEW: Populate Price-Fundamental Comparison single select
        if (pfcTickerSelect) {
            pfcTickerSelect.innerHTML = ''; // Clear existing options
            if (sortedTickers.length > 0) {
                const placeholderOption = document.createElement('option');
                placeholderOption.value = "";
                placeholderOption.textContent = "Select a Ticker...";
                placeholderOption.disabled = true;
                placeholderOption.selected = true;
                pfcTickerSelect.appendChild(placeholderOption);
                sortedTickers.forEach(ticker => {
                    const option = document.createElement('option');
                    option.value = ticker;
                    option.textContent = ticker;
                    pfcTickerSelect.appendChild(option);
                });
            } else {
                const noTickerOption = document.createElement('option');
                noTickerOption.value = "";
                noTickerOption.textContent = "No tickers in loaded data";
                pfcTickerSelect.appendChild(noTickerOption);
            }
            console.log(LOG_PREFIX, "populateLoadedTickerSelect - Populated PFC select with tickers:", sortedTickers);
        }

        // NEW: Populate Price-Fundamental Ratios multi-select for tickers
        if (pfrTickerSelect) {
            pfrTickerSelect.innerHTML = ''; // Clear existing options
            if (sortedTickers.length > 0) {
                // No placeholder like "Select Ticker..." for multi-select, users just select
                sortedTickers.forEach(ticker => {
                    const option = document.createElement('option');
                    option.value = ticker;
                    option.textContent = ticker;
                    pfrTickerSelect.appendChild(option);
                });
            } else {
                const noTickerOption = document.createElement('option');
                noTickerOption.value = "";
                noTickerOption.textContent = "No tickers in loaded data";
                noTickerOption.disabled = true; // Disable if no tickers
                pfrTickerSelect.appendChild(noTickerOption);
            }
            console.log(LOG_PREFIX, "populateLoadedTickerSelect - Populated PFR multi-select with tickers:", sortedTickers);
             // Initialize Bootstrap Multiselect if available for PFR tickers
            if (typeof $(pfrTickerSelect).multiselect === 'function') {
                if (!$(pfrTickerSelect).data('multiselect')) {
                    $(pfrTickerSelect).multiselect({
                        buttonWidth: '100%',
                        enableFiltering: true,
                        enableCaseInsensitiveFiltering: true,
                        maxHeight: 200,
                        includeSelectAllOption: true,
                        nonSelectedText: 'Select Ticker(s)',
                        numberDisplayed: 1,
                        nSelectedText: ' tickers selected',
                        allSelectedText: 'All tickers selected'
                    });
                } else {
                    $(pfrTickerSelect).multiselect('rebuild'); // Rebuild if already initialized
                }
            }
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

        if (timeseriesChartInstance) {
            timeseriesChartInstance.destroy();
            timeseriesChartInstance = null;
        }
        if (tsResetZoomBtn) tsResetZoomBtn.style.display = 'none'; 

        if (!apiData || (chartType !== 'performance_comparison_line' && chartType !== 'pair_relative_price_line' && apiData.length === 0)) {
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

            // Determine theme for plugin text color
            const isDarkMode = document.documentElement.getAttribute('data-bs-theme') === 'dark';
            const indicatorTextColor = isDarkMode ? '#E0E0E0' : '#333333';
            // const pillBackgroundColor = isDarkMode ? 'rgba(70, 70, 70, 0.7)' : 'rgba(230, 230, 230, 0.7)'; // REMOVED: Plugin will derive from dataset

            let chartOptions = { 
                responsive: true,
                maintainAspectRatio: false,
                layout: { // NEW: Add padding to the right for the indicator
                    padding: {
                        right: 60 // Increased to 60px to ensure space for text and pill padding
                    }
                },
                plugins: {
                    lastValueIndicator: { 
                        appChartType: chartType,
                        textColor: indicatorTextColor, 
                        // pillBackgroundColor: pillBackgroundColor, // REMOVED: Let plugin use dataset.borderColor
                        font: 'bold 11px Arial',         
                        xPadding: 11, // UPDATED: 5px gap + 6px pillHPadding = 11px
                        pillHPadding: 6,                 
                        pillVPadding: 3,                 
                        pillBorderRadius: 4              
                    },
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
                if (!window.Chart || !window.Chart.controllers || !(window.Chart.controllers.candlestick || window.Chart.controllers.ohlc)) {
                    const errorMsg = `${chartType === 'candlestick' ? 'Candlestick' : 'OHLC'} chart components not available.`;
                    console.error(LOG_PREFIX, errorMsg);
                    showPlaceholderWithMessage(errorMsg); return;
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
            } else if (chartType === 'performance_comparison_line') {
                console.log(LOG_PREFIX, "Configuring for performance_comparison_line chart type.");
                chartJsType = 'line';
                specificTitlePart = "Price Performance Comparison";

                const lineColors = [
                    'rgb(75, 192, 192)', 'rgb(255, 99, 132)', 'rgb(54, 162, 235)',
                    'rgb(255, 206, 86)', 'rgb(153, 102, 255)', 'rgb(255, 159, 64)',
                    'rgb(199, 199, 199)', 'rgb(83, 102, 255)', 'rgb(100, 255, 100)'
                ];

                datasets = apiData.map((series, index) => ({
                    label: series.ticker + " Performance",
                    data: series.data,
                    borderColor: lineColors[index % lineColors.length],
                    backgroundColor: lineColors[index % lineColors.length].replace('rgb', 'rgba').replace(')', ',0.1)'),
                    borderWidth: 1, // MODIFIED: Changed from 2 to 1
                    fill: false,
                    pointRadius: 0,
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

            } else if (chartType === 'pair_relative_price_line') {
                console.log(LOG_PREFIX, "Configuring for pair_relative_price_line chart type.");
                chartJsType = 'line';
                specificTitlePart = `Relative Price: ${ticker}`;

                datasets = [{
                    label: `Ratio (${ticker})`,
                    data: apiData,
                    borderColor: 'rgb(255, 159, 64)', 
                    backgroundColor: 'rgba(255, 159, 64, 0.1)',
                    borderWidth: 1,
                    fill: false,
                    pointRadius: 0,
                    tension: 0.1
                }];

                chartOptions.scales.x = {
                    type: 'time',
                    time: {
                        tooltipFormat: 'MMM d, yyyy' + (['15m', '30m', '1h'].includes(interval) ? ', HH:mm' : '')
                    },
                    title: { display: true, text: 'Date' + (['15m', '30m', '1h'].includes(interval) ? '/Time' : '') },
                    ticks: { source: 'auto', maxRotation: 45, minRotation: 0 }
                };
                chartOptions.scales.y = {
                    title: { display: true, text: 'Price Ratio' }, 
                };

                chartOptions.plugins.tooltip.callbacks = {
                    label: function(tooltipItem) {
                        let label = tooltipItem.dataset.label || '';
                        if (label) {
                            label += ': ';
                        }
                        if (tooltipItem.parsed.y !== null) {
                            label += tooltipItem.parsed.y.toFixed(4); 
                        }
                        return label;
                    }
                };
            } else {
                const isIntraday = ['15m', '30m', '1h'].includes(interval);

                if (isIntraday) {
                    chartOptions.scales.x = {
                        type: 'time',
                        time: {
                            tooltipFormat: 'MMM d, yyyy, HH:mm'
                        },
                        title: { display: true, text: 'Date/Time' }, 
                        ticks: { source: 'auto', maxRotation: 45, minRotation: 0 }
                    };
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
                    const labels = apiData.map(d => new Date(d.Datetime || d.Date).toLocaleDateString());
                    chartOptions.scales.x = {
                        type: 'category',
                        labels: labels,
                        title: { display: true, text: 'Date' },
                        ticks: { maxRotation: 45, minRotation: 0 }
                    };
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
                text: `${ (chartType === 'performance_comparison_line' || chartType === 'pair_relative_price_line') ? '' : ticker + " - "}${specificTitlePart} ${titleRangePart ? '(' + titleRangePart + ')' : ''}`.trim(),
                font: { size: 16 }
            };

            timeseriesChartInstance = new Chart(ctx, {
                type: chartJsType,
                data: { datasets: datasets }, 
                options: chartOptions
            });
            console.log(LOG_PREFIX, "Chart rendered successfully for", ticker, "as", chartJsType);
        };

        const isIntradayLine = chartType === 'line' && ['15m', '30m', '1h'].includes(interval);
        
        const needsDateAdapter = 
            chartType === 'candlestick' || 
            chartType === 'ohlc' || 
            chartType === 'performance_comparison_line' || 
            chartType === 'pair_relative_price_line' ||
            isIntradayLine;

        const needsFinancialLib = chartType === 'candlestick' || chartType === 'ohlc';

        if (needsDateAdapter) {
            console.log(LOG_PREFIX, `Chart type ${chartType} (interval: ${interval}) requires date adapter.`);
            showLoadingIndicator(true);
            loadDateAdapterLibrary()
                .then(() => {
                    if (needsFinancialLib) {
                        console.log(LOG_PREFIX, `Chart type ${chartType} also requires financial library.`);
                        return loadFinancialChartLibrary();
                    }
                    return Promise.resolve(); // Financial library not needed
                })
                .then(() => {
                    console.log(LOG_PREFIX, "All required libraries loaded. Proceeding to create chart.");
                    showLoadingIndicator(false);
                    createChartLogic();
                })
                .catch(error => {
                    showLoadingIndicator(false);
                    const libType = needsFinancialLib ? 'financial and/or date adapter' : 'date adapter';
                    const errorMsg = `Failed to load required ${libType} for ${chartType} chart: ${error.message}`;
                    console.error(LOG_PREFIX, errorMsg, error);
                    alert(errorMsg + " Please check the console for details.");
                    showPlaceholderWithMessage(errorMsg);
                    if (tsResetZoomBtn) tsResetZoomBtn.style.display = 'none';
                });
        } else {
            createChartLogic(); // For line chart, no special library needed beyond core Chart.js
        }
    }

    // --- NEW: Generic Chart Rendering Function ---
    /**
     * Renders a generic timeseries chart based on provided datasets and options.
     * @param {Array<Object>} datasets - Array of Chart.js dataset objects.
     * @param {string} chartTitle - The title for the chart.
     * @param {string} yAxisLabel - Label for the Y-axis.
     * @param {Object} options - Additional options for chart configuration.
     * @param {string} options.chartType - 'line' or 'bar'.
     * @param {boolean} [options.isTimeseries=true] - Whether the x-axis is a time series.
     * @param {Array<number|string>} [options.labelsForTimeAxis] - Array of labels (numeric timestamps or date strings) for x-axis if isTimeseries is true.
     * @param {Object} [options.rangeDetails] - Optional details for subtitle (e.g., {period, start, end}).
     * @param {Array<Object>} [options.yAxesConfig] - Optional. Configuration for multiple Y-axes. E.g., [{id: 'yLeft', position: 'left', title: 'Price'}, {id: 'yRight', position: 'right', title: 'Fundamental', grid: {drawOnChartArea: false}}]
     */
    function renderGenericTimeseriesChart(datasets, chartTitle, yAxisLabel, chartOptions = {}) {
        console.log(LOG_PREFIX, "renderGenericTimeseriesChart called. Datasets:", datasets.length, "Title:", chartTitle, "Options:", chartOptions);

        const chartCanvas = document.getElementById('ts-chart-canvas'); 
        const chartPlaceholder = document.getElementById('ts-chart-placeholder');

        if (!chartCanvas || !chartPlaceholder) {
            console.error(LOG_PREFIX, "Chart canvas (ts-chart-canvas) or placeholder (ts-chart-placeholder) not found!");
            return;
        }

        if (timeseriesChartInstance) {
            timeseriesChartInstance.destroy();
            timeseriesChartInstance = null;
        }
        if (tsResetZoomBtn) tsResetZoomBtn.style.display = 'none'; 

        if (!datasets || datasets.length === 0) {
            showPlaceholderWithMessage(`No data available to render chart: ${chartTitle}`);
            return;
        }
        
        const createChartLogic = () => {
            chartCanvas.style.display = 'block';
            chartPlaceholder.style.display = 'none';
            if (tsResetZoomBtn) tsResetZoomBtn.style.display = 'inline-block';

            const ctx = chartCanvas.getContext('2d');
            const isDarkMode = document.documentElement.getAttribute('data-bs-theme') === 'dark';
            const indicatorTextColor = isDarkMode ? '#E0E0E0' : '#333333';

            let finalChartType = chartOptions.chartType || 'line'; // Default to line if not specified

            let commonChartOptions = {
                responsive: true,
                maintainAspectRatio: false,
                layout: {
                    padding: { right: 60 }
                },
                plugins: {
                    lastValueIndicator: { 
                        appChartType: finalChartType, // Use the actual chart type for plugin logic
                        textColor: indicatorTextColor,
                        font: 'bold 11px Arial',         
                        xPadding: 11, 
                        pillHPadding: 6, pillVPadding: 3, pillBorderRadius: 4              
                    },
                    legend: { 
                        position: 'top',
                        display: datasets.length <= 15 // Hide legend if too many datasets
                    },
                    tooltip: {
                        mode: 'index', 
                        intersect: false,
                        callbacks: {
                            title: function(tooltipItems) {
                                // Assuming the first item's label is representative for the x-axis value (date)
                                if (tooltipItems.length > 0) {
                                    const item = tooltipItems[0];
                                    // Ensure item.parsed.x exists and is a number (timestamp)
                                    if (item.parsed && typeof item.parsed.x === 'number') {
                                        const date = new Date(item.parsed.x);
                                        // Format date as desired, e.g., "MMM d, yyyy"
                                        // Using a simple format here, can be expanded with date-fns if needed
                                        return date.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
                                    }
                                    return item.label; // Fallback to default label if x is not a parsable timestamp
                                }
                                return '';
                            },
                            label: function(context) {
                                let label = context.dataset.label || '';
                                if (label) {
                                    label += ': ';
                                }
                                if (context.parsed.y !== null) {
                                    if (Array.isArray(context.raw) && context.raw.length === 2) {
                                        // For floating bars or similar data where raw is [start, end]
                                        label += `${context.raw[0]} - ${context.raw[1]}`;
                                    } else if (typeof context.parsed.y === 'number') {
                                        let decimals = 2;
                                        const isPercentChange = context.dataset.appDataType === 'percent_change';
                                        const isRawOrDefault = context.dataset.appDataType === 'raw' || typeof context.dataset.appDataType === 'undefined';

                                        const isRawAndSmall = isRawOrDefault && 
                                                              Math.abs(context.parsed.y) > 0 && 
                                                              Math.abs(context.parsed.y) < 0.01;
                                        if (isRawAndSmall) {
                                            // Ensure at least 2 significant figures after decimal for small raw numbers
                                            decimals = Math.max(2, -Math.floor(Math.log10(Math.abs(context.parsed.y))) + 1); 
                                        }
                                        label += context.parsed.y.toFixed(decimals);
                                        if (isPercentChange) {
                                            label += '%';
                                        }
                                    } else {
                                        // Fallback for non-numeric y or unexpected type
                                        label += context.parsed.y; 
                                        if (context.dataset.appDataType === 'percent_change') {
                                            label += '%'; // Still attempt to add % if type is percent_change
                                        }
                                    }
                                } else {
                                    label += 'N/A';
                                }
                                return label;
                            }
                        }
                    },
                    zoom: {
                        pan: { enabled: true, mode: 'xy', threshold: 5 },
                        zoom: {
                            wheel: { enabled: true },
                            pinch: { enabled: true },
                            drag: { enabled: true, backgroundColor: 'rgba(0,123,255,0.2)' },
                            mode: 'xy'
                        }
                    }
                },
                scales: {
                    y: {
                        title: { display: true, text: yAxisLabel || 'Value' },
                        beginAtZero: finalChartType === 'bar'
                    }
                }
            };

            // NEW: Handle multiple Y-axes configuration
            if (chartOptions.yAxesConfig && Array.isArray(chartOptions.yAxesConfig) && chartOptions.yAxesConfig.length > 0) {
                commonChartOptions.scales = {}; // Reset scales object to define all axes including X
                chartOptions.yAxesConfig.forEach(axisConf => {
                    commonChartOptions.scales[axisConf.id] = {
                        type: 'linear', // Assuming linear for now, could be configurable
                        display: true,
                        position: axisConf.position,
                        title: { display: true, text: axisConf.title },
                        // Optional grid configuration for the specific Y-axis
                        ...(axisConf.grid && { grid: axisConf.grid }),
                        // Ensure y-axes don't start at zero unless specified or bar chart on that axis
                        beginAtZero: axisConf.beginAtZero === true || (axisConf.associatedChartType === 'bar' && axisConf.beginAtZero !== false)
                    };
                });
            } else {
                // Default single y-axis if no yAxesConfig (current behavior)
                commonChartOptions.scales.y = {
                    title: { display: true, text: yAxisLabel || 'Value' },
                    beginAtZero: finalChartType === 'bar'
                };
            }

            if (chartOptions.isTimeseries !== false) { // Default to true for timeseries behavior
                commonChartOptions.scales.x = { // Ensure X-axis is always added
                    type: 'time',
                    time: {
                        tooltipFormat: 'MMM d, yyyy' // Basic tooltip format, can be customized further
                        // unit: 'day' // Auto-detect unit based on data range
                    },
                    title: { display: true, text: 'Date' },
                    ticks: { source: 'auto', maxRotation: 45, minRotation: 0, autoSkip: true, maxTicksLimit: 20 }
                };
                // If explicit labels are provided for time axis (e.g. numeric timestamps for x values in datasets)
                // Chart.js 'time' scale handles this automatically if data points are {x: timestamp, y: value}
                // No need to set commonChartOptions.data.labels if x values are numeric timestamps for time scale
            } else {
                // For non-timeseries or category-based x-axis
                commonChartOptions.scales.x = { // Ensure X-axis is always added (using 'x' as default ID)
                    type: 'category',
                    labels: chartOptions.labelsForCategoryAxis || [], // Expects labels if not timeseries and using category
                    title: { display: true, text: chartOptions.xAxisLabel || 'Category' },
                    ticks: { maxRotation: 45, minRotation: 0, autoSkip: true, maxTicksLimit: 20 }
                };
            }
            
            let titleRangePart = "";
            if (chartOptions.rangeDetails) {
                if (chartOptions.rangeDetails.period && chartOptions.rangeDetails.period !== 'custom') {
                    titleRangePart = `Period: ${chartOptions.rangeDetails.period.toUpperCase()}`;
                } else if (chartOptions.rangeDetails.start && chartOptions.rangeDetails.end) {
                    try {
                        titleRangePart = `Range: ${new Date(chartOptions.rangeDetails.start).toLocaleDateString()} - ${new Date(chartOptions.rangeDetails.end).toLocaleDateString()}`;
                    } catch (e) { /* ignore date parsing error for title */ }
                }
            }
            
            commonChartOptions.plugins.title = {
                display: true,
                text: `${chartTitle} ${titleRangePart ? '(' + titleRangePart + ')' : ''}`.trim(),
                font: { size: 16 }
            };

            timeseriesChartInstance = new Chart(ctx, {
                type: finalChartType,
                data: { 
                    // If x-axis is 'time', datasets should have {x: timestamp, y: value}
                    // If x-axis is 'category', datasets are arrays of values, and `labels` should be set at data level
                    labels: (chartOptions.isTimeseries === false && chartOptions.labelsForCategoryAxis) ? chartOptions.labelsForCategoryAxis : undefined,
                    datasets: datasets 
                }, 
                options: commonChartOptions
            });
            console.log(LOG_PREFIX, "Generic chart rendered successfully as", finalChartType);
        };

        // Check for library dependencies if needed (e.g., financial charts, date adapter)
        // For now, this generic function assumes Chart.js core and time scale are sufficient.
        // Date adapter is loaded by renderTimeseriesChart if specific types are used.
        // Fundamentals history currently uses line/bar which don't strictly need external adapters beyond Chart.js time scale.
        // createChartLogic(); // OLD direct call

        // NEW: Load date adapter if using a time series axis
        if (chartOptions.isTimeseries !== false) { // Default to true for timeseries behavior
            console.log(LOG_PREFIX, "renderGenericTimeseriesChart: Time series chart, ensuring date adapter is loaded.");
            showLoadingIndicator(true); // Show loading while adapter loads, if not already visible
            loadDateAdapterLibrary()
                .then(() => {
                    console.log(LOG_PREFIX, "renderGenericTimeseriesChart: Date adapter loaded. Proceeding to create chart.");
                    showLoadingIndicator(false);
                    createChartLogic();
                })
                .catch(error => {
                    showLoadingIndicator(false);
                    const errorMsg = `Failed to load date adapter for generic chart: ${error.message}`;
                    console.error(LOG_PREFIX, errorMsg, error);
                    alert(errorMsg + " Please check the console for details.");
                    showPlaceholderWithMessage(errorMsg);
                    if (tsResetZoomBtn) tsResetZoomBtn.style.display = 'none';
                });
        } else {
            // Not a time series chart, no special date adapter needed beyond what Chart.js might bundle.
            console.log(LOG_PREFIX, "renderGenericTimeseriesChart: Not a time series chart, creating directly.");
            createChartLogic();
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
    window.AnalyticsTimeseriesModule = {
        // functionsToExpose
        renderGenericTimeseriesChart, // EXPOSE THE NEW FUNCTION
        showPlaceholderWithMessage, // Expose for fundamentals module to use on error/no data
        loadDateAdapterLibrary,      // Expose for external modules that might need it for custom charting
        loadFinancialChartLibrary  // Expose for external modules
    };

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