document.addEventListener('DOMContentLoaded', function() {
    console.log("AnalyticsDataTableModule: DOMContentLoaded event fired.");

    let finalDataTableInstance = null;
    let lastAppliedHeaders = [];
    let currentHeaders = [];
    let globalSummaryData = {};
    const tableElement = document.getElementById('final-data-table');
    const statusElement = document.getElementById('final-table-status');
    const tableContainer = document.getElementById('final-data-table-container'); // For visibility checks
    const dataTableTabTrigger = document.getElementById('data-table-tab'); // ID of the main tab button
    const groupBySelector = document.getElementById('final-table-group-by-selector');

    // --- Module-scoped variables for popover management --- <<< NEW >>>
    let currentCellPopoverInstance = null;
    let currentPopoverTriggerElement = null;

    // --- Helper: Get necessary functions/data from other modules ---
    function getRequiredModules() {
        const mainModule = window.AnalyticsMainModule;
        const postTransformModule = window.AnalyticsPostTransformModule;
        if (!mainModule || !postTransformModule) {
            console.error("DataTableModule: Required main or post-transform modules not found.");
            return null;
        }
        // <<< Add check for the new getter >>>
        if (typeof postTransformModule.getPostTransformNumericFormats !== 'function') {
            console.error("DataTableModule: Required getPostTransformNumericFormats function not found on postTransformModule.");
            return null;
        }
        return { mainModule, postTransformModule };
    }

    // --- Helper: Statistical Calculations --- <<< NEW >>>
    /**
     * Calculates the median of a numeric array.
     * @param {number[]} dataArray - Array of numbers.
     * @returns {number|NaN} The median value or NaN if input is invalid/empty after filtering.
     */
    function calculateMedian(dataArray) {
        if (!Array.isArray(dataArray)) return NaN;
        const sortedData = dataArray
            .map(val => Number(val)) // Ensure numeric type
            .filter(val => !isNaN(val)) // Filter out NaN
            .sort((a, b) => a - b);

        if (sortedData.length === 0) return NaN;

        const mid = Math.floor(sortedData.length / 2);
        if (sortedData.length % 2 === 0) {
            // Even number of elements: average of the two middle elements
            return (sortedData[mid - 1] + sortedData[mid]) / 2;
        } else {
            // Odd number of elements: the middle element
            return sortedData[mid];
        }
    }

    /**
     * Calculates the sample standard deviation of a numeric array.
     * @param {number[]} dataArray - Array of numbers.
     * @returns {number|NaN} The sample standard deviation or NaN if input is invalid or has < 2 valid numbers.
     */
    function calculateSampleStandardDeviation(dataArray) {
        if (!Array.isArray(dataArray)) return NaN;
        const numericData = dataArray
            .map(val => Number(val)) // Ensure numeric type
            .filter(val => !isNaN(val)); // Filter out NaN

        const n = numericData.length;
        if (n < 2) return NaN; // Sample std dev requires at least 2 points

        const mean = numericData.reduce((acc, val) => acc + val, 0) / n;
        const variance = numericData.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / (n - 1); // Use n-1 for sample

        return Math.sqrt(variance);
    }

    /**
     * Calculates the sample skewness of a numeric array.
     * Skewness measures the asymmetry of the probability distribution.
     * Requires at least 3 data points.
     * @param {number[]} dataArray - Array of numbers.
     * @returns {number|NaN} The sample skewness or NaN if input is invalid or has < 3 valid numbers.
     */
    function calculateSkewness(dataArray) {
        if (!Array.isArray(dataArray)) return NaN;
        const numericData = dataArray
            .map(val => Number(val))
            .filter(val => !isNaN(val));

        const n = numericData.length;
        if (n < 3) return NaN; // Skewness requires at least 3 points for sample correction

        const mean = numericData.reduce((acc, val) => acc + val, 0) / n;
        const stdDev = calculateSampleStandardDeviation(numericData); // Reuse existing helper

        if (isNaN(stdDev) || stdDev === 0) {
            // If std dev is 0 (all numbers are the same) or NaN, skewness is 0 or undefined.
            // Define as 0 for the case where stdDev is 0. Return NaN if stdDev calculation failed.
            return isNaN(stdDev) ? NaN : 0;
        }

        const skew = numericData.reduce((acc, val) => acc + Math.pow((val - mean) / stdDev, 3), 0);

        // Apply sample correction factor
        const sampleSkewness = (n / ((n - 1) * (n - 2))) * skew;

        return sampleSkewness;
    }

    // --- Helper: Dispose current popover and remove listener --- <<< NEW >>>
    function disposeCurrentPopoverAndListener() {
        if (currentCellPopoverInstance) {
            console.log("[DataTableModule] Disposing previous popover and removing listener.");
            document.removeEventListener('click', handleDocumentClickForPopoverDismiss, true); // Remove listener
            try {
                currentCellPopoverInstance.dispose();
            } catch (e) {
                console.warn("[DataTableModule] Error disposing popover instance:", e);
            }
            currentCellPopoverInstance = null;
            currentPopoverTriggerElement = null;
        }
    }

    // --- Helper: Document click handler for popover dismissal --- <<< NEW >>>
    function handleDocumentClickForPopoverDismiss(event) {
        if (!currentCellPopoverInstance || !currentPopoverTriggerElement) {
            // Should not happen if listener is managed correctly, but good safety check
            document.removeEventListener('click', handleDocumentClickForPopoverDismiss, true);
            return;
        }

        const popoverElement = currentCellPopoverInstance.tip; // Get the popover DOM element

        // Check if the click is outside the trigger and the popover itself
        const clickedTrigger = currentPopoverTriggerElement.contains(event.target);
        const clickedPopover = popoverElement ? popoverElement.contains(event.target) : false;

        // console.log(`[Doc Click] Trigger clicked: ${clickedTrigger}, Popover clicked: ${clickedPopover}, Target:`, event.target);

        if (!clickedTrigger && !clickedPopover) {
            // console.log("[DataTableModule] Click outside detected, disposing popover.");
            disposeCurrentPopoverAndListener();
        }
    }

    // --- Core Update Function --- <<< IMPLEMENTED >>>
    function updateFinalDataTable() {
        console.log("[DataTableModule] updateFinalDataTable called.");
        if (!tableElement || !statusElement) {
            console.error("DataTableModule: Table or status element not found.");
            return;
        }

        const modules = getRequiredModules();
        if (!modules) {
             statusElement.textContent = 'Error: Cannot load data (required modules missing).';
             return;
        }
        const { mainModule, postTransformModule } = modules;

        // Get data and settings from modules
        const finalData = mainModule.getFinalDataForAnalysis() || [];
        const finalMetadata = mainModule.getFinalFieldMetadata() || {};
        const enabledStatus = postTransformModule.getPostTransformEnabledStatus() || {};
        const postTransformNumericFormats = postTransformModule.getPostTransformNumericFormats() || {};
        const preTransformNumericFormats = mainModule.getNumericFieldFormats() || {};
        const infoTips = mainModule.getFieldInfoTips() || {}; // Use pre-transform tips for headers
        const formatNumericValue = (mainModule && typeof mainModule.getFormatter === 'function')
                                      ? mainModule.getFormatter()
                                      : ((val, fmt) => String(val)); // More robust fallback
        const generateFilename = mainModule.generateTimestampedFilename; // Get function reference

        // --- Destroy existing instance BEFORE preparing new options ---
        if (finalDataTableInstance) {
            console.log("[DataTableModule] Destroying previous table instance.");
            try {
                finalDataTableInstance.destroy();
                $(tableElement).empty(); // Clear headers/footers left by destroy
            } catch (e) { console.error("[DataTableModule] Error destroying instance:", e); }
            finalDataTableInstance = null;
        }

        // --- ADDED: Clear global summary at the start ---
        globalSummaryData = {};

        statusElement.textContent = 'Processing data for table...';

        // --- Handle No Data --- 
        if (finalData.length === 0) {
            console.log("[DataTableModule] No final data available.");
            if (finalDataTableInstance) {
                try {
                    finalDataTableInstance.destroy();
                    finalDataTableInstance = null;
                } catch (e) { console.error("[DataTableModule] Error destroying previous instance:", e);}
            }
             // Ensure thead is cleared
             const theadRow = tableElement.querySelector('thead tr');
             if (theadRow) theadRow.innerHTML = '<th>No Data Available</th>';
             // Clear and disable group by selector
             if (groupBySelector) {
                 groupBySelector.innerHTML = '<option value="-1" selected>-- No Data --</option>';
                 groupBySelector.disabled = true;
             }
             // Clear tbody as well
             const tbody = tableElement.querySelector('tbody');
             if (tbody) tbody.innerHTML = ''; 
             statusElement.textContent = 'No data to display.';
             lastAppliedHeaders = [];
            return;
        }

        // 1. Determine Headers/Columns: Use finalAvailableFields & filter by Post-Transform Status
        const finalAvailableFields = mainModule.getFinalAvailableFields() || [];
        const postTransformEnabledStatus = postTransformModule.getPostTransformEnabledStatus() || {}; // Get post-transform status
        currentHeaders = finalAvailableFields.filter(field => 
            postTransformEnabledStatus[field] !== false // Filter by post-transform status (default true)
        );

        // Ensure 'ticker' (lowercase) is first if it's present in the filtered list
        if (currentHeaders.includes('ticker')) {
            currentHeaders = ['ticker', ...currentHeaders.filter(h => h !== 'ticker')];
        }
        
        // REMOVED: Previous logic using only finalAvailableFields
        console.log("[DataTableModule] Effective Headers (finalAvailableFields filtered by post-transform status):", currentHeaders);

        // 2. Check if headers changed
        const headersChanged = JSON.stringify(currentHeaders) !== JSON.stringify(lastAppliedHeaders);
        if (headersChanged && finalDataTableInstance) {
            console.log("[DataTableModule] Headers changed, destroying existing DataTable.");
             try {
                finalDataTableInstance.destroy();
             } catch (e) { console.error("[DataTableModule] Error destroying instance on header change:", e); }
            finalDataTableInstance = null;
            // Clear the tbody and thead manually after destroying
            $(tableElement).find('tbody').empty();
            $(tableElement).find('thead tr').empty();
        }
        lastAppliedHeaders = [...currentHeaders]; // Store current headers

        // --- ADDED: Calculate Global Summaries if NOT Grouping ---
        const groupIndex = parseInt(groupBySelector?.value);
        if (groupIndex < 0 && finalData.length > 0) {
            console.log("[DataTableModule] Calculating global summary statistics...");
            const modules = getRequiredModules(); // Need modules here too
            if (!modules) { // <<< ADD check for modules here
                console.error("[DataTableModule] Cannot calculate global stats: Required modules missing.");
                return; 
            }
            const finalMetadata = modules?.mainModule?.getFinalFieldMetadata ? modules.mainModule.getFinalFieldMetadata() : {};
            const postTransformNumericFormats = modules?.postTransformModule?.getPostTransformNumericFormats ? modules.postTransformModule.getPostTransformNumericFormats() : {};
            const preTransformNumericFormats = modules?.mainModule?.getNumericFieldFormats ? modules.mainModule.getNumericFieldFormats() : {};
            // const formatNumericValue = modules?.mainModule?.formatNumericValue || ((val, fmt) => val); // <<< REMOVE old direct access
            // <<< ADD access via getter INSIDE this block >>>
            const formatNumericValue = (modules.mainModule && typeof modules.mainModule.getFormatter === 'function')
                                          ? modules.mainModule.getFormatter()
                                          : ((val, fmt) => String(val));

            currentHeaders.forEach((header, colIdx) => {
                const meta = finalMetadata[header] || {};
                if (meta.type === 'numeric') {
                    // Pluck data for this column from the *entire* finalData set
                    const colDataArray = finalData.map(item => {
                        // Handle potential data structure differences if needed
                        if (header === 'ticker') return item?.ticker;
                        return item?.processed_data?.[header];
                    });
                    const colData = colDataArray.map(parseFloat).filter(val => !isNaN(val));

                    if (colData.length > 0) {
                        // REMOVED: var sum = colData.reduce((a, b) => a + b, 0);
                        var min = Math.min(...colData);
                        var max = Math.max(...colData);
                        var avg = colData.reduce((a, b) => a + b, 0) / colData.length; // Keep avg calculation
                        // --- NEW: Calculate Median and Std Dev --- 
                        var median = calculateMedian(colData);
                        var stdDev = calculateSampleStandardDeviation(colData);
                        // --- END NEW --- 
                        // --- NEW: Calculate Skewness --- 
                        var skewness = calculateSkewness(colData);
                        // --- END NEW --- 

                        const effectiveFormat = postTransformNumericFormats.hasOwnProperty(header)
                                                    ? postTransformNumericFormats[header]
                                                    : (preTransformNumericFormats[header] || 'default');
                        // Store in the global object
                        // <<< Use the locally retrieved formatNumericValue >>>
                        globalSummaryData[header] = {
                            raw: {
                                // sum: sum, // REMOVED
                                min: min,
                                max: max,
                                count: colData.length,
                                avg: avg,
                                median: median, // ADDED
                                stdDev: stdDev,  // ADDED
                                skewness: skewness // ADDED
                            },
                            formatted: {
                                // sum: formatNumericValue(sum, effectiveFormat), // REMOVED
                                min: formatNumericValue(min, effectiveFormat),
                                max: formatNumericValue(max, effectiveFormat),
                                avg: formatNumericValue(avg, effectiveFormat),
                                median: formatNumericValue(median, effectiveFormat), // ADDED
                                stdDev: formatNumericValue(stdDev, effectiveFormat),  // ADDED
                                skewness: formatNumericValue(skewness, 'numeric_2') // ADDED - Using 'numeric_2' format for skewness
                            }
                        };
                    }
                }
            });
            console.log("[DataTableModule] Global stats calculated:", globalSummaryData);
        }
        // --- END Global Summary Calculation ---

        // 3. Format data for DataTable (array of arrays, raw numeric values)
        const tableData = finalData.map(item => {
            if (!item) return currentHeaders.map(() => null); // Return array of nulls if item is bad
            return currentHeaders.map(header => {
                let rawValue = null;
                if (header === 'ticker') {
                    rawValue = item.ticker;
                } else if (item.processed_data && item.processed_data.hasOwnProperty(header)) {
                    rawValue = item.processed_data[header];
                } // Add checks for source/error if included later
                // Return raw value (number, string, null). Avoid formatting here.
                return rawValue;
            });
        });

        // 4. Always Recreate Table Header (since we destroy)
        const thead = tableElement.querySelector('thead') || tableElement.createTHead();
        thead.innerHTML = ''; // Clear existing header
        const theadRow = thead.insertRow();
            currentHeaders.forEach(headerText => {
                const th = document.createElement('th');
            th.textContent = (headerText === 'ticker' ? 'Ticker' : headerText);
                const tip = infoTips[headerText];
                if (tip) {
                    th.title = tip;
                    th.dataset.bsToggle = 'tooltip';
                    th.dataset.bsPlacement = 'top';
                }
                theadRow.appendChild(th);
            });

        // Ensure tbody exists
        let tbody = tableElement.querySelector('tbody');
        if (!tbody) {
            tbody = tableElement.createTBody();
        }
        tbody.innerHTML = ''; // Clear body before potential init error

        // 5. Initialize or Update DataTable
        try {
            // Define columnDefs for custom rendering (formatting, negative numbers)
            const columnDefs = currentHeaders.map((headerText, index) => {
                const meta = finalMetadata[headerText] || {};
                if (meta.type === 'numeric') {
                    const effectiveFormat = postTransformNumericFormats.hasOwnProperty(headerText) 
                                                ? postTransformNumericFormats[headerText] 
                                                : (preTransformNumericFormats[headerText] || 'default');
                    return {
                        targets: index,
                        render: function (data, type, row) {
                            // Apply formatting only for display
                            if (type === 'display') {
                                const rawValue = data; // Data is the raw value passed
                                const num = Number(rawValue);
                                const formattedValue = formatNumericValue(rawValue, effectiveFormat);
                                // Apply styling for negative numbers
                                if (!isNaN(num) && num < 0) {
                                    return `<span class="text-negative">${formattedValue}</span>`;
                                }
                                return formattedValue; // Return formatted (non-negative) value
                            }
                            // For sorting, filtering, type detection etc., return raw data
                            return data;
                        }
                    };
                }
                return null; // No specific def for non-numeric columns
            }).filter(def => def !== null); // Remove nulls

             if (finalDataTableInstance) {
                 // --- This block is now removed, initialization happens below ---
             } else {
                 // --- Prepare DataTable Options --- 
                 const groupIndex = parseInt(groupBySelector?.value);
                 const dtOptions = {
                     data: tableData,
                     columns: currentHeaders.map(header => ({ title: (header === 'ticker' ? 'Ticker' : header) })),
                     order: [[0, 'asc']], // Default dynamic order (will be overridden by fixed if grouping)
                     columnDefs: columnDefs,
                     deferRender: true,
                     paging: true,
                     searching: true,
                     lengthChange: true,
                     pageLength: 50,
                     scrollX: true,
                     scrollY: '500px', // Adjust as needed
                     scrollCollapse: true,
                     stateSave: false,
                     language: {
                         emptyTable: "No data available after transformations and filtering.",
                         zeroRecords: "No matching records found"
                     },
                     dom: "<'row'<'col-sm-12 col-md-6'l><'col-sm-12 col-md-6 d-flex justify-content-end align-items-center'fB>>" +
                          "<'row'<'col-sm-12'tr>>" +
                          "<'row'<'col-sm-12 col-md-5'i><'col-sm-12 col-md-7'p>>",
                     buttons: [
                         { 
                             extend: 'copyHtml5', 
                             filename: () => generateFilename('FinalData_Copy'),
                             title: () => `Final Data (${new Date().toLocaleString()})` 
                         },
                         { 
                             extend: 'excelHtml5', 
                             filename: () => generateFilename('FinalData'),
                             title: () => `Final Data (${new Date().toLocaleString()})`
                         },
                         { 
                             extend: 'csvHtml5', 
                             filename: () => generateFilename('FinalData'),
                             title: () => `Final Data (${new Date().toLocaleString()})`
                         }
                         // TODO: Add ColVis button if desired
                     ],
                     destroy: true // Important to allow re-initialization
                 };

                 // Conditionally add orderFixed
                 if (groupIndex >= 0) {
                    dtOptions.orderFixed = { pre: [[groupIndex, 'asc']] };
                 }

                 // Conditionally add rowGroup option
                 if (groupIndex >= 0) {
                     dtOptions.rowGroup = {
                         dataSrc: groupIndex,
                         startRender: function (rows, group) {
                             var table = $(tableElement).DataTable(); 
                             var columns = table.columns(); 
                             var visibleColumnCount = 0;
                             columns.every(function() { if (this.visible()) visibleColumnCount++; });
 
                             var summaryData = {}; 
                             const modules = getRequiredModules();
                             if (!modules) { // <<< ADD Check for modules >>>
                                console.error("[DataTableModule/startRender] Cannot calculate group stats: Required modules missing.");
                                return group + ' (Error)'; // Return basic group name on error
                             }
                             const finalMetadata = modules?.mainModule?.getFinalFieldMetadata ? modules.mainModule.getFinalFieldMetadata() : {};
                             const postTransformNumericFormats = modules?.postTransformModule?.getPostTransformNumericFormats ? modules.postTransformModule.getPostTransformNumericFormats() : {};
                             const preTransformNumericFormats = modules?.mainModule?.getNumericFieldFormats ? modules.mainModule.getNumericFieldFormats() : {};
                              // <<< ADD access via getter INSIDE callback >>>
                             const formatNumericValue = (modules.mainModule && typeof modules.mainModule.getFormatter === 'function')
                                                           ? modules.mainModule.getFormatter()
                                                           : ((val, fmt) => String(val));
 
                             columns.every(function (colIdx) {
                                 const header = currentHeaders[colIdx]; 
                                 const meta = finalMetadata[header] || {};
                                 if (meta.type === 'numeric') {
                                     var colDataArray = rows.data().pluck(colIdx).toArray();
                                     var colData = colDataArray.map(parseFloat).filter(val => !isNaN(val));
                                     if (colData.length > 0) {
                                         // REMOVED: var sum = colData.reduce((a, b) => a + b, 0);
                                         var min = Math.min(...colData);
                                         var max = Math.max(...colData);
                                         var avg = colData.reduce((a, b) => a + b, 0) / colData.length; // Keep avg calc
                                         // --- NEW: Calculate Median and Std Dev --- 
                                         var median = calculateMedian(colData);
                                         var stdDev = calculateSampleStandardDeviation(colData);
                                         // --- END NEW --- 
                                         // --- NEW: Calculate Skewness --- 
                                         var skewness = calculateSkewness(colData);
                                         // --- END NEW --- 
                                         const effectiveFormat = postTransformNumericFormats.hasOwnProperty(header) 
                                                                 ? postTransformNumericFormats[header] 
                                                                 : (preTransformNumericFormats[header] || 'default');
                                         // <<< Use the locally retrieved formatNumericValue >>>
                                         summaryData[header] = { 
                                             raw: {
                                                 // sum: sum, // REMOVED
                                                 min: min,
                                                 max: max,
                                                 count: colData.length,
                                                 avg: avg,
                                                 median: median, // ADDED
                                                 stdDev: stdDev,  // ADDED
                                                 skewness: skewness // ADDED
                                             },
                                             formatted: { 
                                                 // sum: formatNumericValue(sum, effectiveFormat), // REMOVED
                                                 min: formatNumericValue(min, effectiveFormat),
                                                 max: formatNumericValue(max, effectiveFormat),
                                                 avg: formatNumericValue(avg, effectiveFormat),
                                                 median: formatNumericValue(median, effectiveFormat), // ADDED
                                                 stdDev: formatNumericValue(stdDev, effectiveFormat),  // ADDED
                                                 skewness: formatNumericValue(skewness, 'numeric_2') // ADDED - Using 'numeric_2' format for skewness
                                             }
                                         };
                                     }
                                 }
                             });
                             return $('<tr class="group-header-row bg-secondary-subtle dt-group-title">>')
                                 .attr('data-group', group)
                                 .attr('data-summary', JSON.stringify(summaryData))
                                 .append('<td colspan="' + visibleColumnCount + '">' + group + ' (' + rows.count() + ' rows)</td>')
                                 .get(0);
                         }
                     };
                 } // Else: rowGroup option is omitted, disabling it

                 // Initialize DataTable for the first time or after destruction
                 console.log("[DataTableModule] Initializing DataTable.");
                 finalDataTableInstance = $(tableElement).DataTable(dtOptions);
                // Adjust columns after initial draw
                finalDataTableInstance.columns.adjust().draw();
                 // Update group by options
                 populateGroupBySelector(currentHeaders);
             }

            // 6. Initialize tooltips on header
            initializeFinalTableTooltips();

            // Attach hover highlights AND the new click listener
            attachTableListeners(tableElement);

            // 7. Update Status
            statusElement.textContent = `Showing ${finalData.length} records.`;

         } catch (error) {
             console.error("[DataTableModule] Error initializing or updating DataTable:", error);
             statusElement.textContent = 'Error displaying data table. Check console.';
             // Disable group by selector on error
             if (groupBySelector) groupBySelector.disabled = true;
             if (finalDataTableInstance) { // Attempt cleanup if error occurred mid-update
                  try { finalDataTableInstance.destroy(); } catch (e) {}
                  finalDataTableInstance = null;
             }
        }
    }

    // --- Tooltip Initializer --- <<< IMPLEMENTED >>>
    function initializeFinalTableTooltips() {
        console.log("[DataTableModule] Initializing tooltips for final table header...");
        if (!tableElement) return;
        const tooltipTriggerList = tableElement.querySelectorAll('thead [data-bs-toggle="tooltip"]');
         // Dispose existing tooltips first
         tooltipTriggerList.forEach(tooltipTriggerEl => {
             const existingTooltip = bootstrap.Tooltip.getInstance(tooltipTriggerEl);
             if (existingTooltip) {
                 // console.log("Disposing existing tooltip for:", tooltipTriggerEl);
                 existingTooltip.dispose();
             }
         });
         // Initialize new tooltips
         const newTooltipList = [...tooltipTriggerList].map(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl));
         console.log(`[DataTableModule] Initialized ${newTooltipList.length} tooltips.`);
    }

    // --- NEW: Combined Listener Attachment Function --- <<< RENAMED & UPDATED >>>
    function attachTableListeners(tableElem) {
        // Remove previous listeners to prevent duplicates if table is updated
        $(tableElem).off('mouseenter', 'tbody td');
        $(tableElem).off('mouseleave', 'tbody td');
        $(tableElem).off('click', 'tbody tr.group-header-row');
        $(tableElem).off('click', 'tbody tr:not(.group-header-row) td');
        $(tableElem).off('click', 'thead th');

        // Attach new listeners using jQuery delegation
        $(tableElem).on('mouseenter', 'tbody td', function() {
            // console.log("[Hover Highlight] Mouseenter on TD triggered.");
            const cell = this;
            const row = $(cell).parent('tr')[0]; // Get DOM element for classList
            const cellIndex = cell.cellIndex;

            // Highlight row
            if (row) {
                row.classList.add('row-highlight');
            }

            // Highlight column (using jQuery for selector efficiency)
            $(tableElem).find('tbody tr').each(function() {
                $(this).find('td').eq(cellIndex).addClass('col-highlight');
            });
        });

        $(tableElem).on('mouseleave', 'tbody td', function() {
             // console.log("[Hover Highlight] Mouseleave on TD triggered.");
            const cell = this;
            const row = $(cell).parent('tr')[0];
            const cellIndex = cell.cellIndex;

            // Remove row highlight
            if (row) {
                row.classList.remove('row-highlight');
            }

            // Remove column highlight
            $(tableElem).find('tbody tr').each(function() {
                $(this).find('td').eq(cellIndex).removeClass('col-highlight');
            });
        });

        // --- UPDATED: Attach Click Listener for Cell Summary Popover ---
        $(tableElem).on('click', 'tbody tr:not(.group-header-row) td', function(e) {
            e.preventDefault();
            console.log("[DataTableModule] Data cell clicked.");

            const cellElement = this; // Keep 'this' as the trigger element

            // --- NEW: Dispose previous popover and listener first ---
            disposeCurrentPopoverAndListener();
            // --- END NEW ---

            const $cell = $(cellElement);
            const cellIndex = cellElement.cellIndex;
            let groupName = "Overall"; // Default for no grouping
            let summaryJson = null;
            let summaryDataSource = null; // To hold either group or global summary

            const groupIndex = parseInt(groupBySelector?.value); // Check current grouping state

            if (groupIndex >= 0) {
                // --- Grouping Active: Get group summary ---
                const $groupHeaderRow = $cell.parent('tr').prevAll('tr.group-header-row').first();
                if ($groupHeaderRow.length > 0) {
                    groupName = $groupHeaderRow.data('group');
                    summaryJson = $groupHeaderRow.attr('data-summary');
                    console.log(`[DataTableModule] Using Group Summary for: ${groupName}`);
                } else {
                    console.warn("[DataTableModule] Could not find preceding group header row.");
                    return; // Cannot proceed without group summary
                }
            } else {
                // --- No Grouping: Use global summary ---
                summaryDataSource = globalSummaryData; // Use the module-scoped global data
                console.log("[DataTableModule] Using Global Summary.");
            }

            // --- Parse Summary Data ---
            let summaryData = {};
            if (summaryDataSource) {
                 summaryData = summaryDataSource; // Already an object
            } else if (summaryJson) {
                 try {
                     summaryData = JSON.parse(summaryJson || '{}');
                 } catch (err) {
                     console.error("[DataTableModule] Error parsing summary data from attribute:", err);
                     return; // Stop if summary data is bad
                 }
            } else {
                 console.warn("[DataTableModule] No summary data source found.");
                 return; // No summary to show
            }

            // Get column header for the clicked cell
            const modules = getRequiredModules(); 
            if (!modules) return; // Stop if modules not found
            const { mainModule, postTransformModule } = modules;
            const formatNumericValue = (mainModule && typeof mainModule.getFormatter === 'function')
                                          ? mainModule.getFormatter()
                                          : ((val, fmt) => String(val)); // Get formatter here
            const postTransformNumericFormats = postTransformModule.getPostTransformNumericFormats ? postTransformModule.getPostTransformNumericFormats() : {};
            const preTransformNumericFormats = mainModule.getNumericFieldFormats ? mainModule.getNumericFieldFormats() : {};
            const tableApi = $(tableElem).DataTable();
            const headerText = tableApi.column(cellIndex).header().textContent; // Get header text
            // Ensure actualHeaderKey uses module-scoped currentHeaders
            const actualHeaderKey = currentHeaders[cellIndex]; // <<< Uses module-scoped currentHeaders

            console.log(`Clicked cell: Header='${headerText}', Key='${actualHeaderKey}', Context='${groupName}'`);

            // Check if summary exists for this specific column
            if (summaryData.hasOwnProperty(actualHeaderKey)) {
                const stats = summaryData[actualHeaderKey].formatted;
                const rawStats = summaryData[actualHeaderKey].raw;

                // --- Generate Text Stats --- 
                const textStatsHtml = 
                    `<div class="mb-2"><strong>Avg:</strong> ${stats.avg} | <strong>Median:</strong> ${stats.median}<br>` +
                    `<strong>Min:</strong> ${stats.min} | <strong>Max:</strong> ${stats.max}<br>` +
                    `<strong>Std Dev:</strong> ${stats.stdDev} | <strong>Skew:</strong> ${stats.skewness}</div>`;

                // --- Generate Visual Summary --- 
                let visualHtml = '';
                const cellData = tableApi.cell(cellElement).data(); // Get raw cell data
                const cellValue = parseFloat(cellData);

                if (rawStats && !isNaN(cellValue) && typeof rawStats.min === 'number' && typeof rawStats.max === 'number') {
                    const minVal = rawStats.min;
                    const maxVal = rawStats.max;
                    const avgVal = rawStats.avg;
                    const range = maxVal - minVal;

                    if (range > 0) {
                        const avgPercent = Math.max(0, Math.min(100, ((avgVal - minVal) / range) * 100));
                        const valuePercent = Math.max(0, Math.min(100, ((cellValue - minVal) / range) * 100));

                        // Determine effective format for this column
                        const effectiveFormat = postTransformNumericFormats.hasOwnProperty(actualHeaderKey) 
                                                   ? postTransformNumericFormats[actualHeaderKey] 
                                                   : (preTransformNumericFormats[actualHeaderKey] || 'default');

                        // <<< Format values for titles >>>
                        const formattedMin = formatNumericValue(minVal, effectiveFormat);
                        const formattedMax = formatNumericValue(maxVal, effectiveFormat);
                        const formattedAvg = formatNumericValue(avgVal, effectiveFormat);
                        const formattedValue = formatNumericValue(cellValue, effectiveFormat);

                        visualHtml = `
                            <div class="summary-visual-container mt-2 pt-3 pb-3 position-relative">
                                <div class="summary-line" title="Range: ${formattedMin} to ${formattedMax}" style="height: 1px; background-color: darkred; width: 100%; position: absolute;"></div>
                                ${typeof avgVal === 'number' ? `<span class="summary-marker marker-avg" style="left: ${avgPercent.toFixed(1)}%;" title="Avg: ${formattedAvg}"></span>` : ''}
                                <span class="summary-marker marker-value marker-value-large" style="left: ${valuePercent.toFixed(1)}%;" title="Value: ${formattedValue}"></span> 
                            </div>
                        `;
                    } else { // Handle case where min === max
                        // <<< Format the single value >>>
                        const formattedMinMax = formatNumericValue(minVal, postTransformNumericFormats.hasOwnProperty(actualHeaderKey) ? postTransformNumericFormats[actualHeaderKey] : (preTransformNumericFormats[actualHeaderKey] || 'default'));
                        visualHtml = `<div class="mt-2 text-center text-muted small">(Min/Max are equal: ${formattedMinMax})</div>`;
                    }
                } else {
                     visualHtml = `<div class="mt-2 text-center text-muted small">(Could not render visual: Invalid data)</div>`;
                }

                const popoverContent = textStatsHtml + visualHtml;

                // --- UPDATED: Destroy *other* existing popovers on this table (redundant if disposeCurrentPopoverAndListener works, but safe) ---
                $(tableElem).find('[data-bs-toggle="popover"]').each(function() {
                     if (this !== cellElement) { // Don't dispose self if somehow marked already
                         const existingPopover = bootstrap.Popover.getInstance(this);
                         if (existingPopover) {
                             existingPopover.dispose();
                         }
                         $(this).removeAttr('data-bs-toggle data-bs-original-title title data-bs-content'); // Clean up attributes
                     }
                });
                // --- END UPDATE ---

                // Initialize Popover on the clicked cell
                const popover = new bootstrap.Popover(cellElement, {
                    title: `Summary: ${groupName}<br><small>Column: ${headerText}</small>`,
                    content: popoverContent,
                    html: true,
                    trigger: 'manual', // Keep manual trigger
                    placement: 'auto',
                    fallbackPlacements: ['bottom', 'top', 'right', 'left'],
                    customClass: 'analytics-summary-popover',
                    sanitize: false
                });

                // --- NEW: Store instance, trigger, and add document listener ---
                currentCellPopoverInstance = popover;
                currentPopoverTriggerElement = cellElement; // Store the actual clicked cell
                document.addEventListener('click', handleDocumentClickForPopoverDismiss, true); // Use capture phase
                // --- END NEW ---

                // Add attribute to mark this cell - useful for cleanup, maybe redundant now
                $(cellElement).attr('data-bs-toggle', 'popover');

                popover.show();

            } else {
                console.log(`[DataTableModule] No summary data found for column '${actualHeaderKey}' in context '${groupName}'.`);
                // If no popover is shown, ensure no dangling listener exists
                disposeCurrentPopoverAndListener(); // Call cleanup just in case
            }
        });
        console.log("[DataTableModule] Cell summary popover listener attached (with outside click dismissal).");

        // --- Attach Click Listener for Header Sorting (within groups) ---
        $(tableElem).on('click', 'thead th', function() {
            if (!finalDataTableInstance) return; // No table instance

            const groupIndex = parseInt(groupBySelector?.value);
            if (groupIndex < 0) return; // Grouping not active, allow default sorting

            const headerElement = this;
            const clickedColumnIndex = finalDataTableInstance.column(headerElement).index();

            if (clickedColumnIndex === undefined || clickedColumnIndex === groupIndex) {
                // Don't allow sorting by the grouping column itself via header click
                return; 
            }

            console.log(`[DataTableModule] Header clicked for column index: ${clickedColumnIndex}, Grouping active on index: ${groupIndex}`);

            // Determine new sort direction
            let currentOrder = finalDataTableInstance.order(); // Get current full order
            let newDirection = 'asc';
            // Check if the clicked column is already the secondary sort
            if (currentOrder.length > 1 && currentOrder[1][0] === clickedColumnIndex) {
                newDirection = currentOrder[1][1] === 'asc' ? 'desc' : 'asc';
            }

            // Apply only the dynamic secondary sort (fixed order handles the primary)
            finalDataTableInstance.order([[clickedColumnIndex, newDirection]]).draw();
            console.log(`[DataTableModule] Applied secondary sort: Column ${clickedColumnIndex}, Direction ${newDirection}`);

        });
        console.log("[DataTableModule] Header click listener for secondary sorting attached.");
    }

    // --- Expose Public API ---
    window.AnalyticsDataTableModule = {
        updateTable: updateFinalDataTable,
        getInstance: () => finalDataTableInstance // Expose instance if needed externally
    };
    console.log("AnalyticsDataTableModule API exposed.");

    // --- Tab Listener for Column Adjustment ---
    if (dataTableTabTrigger) {
        dataTableTabTrigger.addEventListener('shown.bs.tab', function (event) {
            console.log("[DataTableModule] Data Table tab shown. Adjusting columns (if initialized).");
            if (finalDataTableInstance && tableContainer && tableContainer.offsetParent !== null) { // Check if visible and initialized
                // Use setTimeout to allow rendering to complete
                setTimeout(() => {
                    try {
                         finalDataTableInstance.columns.adjust().draw(false);
                         console.log("[DataTableModule] DataTable columns adjusted.");
                    } catch(e) {
                        console.error("[DataTableModule] Error adjusting columns on tab show:", e);
                    }
                }, 50); // Small delay
            } else {
                console.log("[DataTableModule] Skipping column adjust (table not initialized or not visible).");
            }
        });
    } else {
        console.warn("[DataTableModule] Could not find Data Table tab trigger (#data-table-tab) for column adjust listener.");
    }

    // --- NEW: Populate Group By Selector --- <<< IMPLEMENTED >>>
    function populateGroupBySelector(headers) {
        if (!groupBySelector) return;

        const currentValue = groupBySelector.value; // Remember current selection
        groupBySelector.innerHTML = '<option value="-1">-- No Grouping --</option>'; // Clear and add default

        if (!headers || headers.length === 0) {
            groupBySelector.disabled = true;
            return;
        }

        headers.forEach((header, index) => {
            const option = document.createElement('option');
            option.value = index.toString();
            option.textContent = (header === 'ticker' ? 'Ticker' : header);
            option.selected = (index.toString() === currentValue); // Restore selection
            groupBySelector.appendChild(option);
        });
        groupBySelector.disabled = false;
    }

    // --- NEW: Add event listener for Group By Selector --- <<< IMPLEMENTED >>>
    if (groupBySelector) {
        groupBySelector.addEventListener('change', () => {
            console.log(`[DataTableModule] Group By selection changed to index: ${groupBySelector.value}`);
            updateFinalDataTable(); // Re-render table with new grouping
        });
    }
    
    // --- Initial Setup (Optional - Placeholder) ---
    // If the table needs to load immediately (e.g., if data might be ready from cache):
    // updateFinalDataTable();
    console.log("AnalyticsDataTableModule initialized.");

}); // End DOMContentLoaded 