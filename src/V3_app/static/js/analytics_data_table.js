document.addEventListener('DOMContentLoaded', function() {
    console.log("AnalyticsDataTableModule: DOMContentLoaded event fired.");

    let finalDataTableInstance = null;
    let lastAppliedHeaders = [];
    const tableElement = document.getElementById('final-data-table');
    const statusElement = document.getElementById('final-table-status');
    const tableContainer = document.getElementById('final-data-table-container'); // For visibility checks
    const dataTableTabTrigger = document.getElementById('data-table-tab'); // ID of the main tab button
    const groupBySelector = document.getElementById('final-table-group-by-selector');

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
        const formatNumericValue = mainModule.formatNumericValue || ((val, fmt) => val); // Fallback
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
        let currentHeaders = finalAvailableFields.filter(field => 
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
                     order: [[(groupIndex >= 0 ? groupIndex : 0), 'asc']], // Sort by group column or first column
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
                             const finalMetadata = modules?.mainModule?.getFinalFieldMetadata ? modules.mainModule.getFinalFieldMetadata() : {};
                             const postTransformNumericFormats = modules?.postTransformModule?.getPostTransformNumericFormats ? modules.postTransformModule.getPostTransformNumericFormats() : {};
                             const preTransformNumericFormats = modules?.mainModule?.getNumericFieldFormats ? modules.mainModule.getNumericFieldFormats() : {};
                             const formatNumericValue = modules?.mainModule?.formatNumericValue || ((val, fmt) => val);

                             // Calculate summaries (only need stats, not HTML)
                             columns.every(function (colIdx) {
                                 const header = currentHeaders[colIdx];
                                 const meta = finalMetadata[header] || {};
                                 if (meta.type === 'numeric') {
                                     var colDataArray = rows.data().pluck(colIdx).toArray();
                                     var colData = colDataArray.map(parseFloat).filter(val => !isNaN(val));
                                     if (colData.length > 0) {
                                         var sum = colData.reduce((a, b) => a + b, 0);
                                         var min = Math.min(...colData);
                                         var max = Math.max(...colData);
                                         var avg = sum / colData.length;
                                         const effectiveFormat = postTransformNumericFormats.hasOwnProperty(header) 
                                                                 ? postTransformNumericFormats[header] 
                                                                 : (preTransformNumericFormats[header] || 'default');
                                         // Store raw and formatted values for the modal
                                         summaryData[header] = { 
                                             raw: { sum: sum, min: min, max: max, count: colData.length, avg: avg },
                                             formatted: { 
                                                 sum: formatNumericValue(sum, effectiveFormat),
                                                 min: formatNumericValue(min, effectiveFormat),
                                                 max: formatNumericValue(max, effectiveFormat),
                                                 avg: formatNumericValue(avg, effectiveFormat)
                                             }
                                         };
                                     }
                                 }
                             });

                             // Create the group header row
                             return $('<tr class="group-header-row bg-secondary-subtle" style="cursor: pointer;">')
                                 .attr('data-group', group)
                                 .attr('data-summary', JSON.stringify(summaryData))
                                 .append('<td colspan="' + visibleColumnCount + '"><i class="bi bi-info-circle me-2"></i>' + group + ' (' + rows.count() + ' rows)</td>')
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

        $(tableElem).off('click', 'tbody tr.group-header-row'); // Remove previous click listener

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

        // --- NEW: Attach Click Listener for Group Summary Modal ---
        $(tableElem).on('click', 'tbody tr.group-header-row', function(e) {
            e.preventDefault();
            console.log("[DataTableModule] Group header clicked.");

            const groupName = $(this).data('group');
            // Use .attr() to reliably get the raw attribute value
            const summaryJson = $(this).attr('data-summary'); 
            let summaryData = {};
            try {
                summaryData = JSON.parse(summaryJson || '{}');
            } catch (err) {
                console.error("[DataTableModule] Error parsing summary data from attribute:", err);
            }

            // Get modal elements
            const modalElement = document.getElementById('groupSummaryModal');
            const modalTitle = document.getElementById('groupSummaryModalLabel');
            const modalBody = document.getElementById('groupSummaryModalBody');

            if (!modalElement || !modalTitle || !modalBody) {
                console.error("[DataTableModule] Group summary modal elements not found.");
                return;
            }

            // Populate Modal
            modalTitle.textContent = `Summary for Group: ${groupName}`;

            let bodyHtml = '<dl class="row">'; // Use definition list for layout
            if (Object.keys(summaryData).length > 0) {
                for (const header in summaryData) {
                    if (summaryData.hasOwnProperty(header)) {
                        const stats = summaryData[header].formatted;
                        bodyHtml += `<dt class="col-sm-4 border-top pt-1">${header}</dt>`;
                        bodyHtml += `<dd class="col-sm-8 border-top pt-1">`;
                        bodyHtml += `<span class="me-3">Avg: ${stats.avg}</span>`;
                        bodyHtml += `<span class="me-3">Sum: ${stats.sum}</span>`;
                        bodyHtml += `<span class="me-3">Min: ${stats.min}</span>`;
                        bodyHtml += `<span>Max: ${stats.max}</span>`;
                        bodyHtml += `</dd>`;
                    }
                }
            } else {
                bodyHtml += '<p class="text-muted">No numeric data found for summary in this group.</p>';
            }
            bodyHtml += '</dl>';
            modalBody.innerHTML = bodyHtml;

            // Show Modal
            const bsModal = bootstrap.Modal.getOrCreateInstance(modalElement);
            bsModal.show();
        });
        console.log("[DataTableModule] Group summary click listener attached.");
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