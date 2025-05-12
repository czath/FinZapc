console.log(">>> Executing analytics_pre_transform_table.js (global scope start)");

// === Analytics Pre-Transform Data Set Table Module ===
(function() { // Wrap everything in an IIFE, but don't assign its return value directly
    console.log("AnalyticsPreTransformTableModule IIFE executing...");

    let tableElement = null;
    let dataTableInstance = null;

    /**
     * Initializes the module, caching DOM elements.
     * Should be called on DOMContentLoaded.
     */
    function init() {
        tableElement = document.getElementById('pre-transform-data-set-table');
        console.log("[PreTransformTable Init Check] Finding element #pre-transform-data-set-table:", tableElement);
        if (!tableElement) {
            console.error("[PreTransformTable] Table element #pre-transform-data-set-table not found!");
        }
        console.log("[PreTransformTable] init complete. Table element cached:", tableElement ? 'Found' : 'Not Found');
    }

    /**
     * Updates the DataTable with new data and configuration.
     * 
     * @param {Array<object>} data - Array of filtered data objects (e.g., [{ ticker: 'AAPL', processed_data: { Price: 150, ... } }]).
     * @param {Array<string>} headers - Array of header strings for currently enabled columns (e.g., ['Ticker', 'Price', 'Market Cap']).
     * @param {object} fieldMetadata - Metadata object for fields (e.g., { Price: { type: 'numeric' }, ... }).
     * @param {object} fieldNumericFormats - Numeric format settings (e.g., { 'Market Cap': 'billion' }).
     * @param {function} formatNumericValueFunction - Reference to the numeric formatting function from analytics.js.
     */
    function updateTable(data, headers, fieldMetadata, fieldNumericFormats, formatNumericValueFunction) {
        console.log("[PreTransformTable] updateTable called.");
        console.log("[PreTransformTable Update Check] tableElement:", tableElement);
        console.log("[PreTransformTable Update Check] data length:", data ? data.length : 'null/undefined');
        console.log("[PreTransformTable Update Check] headers length:", headers ? headers.length : 'null/undefined');
        if (!tableElement) {
            console.error("[PreTransformTable] Cannot update table, element not initialized.");
            return;
        }
        if (!data || !headers || !fieldMetadata || !fieldNumericFormats || typeof formatNumericValueFunction !== 'function') {
            console.error("[PreTransformTable] updateTable called with missing or invalid arguments.", {
                data: !!data,
                headers: !!headers,
                fieldMetadata: !!fieldMetadata,
                fieldNumericFormats: !!fieldNumericFormats,
                formatter: typeof formatNumericValueFunction
            });
            // Optionally clear the table or show an error state
            return;
        }

        console.log(`[PreTransformTable] Received ${data.length} data rows, ${headers.length} headers.`);
        console.log("[PreTransformTable] Headers:", headers);
        // console.log("[PreTransformTable] Metadata (sample):", Object.keys(fieldMetadata).length > 0 ? fieldMetadata[Object.keys(fieldMetadata)[0]] : 'Empty');
        // console.log("[PreTransformTable] Formats:", fieldNumericFormats);
        
        // 1. Destroy existing DataTable instance if it exists
        if (dataTableInstance) {
            console.log("[PreTransformTable] Destroying existing DataTable instance.");
            dataTableInstance.destroy();
            // Clear the table body and header explicitly after destroying
            $(tableElement).empty(); 
            dataTableInstance = null;
        }

        // 2. Prepare tableData (array of arrays with RAW values for sorting)
        const tableData = data.map(item => {
            return headers.map(headerText => {
                // Handle 'Ticker' potentially being mapped from 'ticker'
                const fieldName = (headerText === 'Ticker') ? 'ticker' : headerText;
                let value = null;

                if (fieldName === 'ticker' && item.ticker) {
                    value = item.ticker;
                } else if (item.processed_data && item.processed_data.hasOwnProperty(fieldName)) {
                    value = item.processed_data[fieldName];
                } else if (item.error) {
                     value = `Error: ${item.error}`;
                } else {
                     value = null; // Or some placeholder like 'N/A'
                }
                
                // Return raw value - formatting happens in columnDefs render
                return value;
            });
        });
        console.log(`[PreTransformTable] Prepared tableData with ${tableData.length} rows.`);

        // 3. Prepare columnDefs for rendering and type specification
        const columnDefs = headers.map((headerText, index) => {
            const fieldName = (headerText === 'Ticker') ? 'ticker' : headerText;
            const meta = fieldMetadata[fieldName];
            const numericFormat = fieldNumericFormats[fieldName] || 'default';
            const colDef = { targets: index };

            if (meta && meta.type === 'numeric') {
                colDef.type = 'num'; // Helps DataTables sorting
                colDef.render = function (data, type, row) {
                    if (type === 'display') {
                        // Use the passed formatter for display
                        return formatNumericValueFunction(data, numericFormat);
                    }
                    // For sorting, filtering, type, etc., return the raw data
                    return data; 
                };
            } else if (meta && meta.type === 'date') {
                colDef.type = 'date'; // Helps DataTables date sorting
                colDef.render = function (data, type, row) {
                    if (type === 'display' && data) {
                        try {
                            // Attempt to format as date if it's a recognizable date string/number
                            const date = new Date(data);
                            if (!isNaN(date.getTime())) {
                                // Format as YYYY-MM-DD, adjust as needed
                                return date.toISOString().split('T')[0]; 
                            }
                        } catch (e) { /* Ignore formatting errors */ }
                    }
                    return data; // Return original for sorting/filtering
                };
            } else {
                // Default for strings or other types
                colDef.type = 'string';
                colDef.render = function (data, type, row) {
                     // Basic handling for null/undefined
                     return (data === null || data === undefined) ? '' : data;
                };
            }
            return colDef;
        });
        console.log(`[PreTransformTable] Prepared ${columnDefs.length} column definitions.`);

        // 4. Initialize DataTable
        try {
            console.log("[PreTransformTable] Initializing new DataTable instance...");
            dataTableInstance = $(tableElement).DataTable({
                data: tableData,
                columns: headers.map(header => ({ title: header })), // Set column titles
                columnDefs: columnDefs,
                // Standard options - adjust as needed
                destroy: true, // Ensure cleanup (redundant with manual destroy, but safe)
                paging: true, // Enable pagination
                pageLength: 10, // Set records per page to 10
                lengthChange: false, // Hide 'Show X entries' dropdown
                searching: true,
                info: true,
                // Consider adding scrollX/scrollY if table is wide/long
                // scrollX: true, 
                // scrollY: '50vh', // Example: 50% of viewport height
                // scrollCollapse: true, 
                // Add Buttons if needed (e.g., Copy, CSV)
                // dom: 'Bfrtip', 
                // buttons: [ 'copy', 'csv' ] 
            });
            console.log("[PreTransformTable] DataTable initialized successfully.");

            // <<< ADDED: Hover Highlighting Logic >>>
            $(tableElement).on('mouseover', 'tbody td', function() {
                if (!dataTableInstance) return;
                let colIdx = dataTableInstance.cell(this).index().column;
                let rowIdx = dataTableInstance.cell(this).index().row;

                // Add classes to the current column and row
                $(dataTableInstance.rows(rowIdx).nodes()).addClass('row-highlight');
                $(dataTableInstance.column(colIdx).nodes()).addClass('col-highlight');
                // Specifically highlight the hovered cell intersection
                $(dataTableInstance.cell({row: rowIdx, column: colIdx}).node()).addClass('col-highlight'); // Ensure intersection is highlighted
            });

            $(tableElement).on('mouseout', 'tbody td', function() {
                if (!dataTableInstance) return;
                // <<< REVISED LOGIC START >>>
                // Get the index of the cell being left
                let cell = dataTableInstance.cell(this);
                if (!cell) return; // Exit if cell is not valid
                let colIdx = cell.index().column;
                let rowIdx = cell.index().row;

                // Remove highlight ONLY from the row and column being left
                $(dataTableInstance.row(rowIdx).node()).removeClass('row-highlight');
                $(dataTableInstance.column(colIdx).nodes()).removeClass('col-highlight');
                // <<< REVISED LOGIC END >>>
            });
            // <<< END: Hover Highlighting Logic >>>

            // <<< ADDED: Move Pagination Controls >>>
            const tableWrapper = $(tableElement).closest('.table-responsive');
            const paginationControls = $('#pre-transform-data-set-table_paginate');
            if (tableWrapper.length && paginationControls.length) {
                console.log("[PreTransformTable] Moving pagination controls outside of .table-responsive.");
                paginationControls.insertAfter(tableWrapper);
                // Optional: Add some margin to the moved controls
                paginationControls.addClass('mt-2'); 
            } else {
                console.warn("[PreTransformTable] Could not find table wrapper (.table-responsive) or pagination controls (#pre-transform-data-set-table_paginate) to move them.");
            }
            // <<< END: Move Pagination Controls >>>

        } catch (error) {
            console.error("[PreTransformTable] Error initializing DataTable:", error);
            // Optionally clear the tableElement or show a user-friendly error
            tableElement.innerHTML = '<thead></thead><tbody><tr><td>Error initializing table. Check console.</td></tr></tbody>';
        }
    }

    // Define the public interface object
    const publicInterface = {
        init: init,
        updateTable: updateTable
    };

    // --- Assign to window INSIDE the IIFE ---
    console.log('!!! [IIFE] About to assign publicInterface to window.AnalyticsPreTransformTableModule. Current value:', window.AnalyticsPreTransformTableModule);
    window.AnalyticsPreTransformTableModule = publicInterface;
    console.log('!!! [IIFE] Assigned publicInterface to window.AnalyticsPreTransformTableModule. New value:', window.AnalyticsPreTransformTableModule);
    // --- End Assignment ---

    // No explicit return needed from the outer IIFE itself

})(); // Immediately execute the outer IIFE

// Initialize the module when the DOM is ready (listener remains the same)
document.addEventListener('DOMContentLoaded', () => {
    if (window.AnalyticsPreTransformTableModule) {
        // Log what we find *during* DOMContentLoaded init
        console.log('[DOMContentLoaded] Found window.AnalyticsPreTransformTableModule, calling init(). Module:', window.AnalyticsPreTransformTableModule);
        window.AnalyticsPreTransformTableModule.init();
    } else {
        console.error('[DOMContentLoaded] window.AnalyticsPreTransformTableModule NOT FOUND!');
    }
});

// Final check log remains
console.log('!!! AnalyticsPreTransformTableModule script execution finished. Final value on window:', window.AnalyticsPreTransformTableModule); 