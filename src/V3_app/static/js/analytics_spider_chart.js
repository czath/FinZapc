// Placeholder for spider chart logic

document.addEventListener('DOMContentLoaded', function() {
    console.log("analytics_spider_chart.js loaded");

    // Get references to UI elements within the Cross-Field Visualization tab
    const spiderTabPane = document.getElementById('cross-field-viz-tab-pane');
    if (!spiderTabPane) {
        console.warn('Spider chart tab pane (#cross-field-viz-tab-pane) not found. Aborting script.');
        return;
    }

    // Filter Elements (Added in Step 1)
    const filterControlsContainer = spiderTabPane.querySelector('#spider-filter-controls-container');
    const addFilterBtn = spiderTabPane.querySelector('#spider-add-filter-btn');
    const applyFiltersBtn = spiderTabPane.querySelector('#spider-apply-filters-btn');
    const resetFiltersBtn = spiderTabPane.querySelector('#spider-reset-filters-btn');
    const filterResultsCount = spiderTabPane.querySelector('#spider-filter-results-count');

    // Chart Setting Elements
    const tickerSelect = spiderTabPane.querySelector('#spider-ticker-select');
    const fieldSelect = spiderTabPane.querySelector('#spider-field-select');
    const avgCheckbox = spiderTabPane.querySelector('#spider-agg-avg');
    const minCheckbox = spiderTabPane.querySelector('#spider-agg-min');
    const maxCheckbox = spiderTabPane.querySelector('#spider-agg-max');
    const medianCheckbox = spiderTabPane.querySelector('#spider-agg-median');
    const generateBtn = spiderTabPane.querySelector('#generate-spider-chart-btn');
    const canvas = spiderTabPane.querySelector('#spider-chart-canvas');
    const statusDiv = spiderTabPane.querySelector('#spider-chart-status');
    const crossFieldVizTabTrigger = document.getElementById('cross-field-viz-tab'); // Tab button itself
    
    // State Variables
    let spiderChartInstance = null;
    let mainModule = null; // To store reference to AnalyticsMainModule
    let spiderChartFilters = []; // <<< STEP 2: State for ticker filters
    const SPIDER_FILTER_STORAGE_KEY = 'analyticsSpiderChartFilters'; // <<< STEP 2: Storage key
    const TEXT_FILTER_DROPDOWN_THRESHOLD = 30; // <<< Borrowed from analytics.js
    let isSpiderModuleInitialized = false; // <<< Flag to prevent multiple initializations

    // --- Helper: Median Calculation ---
    function calculateMedian(numericValues) {
        if (!numericValues || numericValues.length === 0) {
            return null;
        }
        const sortedValues = [...numericValues].sort((a, b) => a - b);
        const mid = Math.floor(sortedValues.length / 2);
        return sortedValues.length % 2 !== 0 ? sortedValues[mid] : (sortedValues[mid - 1] + sortedValues[mid]) / 2;
    }

    // --- STEP 2: Filter Persistence Functions ---
    function loadSpiderFiltersFromStorage() {
        console.log("[Spider Filter] Loading filters from localStorage...");
        const savedFilters = localStorage.getItem(SPIDER_FILTER_STORAGE_KEY);
        let loaded = [];
        if (savedFilters) {
            try {
                loaded = JSON.parse(savedFilters);
                 // console.log("[Spider Filter load] Parsed from localStorage:", JSON.parse(JSON.stringify(loaded))); // DEBUG REMOVED
                if (!Array.isArray(loaded)) loaded = [];
            } catch (e) {
                console.error("[Spider Filter] Error parsing saved filters:", e);
                loaded = [];
                localStorage.removeItem(SPIDER_FILTER_STORAGE_KEY);
            }
        } else {
            // console.log("[Spider Filter] No saved filters found."); // DEBUG REMOVED
        }

        // Ensure structure (id, field, operator, value, comment)
        spiderChartFilters = loaded.map(f => ({
            id: f.id || Date.now() + Math.random(),
            field: f.field || '',
            operator: f.operator || '=',
            value: f.value !== undefined ? f.value : '',
            comment: f.comment || '' 
        }));
        // console.log("[Spider Filter load] Processed loaded/default filters:", spiderChartFilters); // DEBUG REMOVED

        // Add default blank filter if none loaded
        if (spiderChartFilters.length === 0) {
            spiderChartFilters.push({ id: Date.now() + Math.random(), field: '', operator: '=', value: '', comment: '' }); 
            // console.log("[Spider Filter] Added default blank filter."); // DEBUG REMOVED
        }
    }

    function saveSpiderFiltersToStorage() {
         if (!Array.isArray(spiderChartFilters)) {
             console.error("[Spider Filter] Attempted to save non-array filters:", spiderChartFilters);
             return;
         }
        // Save id, field, operator, value, comment
        const filtersToSave = spiderChartFilters.map(f => ({
             id: f.id,
             field: f.field,
             operator: f.operator,
             value: f.value,
             comment: f.comment
         }));
        // console.log("[Spider Filter] Saving filters to localStorage:", filtersToSave); // DEBUG REMOVED
        try {
             localStorage.setItem(SPIDER_FILTER_STORAGE_KEY, JSON.stringify(filtersToSave));
        } catch (e) {
            console.error("[Spider Filter] Error saving filters to localStorage:", e);
            // Optionally update statusDiv here
        }
    }
    // --- END STEP 2 --- 

    // --- Initialization Logic --- 
    function initializeSpiderChartModule() {
        // <<< Prevent multiple initializations >>>
        if (isSpiderModuleInitialized) {
            // console.log("Spider Chart Module already initialized. Skipping."); // DEBUG REMOVED
            return;
        }
        isSpiderModuleInitialized = true; // Set flag
        console.log("Initializing Spider Chart Module (triggered by AnalyticsDataReady)...", performance.now()); // Log timing
        
        mainModule = window.AnalyticsMainModule;
        if (!mainModule) {
            console.error("AnalyticsMainModule not found! Cannot populate spider chart selectors or data.");
            if(statusDiv) statusDiv.textContent = 'Error: Core analytics module not loaded.';
            return;
        }
        loadSpiderFiltersFromStorage(); // <<< STEP 2: Load filters on init
        renderSpiderFilterUI(); // <<< STEP 3: Render initial filter UI
        populateSelectors(); // Populate chart selectors
        applySpiderFilters(); // <<< STEP 6: Apply initial filters to populate ticker list
        setupFilterEventListeners(); // <<< STEP 3: Attach listeners for filter buttons
    }

    // Listener for when the tab becomes active
    if (crossFieldVizTabTrigger) {
        crossFieldVizTabTrigger.addEventListener('shown.bs.tab', function (event) {
            // console.log('Cross-Field Visualization tab shown.'); // DEBUG REMOVED
            // <<< REMOVE initialization call from here >>>
            // initializeSpiderChartModule(); 
            
            // <<< ADDED: Re-render chart if already initialized when tab becomes visible >>>
            if (isSpiderModuleInitialized && spiderChartInstance) {
                // console.log("Tab shown again, forcing chart redraw/resize."); // DEBUG REMOVED
                // Might need resize logic if canvas size changes on tab visibility
                setTimeout(() => { // Use timeout to ensure canvas is visible
                   spiderChartInstance.resize(); 
                   spiderChartInstance.update();
                }, 0);
            }
        });
    } else {
        console.warn('Cross-Field Visualization tab trigger (#cross-field-viz-tab) not found.');
    }

    // --- Listener for Main Module Data Readiness --- 
    console.log("Spider Chart Script: Setting up listener for AnalyticsDataReady event...");
    window.addEventListener('AnalyticsDataReady', initializeSpiderChartModule);
    // --- Removed immediate check and old listener --- 

    // --- Function to populate selectors (Chart controls, not filters) ---
    function populateSelectors() {
        // console.log("Populating spider chart controls (Fields)..."); // DEBUG REMOVED
        if (!mainModule || !fieldSelect) { // Removed tickerSelect check here
            console.error("Cannot populate field selector: Main module or field select element missing.");
            return;
        }

        // Ticker selector population is now handled by applySpiderFilters

        // Populate Field Select (only numeric fields)
        const finalFields = mainModule.getFinalAvailableFields ? mainModule.getFinalAvailableFields() : [];
        const finalMetadata = mainModule.getFinalFieldMetadata ? mainModule.getFinalFieldMetadata() : {};
        fieldSelect.innerHTML = ''; // Clear existing
        const numericFields = finalFields.filter(field => finalMetadata[field]?.type === 'numeric');
        if (numericFields.length > 0) {
            numericFields.sort().forEach(field => {
                const option = document.createElement('option');
                option.value = field;
                option.textContent = field;
                fieldSelect.appendChild(option);
            });
        } else {
            fieldSelect.innerHTML = '<option disabled>No numeric fields found</option>';
        }
        // console.log("Field selector populated."); // DEBUG REMOVED
    }

    // --- STEP 3: Filter UI Rendering --- 
    function renderSpiderFilterUI() {
        // console.log("[Spider Filter] Rendering filter UI..."); // DEBUG REMOVED
        if (!filterControlsContainer || !mainModule) {
            console.error("[Spider Filter] Filter container or main module not found.");
            return;
        }
        filterControlsContainer.innerHTML = ''; // Clear existing rows

        const finalFields = mainModule.getFinalAvailableFields ? mainModule.getFinalAvailableFields() : [];
        const finalMetadata = mainModule.getFinalFieldMetadata ? mainModule.getFinalFieldMetadata() : {};

        if (!finalFields || finalFields.length === 0) {
             filterControlsContainer.innerHTML = '<p class="text-muted small mb-1">Load data first to define filters.</p>';
            return;
        }

        if (!spiderChartFilters || spiderChartFilters.length === 0) {
            filterControlsContainer.innerHTML = '<p class="text-muted small mb-1">No filters defined.</p>';
            return;
        }

        const operators = [
            { value: '=', text: '=' },
            { value: '>', text: '>' },
            { value: '<', text: '<' },
            { value: '>=', text: '>=' },
            { value: '<=', text: '<=' },
            { value: '!=', text: '!=' },
            { value: 'contains', text: 'contains' },
            { value: 'startsWith', text: 'starts with' },
            { value: 'endsWith', text: 'ends with' },
            { value: 'exists', text: 'exists (non-empty)'}, 
            { value: 'notExists', text: 'does not exist / empty'}
        ];

        spiderChartFilters.forEach((filter, index) => {
            const filterId = filter.id;
            const row = document.createElement('div');
            row.className = 'filter-row-container mb-2'; 
            row.dataset.filterId = filterId;

            const filterRowDiv = document.createElement('div');
            filterRowDiv.className = 'd-flex align-items-center filter-row'; 

            // Field Select (Use ALL final fields)
            const fieldSelectElement = document.createElement('select');
            fieldSelectElement.className = 'form-select form-select-sm me-2 w-auto';
            fieldSelectElement.title = 'Select Field';
            fieldSelectElement.innerHTML = '<option value="">-- Field --</option>';
            finalFields.forEach(fieldName => {
                const option = document.createElement('option');
                option.value = fieldName;
                option.textContent = fieldName;
                if (filter.field === fieldName) option.selected = true;
                fieldSelectElement.appendChild(option);
            });
            filterRowDiv.appendChild(fieldSelectElement);

            // Operator Select
            const operatorSelect = document.createElement('select');
            operatorSelect.className = 'form-select form-select-sm me-2 w-auto';
            operatorSelect.title = 'Select Operator';
            operators.forEach(op => {
                const option = document.createElement('option');
                option.value = op.value;
                option.textContent = op.text;
                if (filter.operator === op.value) option.selected = true;
                operatorSelect.appendChild(option);
            });
             filterRowDiv.appendChild(operatorSelect);

            // Value Input Wrapper
            const valueWrapper = document.createElement('div');
            valueWrapper.className = 'value-input-wrapper me-2';
            valueWrapper.style.flexGrow = '0'; 
            valueWrapper.style.flexShrink = '0';
            valueWrapper.style.maxWidth = '150px'; // Smaller max width for this context
            valueWrapper.style.width = '150px'; 
            filterRowDiv.appendChild(valueWrapper);
            
            // Comment Input (Removed for space in this simpler filter context, could be added back)
            // const commentInput = ...

            // Hint Span (Minimal)
            const hintSpan = document.createElement('span');
            hintSpan.className = 'value-hint small text-muted d-block ms-1';
            hintSpan.style.fontSize = '0.75em'; // Make hint smaller
            hintSpan.style.lineHeight = '1.2';

            // Remove Button
            const removeBtn = document.createElement('button');
            removeBtn.innerHTML = '&times;'; // Use HTML entity for X
            removeBtn.className = 'btn btn-sm btn-outline-danger ms-auto py-0 px-1'; // Compact button
            removeBtn.title = 'Remove this filter';
            removeBtn.style.lineHeight = '1'; // Adjust line height
            removeBtn.addEventListener('click', () => {
                 const indexToRemove = spiderChartFilters.findIndex(f => f.id === filterId);
                 if (indexToRemove > -1) {
                     spiderChartFilters.splice(indexToRemove, 1);
                     // saveSpiderFiltersToStorage(); // Optional: save on remove, or wait for Apply
                     renderSpiderFilterUI();
                 }
             });
            filterRowDiv.appendChild(removeBtn);

            // --- Event Listeners --- 
            fieldSelectElement.addEventListener('change', (e) => {
                const indexToUpdate = spiderChartFilters.findIndex(f => f.id === filterId);
                if (indexToUpdate > -1) {
                    spiderChartFilters[indexToUpdate].field = e.target.value;
                    // Pass the finalMetadata specific to this module
                    updateSpiderValueInputUI(indexToUpdate, e.target.value, valueWrapper, hintSpan, finalMetadata);
                }
            });
             operatorSelect.addEventListener('change', (e) => {
                 const indexToUpdate = spiderChartFilters.findIndex(f => f.id === filterId);
                 if (indexToUpdate > -1) {
                     spiderChartFilters[indexToUpdate].operator = e.target.value;
                     const op = e.target.value;
                     valueWrapper.style.display = (op === 'exists' || op === 'notExists') ? 'none' : '';
                     hintSpan.style.display = (op === 'exists' || op === 'notExists') ? 'none' : '';
                 }
            });
            // Value input listener added in updateSpiderValueInputUI

            // Initial UI update for value input
            const currentIndex = spiderChartFilters.findIndex(f => f.id === filterId);
            if (currentIndex > -1) {
                // Pass the finalMetadata specific to this module
                updateSpiderValueInputUI(currentIndex, filter.field, valueWrapper, hintSpan, finalMetadata);
            }

            row.appendChild(filterRowDiv);
            row.appendChild(hintSpan);
            filterControlsContainer.appendChild(row);
        });
    }

    // --- STEP 3 Helper: Update Value Input UI --- 
    function updateSpiderValueInputUI(index, fieldName, inputWrapper, hintSpan, metadataSource) {
        const metadata = metadataSource[fieldName];
        // console.log(`[Spider Filter UI] Updating value input for filter ${index}, field '${fieldName}'. Metadata found: ${!!metadata}`); // DEBUG REMOVED
        
        inputWrapper.innerHTML = ''; // Clear previous input/select
        const filterValueForUI = spiderChartFilters[index]?.value;
        // console.log(`[Spider Filter UI] Rendering value input. Loaded Value:`, filterValueForUI); // DEBUG REMOVED

        // Set minimal hint text (field type)
        hintSpan.textContent = metadata ? `(${metadata.type})` : '(?) '; // Show type or ?

        // Define a common handler for updating the filter state
        const updateFilterValue = (newValue) => {
            if (spiderChartFilters[index]) { 
                spiderChartFilters[index].value = newValue;
                // console.log(`[Spider Filter State] Filter ${index} value updated:`, newValue); // Debug log
            }
        };

        // Create input/select based on metadata (Simplified version of analytics.js logic)
        if (metadata && metadata.type === 'text' && metadata.uniqueValues && metadata.uniqueValues.length > 0
            && metadata.uniqueValues.length <= TEXT_FILTER_DROPDOWN_THRESHOLD) {
            // --- Create Multi-Select (Only if below threshold) ---
            const select = document.createElement('select');
            select.multiple = true;
            select.className = 'form-select form-select-sm w-100';
            select.size = Math.min(metadata.uniqueValues.length, 3); // Limit size

            metadata.uniqueValues.forEach(val => {
                const option = document.createElement('option');
                option.value = val;
                option.textContent = val;
                if (spiderChartFilters[index] && Array.isArray(spiderChartFilters[index].value) && spiderChartFilters[index].value.includes(val)) {
                    option.selected = true;
                }
                select.appendChild(option);
            });
            select.addEventListener('change', (e) => {
                const selectedValues = Array.from(e.target.selectedOptions).map(opt => opt.value);
                updateFilterValue(selectedValues); 
            });
            inputWrapper.appendChild(select);
        } else {
             // --- Create Text/Number Input (Fallback) ---
            const input = document.createElement('input');
            input.className = 'form-control form-control-sm';
            input.placeholder = 'Value';
            let initialValue = '';
            if (spiderChartFilters[index]) {
                const filterVal = spiderChartFilters[index].value;
                initialValue = Array.isArray(filterVal) ? '' : (filterVal !== null && filterVal !== undefined ? String(filterVal) : '');
            }
            input.value = initialValue;

            if (metadata && metadata.type === 'numeric') {
                input.type = 'number';
                input.step = 'any';
                hintSpan.textContent = '(numeric)'; // Simple hint
            } else {
                input.type = 'text';
                hintSpan.textContent = '(text)'; // Simple hint
                // Add datalist for text fields with many options
                if (metadata && metadata.type === 'text' && metadata.uniqueValues && metadata.uniqueValues.length > 0) {
                    const datalistId = `spider-datalist-${index}-${fieldName.replace(/\W/g, '')}`;
                    input.setAttribute('list', datalistId);
                    const datalist = document.createElement('datalist');
                    datalist.id = datalistId;
                    metadata.uniqueValues.forEach(val => {
                        const option = document.createElement('option');
                        option.value = val;
                        datalist.appendChild(option);
                    });
                    inputWrapper.appendChild(datalist);
                }
            }
            if (metadata && metadata.type === 'empty') {
                hintSpan.textContent = '(empty)';
            }

            // Use 'input' event for text/number fields
            input.addEventListener('input', (e) => {
                 updateFilterValue(e.target.value);
            });
            inputWrapper.appendChild(input);
        }

        // Hide/Show based on operator
        if (spiderChartFilters[index]) {
            const operator = spiderChartFilters[index].operator;
            const shouldHide = operator === 'exists' || operator === 'notExists';
            inputWrapper.style.display = shouldHide ? 'none' : '';
            hintSpan.style.display = shouldHide ? 'none' : '';
        }
    }
    // --- END STEP 3 --- 

    // --- STEP 3: Filter Button Event Listeners ---
    function setupFilterEventListeners() {
        if (addFilterBtn) {
            addFilterBtn.addEventListener('click', () => {
                // console.log("[Spider Filter] Add Filter clicked"); // DEBUG REMOVED
                spiderChartFilters.push({ id: Date.now() + Math.random(), field: '', operator: '=', value: '', comment: '' });
                renderSpiderFilterUI();
            });
        }

        if (resetFiltersBtn) {
            resetFiltersBtn.addEventListener('click', () => {
                // console.log("[Spider Filter] Reset Filters clicked"); // DEBUG REMOVED
                spiderChartFilters = [{ id: Date.now() + Math.random(), field: '', operator: '=', value: '', comment: '' }];
                saveSpiderFiltersToStorage(); // Save the reset state
                renderSpiderFilterUI();
                // applySpiderFilters(); // TODO: Call apply (Step 4) to reset ticker list
                if(filterResultsCount) filterResultsCount.textContent = ''; // Clear count display
                // Need to repopulate ticker list with all tickers here
                if (mainModule) {
                    const fullData = mainModule.getFinalDataForAnalysis ? mainModule.getFinalDataForAnalysis() : [];
                    const allTickers = [...new Set(fullData.map(item => item?.ticker).filter(Boolean))].sort();
                    updateTickerSelector(allTickers);
                } 
            });
        }

        if (applyFiltersBtn) {
            applyFiltersBtn.addEventListener('click', () => {
                // console.log("[Spider Filter] Apply Filters clicked"); // DEBUG REMOVED
                saveSpiderFiltersToStorage(); // Save current filter definitions
                applySpiderFilters(); // <<< STEP 4: Call apply function
            });
        }
    }
    // --- END STEP 3 ---

    // --- STEP 4: Function applySpiderFilters() ---
    function applySpiderFilters() {
        // console.log("[Spider Filter] Applying filters to ticker list..."); // DEBUG REMOVED
        if (!mainModule) {
            console.error("[Spider Filter Apply] Main module not available.");
            if(filterResultsCount) filterResultsCount.textContent = 'Error!';
            return;
        }
        // <<< STEP 2.1: Get access to post-transform module (optional) >>>
        const postTransformModule = window.AnalyticsPostTransformModule;

        const fullData = mainModule.getFinalDataForAnalysis ? mainModule.getFinalDataForAnalysis() : [];
        const finalMetadata = mainModule.getFinalFieldMetadata ? mainModule.getFinalFieldMetadata() : {};
        // <<< STEP 2.2: Get access to format functions from main module >>>
        const preTransformFormats = mainModule.getNumericFieldFormats ? mainModule.getNumericFieldFormats() : {};
        // const parseFormattedValueFn = mainModule.parseFormattedValue; // <<< GET PARSER FROM GETTER
        const parseFormattedValueFn = mainModule?.parseFormattedValue; // <<< REVERT: Access directly

        if (!parseFormattedValueFn) {
            // console.error("[Spider Filter Apply] parseFormattedValue function not found on main module via getter!"); // DEBUG REMOVED
            console.error("[Spider Filter Apply] parseFormattedValue function not found directly on main module!"); // Corrected error message
             if(filterResultsCount) filterResultsCount.textContent = 'Error!';
            return;
        }

        if (!fullData || fullData.length === 0) {
            console.log("[Spider Filter Apply] No data loaded."); // Keep this info log
            if(filterResultsCount) filterResultsCount.textContent = '(0 matching)';
            updateTickerSelector([]); // Update selector with empty list (Step 5)
            return;
        }

        const activeFilters = spiderChartFilters.filter(f => f.field && f.field !== '' && f.operator); // Get valid, defined filters
        let filteredData = fullData;

        if (activeFilters.length > 0) {
            // console.log(`[Spider Filter Apply] Applying ${activeFilters.length} active filters...`); // DEBUG REMOVED
            filteredData = fullData.filter(item => {
                if (!item) return false;

                for (const filter of activeFilters) {
                    // --- Get item value (check ticker, source, then processed_data) ---
                    let itemValue = null;
                    if (filter.field === 'ticker') {
                        itemValue = item.ticker;
                    } else if (filter.field === 'source') { // If source becomes available later
                        itemValue = item.source;
                    } else if (item.processed_data && item.processed_data.hasOwnProperty(filter.field)) {
                         itemValue = item.processed_data[filter.field];
                    } else if (item.hasOwnProperty(filter.field)) { // Check top-level too, just in case
                        itemValue = item[filter.field];
                    }
                    // --- End Get item value ---

                    const filterValue = filter.value;
                    const operator = filter.operator;
                    const fieldMeta = finalMetadata[filter.field] || {};
                    const isNumericField = fieldMeta.type === 'numeric';

                    // --- Multi-Select Handling (for text fields rendered as multi-select) ---
                    if (Array.isArray(filterValue)) {
                        const itemValueStr = String(itemValue ?? ''); // Handle null/undefined item values
                        if (operator === '=') {
                            if (!filterValue.includes(itemValueStr)) return false;
                        } else if (operator === '!=') {
                            if (filterValue.includes(itemValueStr)) return false;
                        } else {
                            // console.warn(`[Spider Filter Apply] Operator '${operator}' not supported for multi-select field '${filter.field}'.`);
                            return false; // Or handle differently?
                        }
                        continue; // Go to next filter if multi-select check passed
                    }
                    // --- End Multi-Select Handling ---

                    // --- Single Value Handling (Exists/Not Exists) ---
                    const valueExists = !(itemValue === null || itemValue === undefined || String(itemValue).trim() === '' || String(itemValue).trim() === '-');

                    if (operator === 'exists') {
                        if (!valueExists) return false;
                        continue; 
                    }
                    if (operator === 'notExists') {
                        if (valueExists) return false; 
                        continue; 
                    }
                    // --- End Exists/Not Exists ---

                    // --- Handling for Comparisons when Item Value is Missing ---
                    if (!valueExists) {
                         if ((operator === '=' || operator === '!=') && filterValue === '') {
                             const comparisonResult = (operator === '=') ? !valueExists : valueExists;
                             if (!comparisonResult) return false;
                             continue; 
                         } else {
                             return false; 
                         }
                    }
                    // --- End Missing Value Handling ---

                    // --- Value Comparisons (Numeric and Text) ---
                    const filterValueStr = String(filterValue || ''); 
                    const itemValueStr = String(itemValue).toLowerCase(); // Use lowercase for text comparisons
                    const filterValueLower = filterValueStr.toLowerCase();

                    let comparisonResult = false;
                    if (isNumericField) {
                        const itemNum = Number(itemValue); // Raw item number is fine
                        
                        // <<< STEP 2.3: Determine field format >>>
                        let fieldFormat = 'default';
                        if (postTransformModule && typeof postTransformModule.getPostTransformNumericFormats === 'function') {
                             const postFormats = postTransformModule.getPostTransformNumericFormats();
                             if (postFormats && postFormats.hasOwnProperty(filter.field)) {
                                 fieldFormat = postFormats[filter.field];
                             }
                         }
                         // Fallback to pre-transform format if not found in post-transform
                         if (fieldFormat === 'default' && preTransformFormats && preTransformFormats.hasOwnProperty(filter.field)) {
                             fieldFormat = preTransformFormats[filter.field];
                         }

                        // Allow comparing numeric field with empty string filter
                        if ((operator === '=' || operator === '!=') && filterValueStr === '') {
                             comparisonResult = (operator === '=') ? false : true; // Numeric field can't equal empty string
                        } else {
                            // <<< STEP 2.4: Parse filter input using format >>>
                            const filterNum = parseFormattedValueFn(filterValueStr, fieldFormat);
                            
                            // <<< STEP 2.5: Validate parse result >>>
                            if (isNaN(filterNum)) {
                                console.warn(`[Spider Filter Apply] Skipping filter: Could not parse input '${filterValueStr}' for field '${filter.field}' with format '${fieldFormat}'.`); // Keep warning
                                comparisonResult = false; // Treat unparseable input as non-match
                            } else if (!isNaN(itemNum)) { // Check if item number is valid before comparing
                                 // <<< STEP 2.6: Compare raw itemNum with parsed filterNum >>>
                                 switch (operator) {
                                    case '=': comparisonResult = (itemNum === filterNum); break;
                                    case '>': comparisonResult = (itemNum > filterNum); break;
                                    case '<': comparisonResult = (itemNum < filterNum); break;
                                    case '>=': comparisonResult = (itemNum >= filterNum); break;
                                    case '<=': comparisonResult = (itemNum <= filterNum); break;
                                    case '!=': comparisonResult = (itemNum !== filterNum); break;
                                    default: comparisonResult = false; // Unsupported numeric operator?
                                }
                            } else {
                                // Handle case where item value isn't numeric (should be rare if metadata is correct)
                                comparisonResult = false; 
                            }
                        }
                    } else { // Text comparison (remains the same)
                        switch(operator) {
                            case '=': comparisonResult = (itemValueStr === filterValueLower); break;
                            case '!=': comparisonResult = (itemValueStr !== filterValueLower); break;
                            case 'contains': comparisonResult = itemValueStr.includes(filterValueLower); break;
                            case 'startsWith': comparisonResult = itemValueStr.startsWith(filterValueLower); break;
                            case 'endsWith': comparisonResult = itemValueStr.endsWith(filterValueLower); break;
                            default: comparisonResult = false; // Unsupported text operator
                        }
                    }

                    if (!comparisonResult) {
                        return false; // If any filter fails, the item is excluded
                    }
                    // --- End Value Comparisons ---
                } // End loop through filters for one item
                return true; // Item passes all filters
            });
            // console.log(`[Spider Filter Apply] Filtering complete. ${filteredData.length} items match.`); // DEBUG REMOVED
        } else {
             // console.log(`[Spider Filter Apply] No active filters. Using all ${fullData.length} items.`); // DEBUG REMOVED
        }

        // Extract unique tickers from the filtered data
        const filteredTickers = [...new Set(filteredData.map(item => item?.ticker).filter(Boolean))].sort();
        // console.log(`[Spider Filter Apply] Found ${filteredTickers.length} unique tickers matching filters.`); // DEBUG REMOVED

        // Update results count display
        if (filterResultsCount) {
            filterResultsCount.textContent = `${filteredTickers.length} ticker${filteredTickers.length !== 1 ? 's' : ''} matching.`;
        }

        // Update the ticker selector dropdown
        updateTickerSelector(filteredTickers); // Call Step 5 function
    }
    // --- END STEP 4 ---

    // --- STEP 5: Function updateTickerSelector(filteredTickers) ---
    function updateTickerSelector(tickers) {
        if (!tickerSelect) {
            console.error("[Spider Filter] Ticker select element not found (#spider-ticker-select).");
            return;
        }
        // console.log(`[Spider Filter] Updating ticker selector with ${tickers.length} tickers.`); // DEBUG REMOVED
        
        // Preserve selected values
        const previouslySelected = Array.from(tickerSelect.selectedOptions).map(opt => opt.value);
        
        tickerSelect.innerHTML = ''; // Clear existing options

        if (!tickers || tickers.length === 0) {
            tickerSelect.innerHTML = '<option disabled>No tickers match filters</option>';
        } else {
            tickers.forEach(ticker => {
                const option = document.createElement('option');
                option.value = ticker;
                option.textContent = ticker;
                // Re-select if it was selected before and still exists
                if (previouslySelected.includes(ticker)) {
                    option.selected = true;
                }
                tickerSelect.appendChild(option);
            });
        }
    }
    // --- END STEP 5 --- 

    // --- Function to calculate aggregates ---
    function calculateAggregates(fullData, selectedFields) {
        console.log("Calculating aggregates for selected fields:", selectedFields); // Keep this log
        if (!fullData || fullData.length === 0 || !selectedFields || selectedFields.length === 0) {
            return { aggregates: {}, overallMinMax: {} };
        }

        const aggregates = {}; // { field: { min, max, sum, count, values } }
        const overallMinMax = {}; // { field: { min, max } }

        selectedFields.forEach(field => {
            aggregates[field] = { min: Infinity, max: -Infinity, sum: 0, count: 0, values: [] };
            overallMinMax[field] = { min: Infinity, max: -Infinity }; // Initialize for normalization

            fullData.forEach(item => {
                // Aggregates should use the final processed data structure
                let value = null;
                if (item?.ticker === 'ticker' && item[field] !== undefined) { // Handle potential top-level ticker data? Less likely
                     value = item[field];
                } else if (item?.processed_data && item.processed_data.hasOwnProperty(field)) {
                     value = item.processed_data[field];
                } 
                
                if (value !== null && value !== undefined ) {
                    const num = Number(value);
                     if (String(value).trim() !== '' && !isNaN(num)) {
                        aggregates[field].values.push(num);
                        aggregates[field].sum += num;
                        aggregates[field].count++;
                        if (num < aggregates[field].min) aggregates[field].min = num;
                        if (num > aggregates[field].max) aggregates[field].max = num;

                        // Update overall min/max for normalization
                        if (num < overallMinMax[field].min) overallMinMax[field].min = num;
                        if (num > overallMinMax[field].max) overallMinMax[field].max = num;
                    }
                }
            });

            // Handle cases where no valid data was found for a field
            if (aggregates[field].count === 0) {
                aggregates[field].min = null;
                aggregates[field].max = null;
                aggregates[field].avg = null;
                aggregates[field].median = null;
                overallMinMax[field].min = null; // No min/max if no data
                overallMinMax[field].max = null;
            } else {
                // Finalize calculations
                aggregates[field].avg = aggregates[field].sum / aggregates[field].count;
                aggregates[field].median = calculateMedian(aggregates[field].values);
                 // If min/max remained at Infinity/-Infinity, set to null or single value
                if (overallMinMax[field].min === Infinity) {
                    overallMinMax[field].min = overallMinMax[field].max !== -Infinity ? overallMinMax[field].max : null;
                }
                 if (overallMinMax[field].max === -Infinity) {
                    overallMinMax[field].max = overallMinMax[field].min !== null ? overallMinMax[field].min : null;
                }
            }
            // Keep .values if median might be checked later during chart generation
            // delete aggregates[field].values; 
        });

        console.log("Aggregates calculated:", aggregates); // Keep this log
        console.log("Overall Min/Max for Normalization:", overallMinMax); // Keep this log
        return { aggregates, overallMinMax };
    }

    // --- Function to normalize data using Min-Max Scaling ---
    function normalizeData(value, min, max) {
        if (value === null || min === null || max === null) return 0; // Or handle as error/default?
        if (max === min) return 0.5; // Avoid division by zero, return middle value
        const normalized = (value - min) / (max - min);
        // Clamp between 0 and 1 in case of floating point issues or outliers beyond calculated min/max
        return Math.max(0, Math.min(1, normalized));
    }

    // --- Function to render the radar chart ---
    function renderSpiderChart(chartLabels, datasets, originalValuesMap) {
        // console.log("Rendering spider chart..."); // DEBUG REMOVED
        if (!canvas) {
            console.error("Spider chart canvas not found!");
            if(statusDiv) statusDiv.textContent = 'Error: Chart canvas element missing.';
            return;
        }
        const ctx = canvas.getContext('2d');
        if (spiderChartInstance) {
            spiderChartInstance.destroy();
            // console.log("Previous spider chart instance destroyed."); // DEBUG REMOVED
        }

        const finalMetadata = window.AnalyticsMainModule?.getFinalFieldMetadata ? window.AnalyticsMainModule.getFinalFieldMetadata() : {};
        const preTransformFormats = window.AnalyticsMainModule?.getNumericFieldFormats ? window.AnalyticsMainModule.getNumericFieldFormats() : {};

        const chartData = {
            labels: chartLabels, // Fields
            datasets: datasets   // Tickers/Aggregates
        };

        spiderChartInstance = new Chart(ctx, {
            type: 'radar',
            data: chartData,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'top',
                    },
                    tooltip: {
                        callbacks: {
                            label: function(tooltipItem) {
                                const datasetIndex = tooltipItem.datasetIndex;
                                const dataIndex = tooltipItem.dataIndex;
                                const datasetLabel = tooltipItem.chart.data.datasets[datasetIndex].label || '';
                                const fieldLabel = tooltipItem.chart.data.labels[dataIndex] || ''; // <<< Get the correct field name (string)

                                // <<< Get main module and try to get formatter via getter >>>
                                const mainModule = window.AnalyticsMainModule;
                                const formatNumericValueFn = (mainModule && typeof mainModule.getFormatter === 'function') 
                                                              ? mainModule.getFormatter()
                                                              : null;

                                // Retrieve the original, non-normalized value using the FIELD NAME
                                let originalValue = null;
                                if (originalValuesMap &&
                                    originalValuesMap[datasetIndex] && // Check dataset index exists
                                    originalValuesMap[datasetIndex].hasOwnProperty(fieldLabel)) { // <<< Check if FIELD NAME exists as key
                                    originalValue = originalValuesMap[datasetIndex][fieldLabel]; // <<< Use fieldLabel (string) as key
                                }

                                // If original value is still not valid, return N/A and log for debugging
                                if (originalValue === null || originalValue === undefined || isNaN(originalValue)) {
                                    // console.warn(`[Spider Tooltip] Original value for ${datasetLabel} - ${fieldLabel} is invalid or not found. Retrieved:`, originalValue, `Map entry for dataset ${datasetIndex}:`, originalValuesMap?.[datasetIndex]); // DEBUG REMOVED
                                    return `${datasetLabel} - ${fieldLabel}: N/A`;
                                }

                                // --- Format the original value using hierarchy ---
                                let formattedValue = originalValue; // Default to raw if formatter fails
                                // <<< DEBUG LOG: Check if formatter was successfully retrieved >>>
                                // console.log(`[Spider Tooltip Debug] Formatter function retrieved: ${typeof formatNumericValueFn === 'function'}`); // DEBUG REMOVED

                                // <<< Check if formatter is a valid function >>>
                                if (typeof formatNumericValueFn === 'function' && finalMetadata && fieldLabel) {
                                    const fieldMeta = finalMetadata[fieldLabel];
                                    // Determine format: Post-Transform > Pre-Transform > Default
                                    let format = 'default';
                                    if (fieldMeta?.postTransformFormat) {
                                        format = fieldMeta.postTransformFormat;
                                    } else if (preTransformFormats[fieldLabel]) {
                                        format = preTransformFormats[fieldLabel];
                                    }
                                    // <<< DEBUG LOG: Log determined format >>>
                                    // console.log(`[Spider Tooltip Debug] Field: ${fieldLabel}, Determined format: ${format}`); // DEBUG REMOVED

                                    try {
                                         // Ensure originalValue is a number before formatting
                                         const numericValue = Number(originalValue);
                                         // <<< DEBUG LOG: Log value before formatting >>>
                                         // console.log(`[Spider Tooltip Debug] Formatting value: ${numericValue} with format: ${format}`); // DEBUG REMOVED

                                         if (!isNaN(numericValue)) {
                                             formattedValue = formatNumericValueFn(numericValue, format);
                                             // <<< DEBUG LOG: Log value AFTER formatting >>>
                                             // console.log(`[Spider Tooltip Debug] Formatted value: ${formattedValue}`); // DEBUG REMOVED
                                         } else {
                                             // Handle cases where original value might not be numeric (though unlikely for spider chart)
                                             formattedValue = String(originalValue);
                                             console.warn(`Tooltip Warning: Original value for ${fieldLabel} (${originalValue}) is not numeric. Displaying as string.`); // Keep warning
                                         }
                                    } catch (error) {
                                        console.error(`Error formatting tooltip value for ${fieldLabel} with format ${format}:`, error);
                                        formattedValue = originalValue; // Fallback to raw value on error
                                    }
                                } else {
                                     // console.warn(`Tooltip Warning: Could not format value for ${fieldLabel}. Missing formatter, metadata, or field label.`); // DEBUG REMOVED
                                }
                                // --- END NEW ---

                                return `${datasetLabel} - ${fieldLabel}: ${formattedValue}`; // Keep debug logs removed
                            }
                        }
                    }
                },
                scales: {
                    r: {
                        angleLines: {
                            display: true
                        },
                        suggestedMin: 0,
                        suggestedMax: 1, // Normalized data is 0-1
                         pointLabels: {
                            font: {
                                size: 11 // Smaller font for point labels if needed
                            }
                        },
                        ticks: {
                           display: false // Hide the radial axis ticks (0, 0.2, ..., 1)
                        }
                    }
                },
                elements: {
                    line: {
                        borderWidth: 2 // Thinner lines
                    },
                    point: {
                        radius: 3, // Smaller points
                        hoverRadius: 5
                    }
                }
            }
        });
        // console.log("Spider chart rendered/updated."); // DEBUG REMOVED
        if (statusDiv) statusDiv.textContent = 'Chart generated successfully.';
    }

    // --- Event Listener for Generate Button ---
    if (generateBtn) {
        generateBtn.addEventListener('click', () => {
            console.log("Generate spider chart button clicked.");
            if (!mainModule) {
                 if(statusDiv) statusDiv.textContent = 'Error: Analytics module not loaded.';
                 return;
            }
            if(statusDiv) statusDiv.textContent = 'Generating chart...';

            // 1. Get selections from UI
            const selectedTickers = Array.from(tickerSelect.selectedOptions).map(opt => opt.value);
            const selectedFields = Array.from(fieldSelect.selectedOptions).map(opt => opt.value);
            const includeAvg = avgCheckbox.checked;
            const includeMin = minCheckbox.checked;
            const includeMax = maxCheckbox.checked;
            const includeMedian = medianCheckbox.checked;

            // Basic Validation
            if (selectedTickers.length < 1) { // Allow 1 ticker + aggregates
                if (statusDiv) statusDiv.textContent = 'Please select at least 1 ticker.';
                return;
            }
             if (selectedTickers.length === 1 && !includeAvg && !includeMin && !includeMax && !includeMedian) {
                if (statusDiv) statusDiv.textContent = 'Please select at least 2 tickers OR 1 ticker and an aggregate.';
                return;
            }
            if (selectedFields.length < 3) {
                if (statusDiv) statusDiv.textContent = 'Please select at least 3 fields (axes).';
                return;
            }

            // 2. Fetch relevant data for tickers from AnalyticsMainModule
            const fullData = mainModule.getFinalDataForAnalysis ? mainModule.getFinalDataForAnalysis() : [];
            if (!fullData || fullData.length === 0) {
                if(statusDiv) statusDiv.textContent = 'Error: No data available to process.';
                return;
            }

            const tickerData = fullData.filter(item => item && selectedTickers.includes(item.ticker));

            // 3. Calculate aggregates for selected fields across ALL data
            const { aggregates, overallMinMax } = calculateAggregates(fullData, selectedFields);

            // 4. Prepare datasets (normalize data)
            const datasets = [];
            const originalValuesMap = {}; // { datasetIndex: { fieldLabel: originalValue } }

            // Add Ticker Datasets
            selectedTickers.forEach(ticker => {
                const item = tickerData.find(d => d.ticker === ticker);
                const normalizedValues = [];
                const originalTickerValues = {}; // Store original values for this ticker

                selectedFields.forEach(field => {
                    let originalValue = null;
                    // Ticker data structure check
                    if (item?.ticker === 'ticker' && item[field] !== undefined) { 
                        originalValue = item[field];
                    } else if (item?.processed_data && item.processed_data.hasOwnProperty(field)) {
                        originalValue = item.processed_data[field];
                    }
                    originalTickerValues[field] = originalValue; // <<< Store value by field name
                    const min = overallMinMax[field]?.min;
                    const max = overallMinMax[field]?.max;
                    normalizedValues.push(normalizeData(originalValue, min, max));
                });
                
                // <<< FIX: Get index *before* pushing >>>
                const currentDatasetIndex = datasets.length;
                datasets.push({ label: ticker, data: normalizedValues });
                originalValuesMap[currentDatasetIndex] = originalTickerValues; // <<< FIX: Use index as key
            });

            // Add Aggregate Datasets if selected
            if (includeAvg) {
                const aggValues = selectedFields.map(field => normalizeData(aggregates[field]?.avg, overallMinMax[field]?.min, overallMinMax[field]?.max));
                const currentDatasetIndex = datasets.length; // <<< FIX: Get index
                datasets.push({ label: 'Average', data: aggValues });
                originalValuesMap[currentDatasetIndex] = Object.fromEntries(selectedFields.map(f => [f, aggregates[f]?.avg])); // <<< FIX: Use index
            }
            if (includeMin) {
                const aggValues = selectedFields.map(field => normalizeData(aggregates[field]?.min, overallMinMax[field]?.min, overallMinMax[field]?.max));
                const currentDatasetIndex = datasets.length; // <<< FIX: Get index
                datasets.push({ label: 'Minimum', data: aggValues });
                 originalValuesMap[currentDatasetIndex] = Object.fromEntries(selectedFields.map(f => [f, aggregates[f]?.min])); // <<< FIX: Use index
            }
             if (includeMax) {
                const aggValues = selectedFields.map(field => normalizeData(aggregates[field]?.max, overallMinMax[field]?.min, overallMinMax[field]?.max));
                const currentDatasetIndex = datasets.length; // <<< FIX: Get index
                datasets.push({ label: 'Maximum', data: aggValues });
                originalValuesMap[currentDatasetIndex] = Object.fromEntries(selectedFields.map(f => [f, aggregates[f]?.max])); // <<< FIX: Use index
            }
            if (includeMedian) {
                const aggValues = selectedFields.map(field => normalizeData(aggregates[field]?.median, overallMinMax[field]?.min, overallMinMax[field]?.max));
                const currentDatasetIndex = datasets.length; // <<< FIX: Get index
                datasets.push({ label: 'Median', data: aggValues });
                originalValuesMap[currentDatasetIndex] = Object.fromEntries(selectedFields.map(f => [f, aggregates[f]?.median])); // <<< FIX: Use index
            }

            // 5. Assign colors dynamically AFTER all datasets are added
            const colorPalette = [
                'rgba(54, 162, 235, 0.6)',  // Blue
                'rgba(255, 99, 132, 0.6)',  // Red
                'rgba(75, 192, 192, 0.6)',  // Green
                'rgba(255, 206, 86, 0.6)',  // Yellow
                'rgba(153, 102, 255, 0.6)', // Purple
                'rgba(255, 159, 64, 0.6)',  // Orange
                'rgba(201, 203, 207, 0.6)', // Grey
                'rgba(0, 0, 0, 0.6)',       // Black
                // Add more distinct colors if needed
                'rgba(255, 0, 255, 0.6)',    // Magenta
                'rgba(0, 255, 255, 0.6)',    // Cyan
                'rgba(0, 128, 0, 0.6)',      // Dark Green
                'rgba(128, 0, 128, 0.6)'     // Dark Purple
            ];
            const borderPalette = colorPalette.map(color => color.replace('0.6', '1'));

            datasets.forEach((ds, index) => {
                ds.backgroundColor = colorPalette[index % colorPalette.length];
                ds.borderColor = borderPalette[index % borderPalette.length];
                ds.pointBackgroundColor = borderPalette[index % borderPalette.length];
                ds.pointBorderColor = '#fff'; // Keep white point border
                ds.pointHoverBackgroundColor = '#fff'; // White background on hover
                ds.pointHoverBorderColor = borderPalette[index % borderPalette.length]; // Use border color on hover
            });

            // 6. Call renderSpiderChart
            renderSpiderChart(selectedFields, datasets, originalValuesMap);
        });
    } else {
        console.error("Generate spider chart button not found!");
    }

}); 