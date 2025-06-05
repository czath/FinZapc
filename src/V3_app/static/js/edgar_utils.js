/**** KEEP THIS COMMENT AT THE TOP OF THE FILE AND PREFIX ALL LOGS WITH [Q4 Filler] ****/
document.addEventListener('DOMContentLoaded', function () {
    let currentDisplayableRawData = null; // Holds the latest fetched raw data (single object or array)
    const edgarCleanupToggle = document.getElementById('edgarCleanupToggle');
    const edgarConceptSearchInput = document.getElementById('edgarConceptSearchInput'); // Get search input
    let edgarDataTableInstance = null; // For DataTables
    const edgar10qFillerToggle = document.getElementById('edgar10qFillerToggle'); // Added: Get 10-Q Filler Toggle
    let edgarBarChartInstance = null; // For the bar chart
    let allFetchedConceptDetails = []; // To store the full list of concepts for re-rendering
    const FAVORITES_STORAGE_KEY = 'edgarGlobalFavoriteConcepts';

    const showEdgarChartBtn = document.getElementById('showEdgarChartBtn');
    const edgarChartModal = document.getElementById('edgarChartModal');

    // Helper function to get favorites from localStorage
    function getFavorites() {
        const favorites = localStorage.getItem(FAVORITES_STORAGE_KEY);
        return favorites ? JSON.parse(favorites) : [];
    }

    // Helper function to save favorites to localStorage
    function saveFavorites(favoritesArray) {
        localStorage.setItem(FAVORITES_STORAGE_KEY, JSON.stringify(favoritesArray));
    }

    // Function to toggle favorite status
    function toggleFavoriteStatus(conceptName) {
        let favorites = getFavorites();
        const index = favorites.indexOf(conceptName);
        if (index > -1) {
            favorites.splice(index, 1); // Remove from favorites
        } else {
            favorites.push(conceptName); // Add to favorites
        }
        saveFavorites(favorites);
        // Re-render the list to reflect changes (including sort order)
        displayConceptList(allFetchedConceptDetails, currentEdgarExportContext.cik); // Pass CIK for label links
    }

    // Delegated event listener for concept list actions (like favorite toggle)
    const conceptListUl = document.getElementById('edgarConceptList');
    if (conceptListUl) {
        conceptListUl.addEventListener('click', function(event) {
            const target = event.target;
            if (target.dataset.action === 'toggle-favorite') {
                event.preventDefault(); // Prevent label click / checkbox toggle
                event.stopPropagation(); // Stop event from bubbling further
                const conceptName = target.dataset.conceptName;
                if (conceptName) {
                    toggleFavoriteStatus(conceptName);
                }
            }
        });
    }

    // Helper function to escape regex special characters
    function escapeRegex(string) {
        return string.replace(/[-\/\\\\^$*+?.()|[\\]{}]/g, '\\\\$&');
    }

    // Attempt to attach export button listener early
    /*
    const exportEdgarCustomTableBtn = document.getElementById('exportEdgarCustomTableBtn');
    if (exportEdgarCustomTableBtn) {
        exportEdgarCustomTableBtn.addEventListener('click', function() {
            if (!edgarDataTableInstance) {
                alert('Table data is not available for export.');
                return;
            }
            const ticker = currentEdgarExportContext.ticker || "SelectedConcepts";
            const today = new Date().toISOString().slice(0, 10);
            const filename = `EDGAR_Custom_${ticker}_${today}.xls`;
            
            // Create a temporary table with display data for export
            const tempTable = document.createElement('table');
            const tempThead = tempTable.createTHead();
            const tempTbody = tempTable.createTBody();
            const headerRow = tempThead.insertRow();
            
            edgarDataTableInstance.columns().every(function() {
                const th = document.createElement('th');
                th.textContent = $(this.header()).text();
                headerRow.appendChild(th);
            });

            edgarDataTableInstance.rows({ search: 'applied' }).data().each(function(rowData) {
                const tr = tempTbody.insertRow();
                edgarDataTableInstance.columns().every(function(colIdx) {
                    const cellData = this.dataSrc() ? rowData[this.dataSrc()] : ''; // Get raw data
                    let displayData = cellData;
                    const column = edgarDataTableInstance.column(colIdx);
                    const renderFunc = column.settings()[0].mRender || column.settings()[0].mData;
                    
                    if (typeof renderFunc === 'function') {
                         // For display rendering, we need the full row object if render func expects it
                        displayData = renderFunc(cellData, 'display', rowData, {col: colIdx, row: 0, settings: column.settings()[0]});
                    } else if (typeof renderFunc === 'string') {
                        displayData = rowData[renderFunc];
                    }

                    // Strip HTML for XLS export from rendered concept column
                    if (colIdx === 0 && typeof displayData === 'string') { // Concept column
                        const tempDiv = document.createElement('div');
                        tempDiv.innerHTML = displayData;
                        displayData = tempDiv.textContent || tempDiv.innerText || "";
                    }
                     // Format numbers for value column explicitly for export
                    if (colIdx === 8 && typeof cellData !== 'undefined') { // Value column (index adjusted from 7 to 8)
                        const numericVal = parseFloat(cellData);
                        if (!isNaN(numericVal)) {
                            displayData = numericVal.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 4 });
                        } else {
                            displayData = cellData;
                        }
                    }
                    const td = tr.insertCell();
                    // Date columns: Date of Filing (4), Start Date (6), End Date (7)
                    if ([4, 6, 7].includes(colIdx)) {
                        td.style.msoNumberFormat = '\\@'; // Force text format for date columns
                    }
                    td.textContent = displayData !== undefined ? displayData : '';
                });
            });
            exportTableToXLSNode(tempTable, filename);
        });
    }
    */

    // NEW: Named function to handle the Edgar Custom Table export logic
    function handleExportEdgarCustomTable() {
        if (!edgarDataTableInstance) {
            alert('Table data is not available for export.');
            return;
        }
        const ticker = currentEdgarExportContext.ticker || "SelectedConcepts";
        const today = new Date().toISOString().slice(0, 10);
        const filename = `EDGAR_Custom_${ticker}_${today}.xls`;
        
        const tempTable = document.createElement('table');
        const tempThead = tempTable.createTHead();
        const tempTbody = tempTable.createTBody();
        const headerRow = tempThead.insertRow();
        
        edgarDataTableInstance.columns().every(function() {
            const th = document.createElement('th');
            th.textContent = $(this.header()).text();
            headerRow.appendChild(th);
        });

        edgarDataTableInstance.rows({ search: 'applied' }).data().each(function(rowData) {
            const tr = tempTbody.insertRow();
            edgarDataTableInstance.columns().every(function(colIdx) {
                const cellData = this.dataSrc() ? rowData[this.dataSrc()] : '';
                let displayData = cellData;
                const column = edgarDataTableInstance.column(colIdx);
                const renderFunc = column.settings()[0].mRender || column.settings()[0].mData;
                
                if (typeof renderFunc === 'function') {
                    displayData = renderFunc(cellData, 'display', rowData, {col: colIdx, row: 0, settings: column.settings()[0]});
                } else if (typeof renderFunc === 'string') {
                    displayData = rowData[renderFunc];
                }

                if (colIdx === 0 && typeof displayData === 'string') { 
                    const tempDiv = document.createElement('div');
                    tempDiv.innerHTML = displayData;
                    displayData = tempDiv.textContent || tempDiv.innerText || "";
                }
                if (colIdx === 8 && typeof cellData !== 'undefined') { 
                    const numericVal = parseFloat(cellData);
                    if (!isNaN(numericVal)) {
                        displayData = numericVal.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 4 });
                    } else {
                        displayData = cellData;
                    }
                }
                const td = tr.insertCell();
                if ([4, 6, 7].includes(colIdx)) { // Date of Filing (4), Start Date (6), End Date (7)
                    td.style.msoNumberFormat = '\\@'; 
                }
                td.textContent = displayData !== undefined ? displayData : '';
            });
        });
        exportTableToXLSNode(tempTable, filename);
    }

    // Helper to parse fiscal year input string into a query structure
    function parseFiscalYearInput(fyInputString) {
        const trimmedInput = fyInputString.trim();
        if (!trimmedInput) {
            return [{ type: 'year', year: new Date().getFullYear() }]; 
        }

        const parts = trimmedInput.split(',').map(p => p.trim()).filter(p => p.length > 0);
        const parsedQueries = [];

        for (const part of parts) {
            if (part.includes('-')) {
                const rangeParts = part.split('-');
                if (rangeParts.length === 2) {
                    const startYear = parseInt(rangeParts[0].trim(), 10);
                    const endYear = parseInt(rangeParts[1].trim(), 10);
                    if (!isNaN(startYear) && !isNaN(endYear) && startYear <= endYear) {
                        parsedQueries.push({ type: 'range', start: startYear, end: endYear });
                    } else {
                        console.warn(`Invalid year range detected: ${part}`);
                    }
                } else {
                    console.warn(`Invalid year range format: ${part}`);
                }
            } else {
                const year = parseInt(part.trim(), 10);
                if (!isNaN(year)) {
                    parsedQueries.push({ type: 'year', year: year });
                } else {
                    console.warn(`Invalid year detected: ${part}`);
                }
            }
        }
        // If input was provided but nothing valid parsed, default to current year.
        return parsedQueries.length > 0 ? parsedQueries : [{ type: 'year', year: new Date().getFullYear() }];
    }

    // Listener for the cleanup toggle
    if (edgarCleanupToggle) {
        edgarCleanupToggle.addEventListener('change', function() {
            const isChecked = this.checked;
            if (edgar10qFillerToggle) {
                edgar10qFillerToggle.disabled = !isChecked;
                if (!isChecked) {
                    edgar10qFillerToggle.checked = false;
                }
            }

            const isActiveTab = document.getElementById('tabular-view-tab')?.classList.contains('active');
            if (currentDisplayableRawData && isActiveTab) {
                // Ensure currentDisplayableRawData is an array before reprocessing
                if (!Array.isArray(currentDisplayableRawData)) {
                    console.warn('Cleanup toggle: currentDisplayableRawData is not an array, aborting table refresh.');
                    // Optionally, display a message to the user if this state is problematic
                    const statusDivError = document.getElementById('edgarStatusDiv');
                    if (statusDivError) {
                        statusDivError.textContent = 'Cannot apply cleanup: Previous data load resulted in an error or no data.';
                        statusDivError.className = 'alert alert-warning mt-3';
                        statusDivError.style.display = 'block';
                    }
                    return; 
                }

                const statusDiv = document.getElementById('edgarStatusDiv');
                if (statusDiv) {
                    statusDiv.textContent = 'Applying data clean-up settings...';
                    statusDiv.className = 'alert alert-info mt-3';
                    statusDiv.style.display = 'block';
                }
                const fyInputEl = document.getElementById('edgarFyInput');
                const fiscalYearQuery = parseFiscalYearInput(fyInputEl.value);
                populateCustomEdgarTable(currentDisplayableRawData, fiscalYearQuery);
                if (statusDiv) {
                    setTimeout(() => {
                         if (statusDiv.textContent === 'Applying data clean-up settings...') {
                            statusDiv.style.display = 'none';
                         }
                    }, 1000);
                }
            }
        });
    }

    // Added: Event listener for the 10-Q Filler toggle
    if (edgar10qFillerToggle) {
        // Initialize disabled state based on cleanup toggle
        if (edgarCleanupToggle) {
            edgar10qFillerToggle.disabled = !edgarCleanupToggle.checked;
            if (!edgarCleanupToggle.checked) {
                 edgar10qFillerToggle.checked = false;
            }
        }

        edgar10qFillerToggle.addEventListener('change', function() {
            const isActiveTab = document.getElementById('tabular-view-tab')?.classList.contains('active');
            if (currentDisplayableRawData && isActiveTab) {
                if (!Array.isArray(currentDisplayableRawData)) {
                    console.warn('10-Q Filler toggle: currentDisplayableRawData is not an array, aborting table refresh.');
                    const statusDivError = document.getElementById('edgarStatusDiv');
                    if (statusDivError) {
                        statusDivError.textContent = 'Cannot apply 10-Q Filler: Previous data load resulted in an error or no data.';
                        statusDivError.className = 'alert alert-warning mt-3';
                        statusDivError.style.display = 'block';
                    }
                    return;
                }
                const statusDiv = document.getElementById('edgarStatusDiv');
                if (statusDiv) {
                    statusDiv.textContent = 'Applying 10-Q Filler settings...';
                    statusDiv.className = 'alert alert-info mt-3';
                    statusDiv.style.display = 'block';
                }
                const fyInputEl = document.getElementById('edgarFyInput');
                const fiscalYearQuery = parseFiscalYearInput(fyInputEl.value);
                populateCustomEdgarTable(currentDisplayableRawData, fiscalYearQuery); // Re-populate table
                 if (statusDiv) {
                    setTimeout(() => {
                        if (statusDiv.textContent === 'Applying 10-Q Filler settings...') {
                            statusDiv.style.display = 'none';
                         }
                    }, 1000);
                }
            }
        });
    }

    // Event listener for the new "Clear Selections" button for concepts
    const clearEdgarConceptSelectionsBtn = document.getElementById('clearEdgarConceptSelectionsBtn');
    if (clearEdgarConceptSelectionsBtn) {
        clearEdgarConceptSelectionsBtn.addEventListener('click', function() {
            const conceptListUl = document.getElementById('edgarConceptList');
            if (conceptListUl) {
                const checkboxes = conceptListUl.querySelectorAll('input[type="checkbox"]');
                checkboxes.forEach(checkbox => {
                    checkbox.checked = false;
                });
            }
            // Optionally, you might want to trigger an update or provide feedback here
        });
    }

    // Helper function to filter concept data by fiscal year(s) based on entry.end
    function filterConceptDataByFiscalYears(conceptData, fiscalYearQuery) {
        if (!conceptData || conceptData.error || !conceptData.units) {
            return conceptData; 
        }
        // If fiscalYearQuery is somehow empty (parser should prevent this by defaulting), return all
        if (!fiscalYearQuery || fiscalYearQuery.length === 0) { 
            return conceptData; 
        }

        const filteredUnits = {};
        for (const unitKey in conceptData.units) {
            if (Object.hasOwnProperty.call(conceptData.units, unitKey)) {
                const unitEntries = conceptData.units[unitKey];
                if (Array.isArray(unitEntries)) {
                    const filteredUnitEntries = unitEntries.filter(entry => {
                        if (!entry.end) return false; 
                        try {
                            const entryEndDate = new Date(entry.end);
                            const entryEndYear = entryEndDate.getFullYear();
                            if (isNaN(entryEndYear)) return false; 

                            for (const query of fiscalYearQuery) {
                                if (query.type === 'year' && entryEndYear === query.year) {
                                    return true;
                                }
                                if (query.type === 'range' && entryEndYear >= query.start && entryEndYear <= query.end) {
                                    return true;
                                }
                            }
                            return false; 
                        } catch (e) {
                            console.warn("Error parsing entry.end date in filterConceptDataByFiscalYears:", entry.end, e);
                            return false; 
                        }
                    });
                    if (filteredUnitEntries.length > 0) {
                        filteredUnits[unitKey] = filteredUnitEntries;
                    }
                }
            }
        }
        return { ...conceptData, units: filteredUnits };
    }

    // New Helper: Filter cumulative records from 10-Q forms (now flags instead of filters)
    function flagCumulativeRecords(unitEntries) {
        if (!Array.isArray(unitEntries)) return [];
        return unitEntries.map(entry => {
            const newEntry = {...entry}; // Avoid mutating original entry objects in currentDisplayableRawData
            newEntry._isCumulative = false; // Initialize flag
            if (entry.form !== '10-Q' || !entry.start || entry.start === 'N/A' || !entry.end) {
                return newEntry; 
            }
            try {
                const startDate = new Date(entry.start);
                const endDate = new Date(entry.end);
                if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
                    return newEntry; 
                }
                const diffTime = Math.abs(endDate - startDate);
                const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                const diffMonths = diffDays / 30.4375; 

                if (diffMonths >= 4.5 && diffMonths < 10.5) { 
                    newEntry._isCumulative = true;
                }
                return newEntry;
            } catch (e) {
                console.warn("Date parsing error in flagCumulativeRecords for entry:", entry, e);
                return newEntry; 
            }
        });
    }

    // New Helper: Handle duplicates and restatements (now flags all relevant entries)
    function flagDuplicatesAndRestatements(unitEntries) {
        if (!Array.isArray(unitEntries) || unitEntries.length === 0) return [];

        const groupedByDateAndUnit = {}; 
        unitEntries.forEach(entry => {
            const key = `${entry.unit}_${entry.end}_${entry.start || 'no_start'}`;
            if (!groupedByDateAndUnit[key]) {
                groupedByDateAndUnit[key] = [];
            }
            // Create copies to avoid mutating shared objects if unitEntries comes from a shared source
            groupedByDateAndUnit[key].push({...entry}); 
        });

        const processedEntries = [];
        for (const key in groupedByDateAndUnit) {
            if (Object.hasOwnProperty.call(groupedByDateAndUnit, key)) {
                let group = groupedByDateAndUnit[key];

                // Initialize flags for all in group
                group.forEach(entry => {
                    entry._isRestatement = false;
                    entry._isRedundantDuplicate = false;
                    entry._isSupersededRestatement = false;
                });

                if (group.length === 1) {
                    processedEntries.push(group[0]);
                    continue;
                }

                group.sort((a, b) => new Date(a.filed) - new Date(b.filed)); // Earliest first

                const firstValueStr = group[0].val;
                const firstValueNum = parseFloat(firstValueStr);
                let allValuesSame;
                if (isNaN(firstValueNum)) { 
                    allValuesSame = group.every(item => item.val === firstValueStr);
                } else {
                    allValuesSame = group.every(item => Math.abs(parseFloat(item.val) - firstValueNum) < 0.0001);
                }

                if (allValuesSame) {
                    // Values are the same. First one is primary. Others are redundant.
                    group.forEach((entry, index) => {
                        if (index > 0) entry._isRedundantDuplicate = true;
                        processedEntries.push(entry);
                    });
                } else {
                    // Values differ. Last one (latest filed) is primary and a restatement.
                    // Others are superseded restatements.
                    group.forEach((entry, index) => {
                        if (index < group.length - 1) {
                            entry._isSupersededRestatement = true;
                        } else {
                            entry._isRestatement = true; // The latest one
                        }
                        processedEntries.push(entry);
                    });
                }
            }
        }
        return processedEntries;
    }

    // --- EDGAR Data Fetcher Logic ---
    const edgarForm = document.getElementById('edgarDataForm');
    const edgarTickerInput = document.getElementById('edgarTickerInput');
    let currentEdgarExportContext = { cik: null, ticker: null, companyTitle: null }; // Added companyTitle

    const exportEdgarFactsBtn = document.getElementById('exportEdgarFactsBtn');
    if (exportEdgarFactsBtn) {
        exportEdgarFactsBtn.addEventListener('click', function() {
            if (currentEdgarExportContext.cik && currentEdgarExportContext.ticker) {
                const today = new Date().toISOString().slice(0, 10);
                const filename = `EDGAR_Facts_${currentEdgarExportContext.ticker}_CIK${currentEdgarExportContext.cik}_${today}.xls`;
                exportTableToXLS('edgarFactsTable', filename);
            } else {
                alert('No CIK or Ticker context available for export. Please fetch data first.');
            }
        });
    }

    if (edgarForm) {
        edgarForm.addEventListener('submit', async function(event) {
            event.preventDefault();
            clearEdgarResults();

            const ticker = edgarTickerInput.value.trim().toUpperCase();
            const statusDiv = document.getElementById('edgarStatusDiv');
            const conceptListContainer = document.getElementById('edgarConceptListContainer');
            const conceptDataContainer = document.getElementById('edgarConceptDataContainer');

            if (conceptListContainer) conceptListContainer.style.display = 'none';
            if (conceptDataContainer) conceptDataContainer.style.display = 'none';

            if (!ticker) {
                displayEdgarError('Ticker symbol cannot be empty.');
                return;
            }

            if (statusDiv){
                statusDiv.textContent = `Fetching CIK for ${ticker}...`;
                statusDiv.className = 'alert alert-info mt-3';
                statusDiv.style.display = 'block';
            }

            try {
                // 1. Fetch Company Info (CIK)
                const companyInfoResponse = await fetch(`/api/v3/edgar/company-tickers?ticker=${encodeURIComponent(ticker)}`);
                if (!companyInfoResponse.ok) {
                    const errorData = await companyInfoResponse.json().catch(() => ({ detail: companyInfoResponse.statusText }));
                    throw new Error(errorData.detail || `HTTP error! status: ${companyInfoResponse.status}`);
                }
                const companyInfo = await companyInfoResponse.json();
                if (!companyInfo || !companyInfo.cik_str) {
                    displayEdgarError(`CIK not found for ticker ${ticker}. Review Ticker or check SEC EDGAR.`, true);
                    return;
                }

                displayEdgarCompanyInfo(companyInfo);
                currentEdgarExportContext.ticker = companyInfo.ticker;
                currentEdgarExportContext.companyTitle = companyInfo.title;

                const cik = companyInfo.cik_str;
                currentEdgarExportContext.cik = cik;
                
                if (statusDiv) {
                    statusDiv.textContent = `Fetching all available data concepts for CIK: ${cik}...`; 
                    statusDiv.className = 'alert alert-info mt-3'; 
                    statusDiv.style.display = 'block';
                }

                // 2. Fetch Company Facts (to extract concept list)
                const factsResponse = await fetch(`/api/v3/edgar/company-facts/${cik}`);
                if (!factsResponse.ok) {
                    const errorData = await factsResponse.json().catch(() => ({ detail: factsResponse.statusText }));
                    throw new Error(errorData.detail || `HTTP error! status: ${factsResponse.status}`);
                }
                const factsData = await factsResponse.json();
                if (factsData && factsData.facts) {
                    processAndDisplayConceptList(factsData, cik); // New function
                } else {
                    displayEdgarError(`No facts data returned or facts structure is unexpected for CIK ${cik}.`, true);
                }

            } catch (error) {
                console.error('EDGAR Data Fetch Error:', error);
                displayEdgarError(`EDGAR Data Fetch Error: ${error.message}`);
            }
        });
    }

    function processAndDisplayConceptList(factsData, cik) {
        const statusDiv = document.getElementById('edgarStatusDiv');
        const conceptListContainer = document.getElementById('edgarConceptListContainer');
        const conceptListUl = document.getElementById('edgarConceptList');
        const conceptListHeaderSpan = document.getElementById('edgarConceptListHeaderSpan'); 

        if (!conceptListContainer || !conceptListUl || !conceptListHeaderSpan) {
            console.error('Concept list UI elements (container, ul, or header span) not found.');
            displayEdgarError('UI error: Could not display concept list.');
            return;
        }

        conceptListUl.innerHTML = ''; // Clear previous list
        const conceptsWithDetails = [];

        if (factsData && factsData.facts) {
            for (const taxonomy in factsData.facts) {
                if (Object.hasOwnProperty.call(factsData.facts, taxonomy)) {
                    const taxonomyFacts = factsData.facts[taxonomy];
                    for (const conceptName in taxonomyFacts) {
                        if (Object.hasOwnProperty.call(taxonomyFacts, conceptName)) {
                            const concept = taxonomyFacts[conceptName];
                            // Log the concept object for inspection, especially for deprecated items
                            // console.log(`Inspecting Concept for deprecation: ${conceptName}`, JSON.parse(JSON.stringify(concept))); // Log a clone

                            // Check if the label contains "(Deprecated"
                            const isDeprecated = concept.label && typeof concept.label === 'string' && concept.label.includes('(Deprecated');
                            // console.log(`Concept: ${conceptName}, Label: \"${concept.label}\", Calculated isDeprecated: ${isDeprecated}`);

                            if (concept.label && concept.description) {
                                conceptsWithDetails.push({
                                    label: concept.label,
                                    description: concept.description,
                                    conceptName: conceptName, 
                                    taxonomy: taxonomy, 
                                    isDeprecated: !!isDeprecated // Store deprecated status as boolean
                                });
                            }
                        }
                    }
                }
            }
        }
        // Store the full list for re-use
        allFetchedConceptDetails = conceptsWithDetails; 

        const companyTicker = currentEdgarExportContext.ticker || 'Company';
        const companyTitle = currentEdgarExportContext.companyTitle || 'N/A';

        if (allFetchedConceptDetails.length > 0) {
            conceptListHeaderSpan.textContent = `${companyTicker} (${companyTitle}) - ${allFetchedConceptDetails.length} Available Data Concepts`;
            
            displayConceptList(allFetchedConceptDetails, cik); // This will show the concept list container and other relevant UI
            
            // Ensure status div is hidden and cleared as we have concepts
            if (statusDiv) {
                statusDiv.style.display = 'none';
                statusDiv.textContent = ''; 
            }
            conceptListContainer.style.display = 'block'; // Ensure container is visible

        } else {
            // No concepts found
            conceptListHeaderSpan.textContent = `${companyTicker} (${companyTitle}) - No Data Concepts Found`;
            
            if (statusDiv) {
                statusDiv.textContent = `No data concepts with labels and descriptions found for CIK ${cik}.`;
                statusDiv.className = 'alert alert-warning mt-3';
                statusDiv.style.display = 'block';
            }
            conceptListContainer.style.display = 'block'; // Show container to display the updated header
            // conceptListUl.innerHTML = ''; // Ensure list is empty - displayConceptList will handle this
            
            // Hide elements that depend on concepts being present (handled by displayConceptList with empty array)
            displayConceptList([], cik); 
        }
    }

    function displayConceptList(conceptsToDisplay, cik) { // conceptsToDisplay is now the full list, will be processed
        const conceptListUl = document.getElementById('edgarConceptList');
        const multiFetchControls = document.getElementById('edgarMultiFetchControls');
        const edgarOutputSubTabs = document.getElementById('edgarOutputSubTabs');
        const exportEdgarCustomTableBtn = document.getElementById('exportEdgarCustomTableBtn'); 
        const conceptSearchInput = document.getElementById('edgarConceptSearchInput');
        const conceptListContainer = document.getElementById('edgarConceptListContainer'); 
        const edgarFyInput = document.getElementById('edgarFyInput'); 

        if (!conceptListUl || !conceptListContainer) { 
            console.error("displayConceptList: Core UI elements missing.");
            return;
        }
        conceptListUl.innerHTML = ''; // Clear previous list items

        const globalFavorites = getFavorites();
        
        let renderableConcepts = conceptsToDisplay.map(cd => ({
            ...cd,
            isFavorite: globalFavorites.includes(cd.conceptName)
        }));

        renderableConcepts.sort((a, b) => {
            if (a.isFavorite && !b.isFavorite) return -1;
            if (!a.isFavorite && b.isFavorite) return 1;
            return a.label.localeCompare(b.label);
        });


        if (renderableConcepts.length > 0) {
            conceptListContainer.style.display = 'block'; 
            if (multiFetchControls) multiFetchControls.style.display = 'block';
            if (edgarFyInput) { 
                const currentYear = new Date().getFullYear();
                if (!edgarFyInput.value) { // Only set if not already set (e.g. by user)
                     edgarFyInput.value = `2020-${currentYear}`;
                }
            }
            if (edgarOutputSubTabs) edgarOutputSubTabs.style.display = 'flex';
            if (conceptSearchInput) conceptSearchInput.style.display = 'block';
        } else {
            conceptListContainer.style.display = 'block'; 
            if (multiFetchControls) multiFetchControls.style.display = 'none';
            if (edgarOutputSubTabs) edgarOutputSubTabs.style.display = 'none';
            if (exportEdgarCustomTableBtn) exportEdgarCustomTableBtn.style.display = 'none';
            if (conceptSearchInput) conceptSearchInput.style.display = 'none';
            if (edgarFyInput) edgarFyInput.value = ''; 
        }

        renderableConcepts.forEach((conceptDetail, index) => {
            const listItem = document.createElement('li');
            listItem.className = 'list-group-item';
            // Centralize data attributes on the LI for robust filtering and searching
            listItem.dataset.conceptName = conceptDetail.conceptName;
            listItem.dataset.isDeprecated = conceptDetail.isDeprecated;
            listItem.dataset.label = conceptDetail.label;

            const formCheckDiv = document.createElement('div');
            formCheckDiv.className = 'd-flex align-items-center'; // Removed form-check to allow custom layout

            const checkboxId = `edgar-concept-cb-${index}`;

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'form-check-input me-2'; // Added margin-end to space it from the star
            checkbox.id = checkboxId;
            checkbox.value = conceptDetail.conceptName;
            checkbox.dataset.cik = cik;
            checkbox.dataset.taxonomy = conceptDetail.taxonomy;
            checkbox.dataset.conceptName = conceptDetail.conceptName; // Keep for data collection
            checkbox.dataset.label = conceptDetail.label; // Keep for data collection
            checkbox.dataset.description = conceptDetail.description;

            // Star icon
            const starIcon = document.createElement('i');
            starIcon.className = `bi ${conceptDetail.isFavorite ? 'bi-star-fill text-warning' : 'bi-star'} me-2`; // Bootstrap icons
            starIcon.style.cursor = 'pointer'; // Make star itself look clickable
            starIcon.title = 'Toggle favorite';
            starIcon.dataset.action = 'toggle-favorite';
            starIcon.dataset.conceptName = conceptDetail.conceptName;
            
            const labelElement = document.createElement('label');
            labelElement.className = 'form-check-label';
            labelElement.htmlFor = checkboxId; // Use htmlFor for native checkbox toggling
            labelElement.style.cursor = 'pointer';
            labelElement.title = conceptDetail.description;
            labelElement.textContent = conceptDetail.label;

            // Assemble the structure
            formCheckDiv.appendChild(checkbox);
            formCheckDiv.appendChild(starIcon);
            formCheckDiv.appendChild(labelElement);

            listItem.appendChild(formCheckDiv);

            // Add event listener to the label to show raw JSON, since star is now outside
            labelElement.addEventListener('click', (event) => {
                // The native label click will toggle the checkbox. If we want to prevent that
                // and ONLY show JSON, we'd add event.preventDefault(). But usually, clicking
                // the label should also check the box. If we want a separate "view JSON" action,
                // that would need a different UI element. For now, we assume label click
                // selects the item AND navigates. Let's make it so it doesn't navigate,
                // just selects. The old implementation was confusing. A separate button is better.
                // REVISED PLAN: We will create a small "view raw" icon.

                // Let's stick to the user's primary request: fix layout and events.
                // The old code had a complex event listener on labelTextNode.
                // Now label click correctly toggles the checkbox via for/id.
                // Let's add a separate, small icon for viewing JSON to avoid ambiguity.
            });
            
            conceptListUl.appendChild(listItem);
        });

        filterConceptListDisplay(); // Apply visual filtering
    }

    // New function to handle filtering of the concept list
    function filterConceptListDisplay() {
        const searchTerm = document.getElementById('edgarConceptSearchInput').value.toLowerCase().trim();
        const showDeprecated = document.getElementById('edgarShowDeprecatedToggle').checked;
        const conceptListUl = document.getElementById('edgarConceptList');
        if (!conceptListUl) return;

        const listItems = conceptListUl.getElementsByTagName('li');
        Array.from(listItems).forEach(item => {
            // Read data from the LI element's dataset for robustness
            const label = item.dataset.label || '';
            const isDeprecated = item.dataset.isDeprecated === 'true';

            const matchesSearch = label.toLowerCase().includes(searchTerm);
            const shouldBeVisible = matchesSearch && (showDeprecated || !isDeprecated);

            // Logging for filter decision
            // console.log(`Filtering item: ${label}, Search Term: \"${searchTerm}\", Matches Search: ${matchesSearch}, ShowDeprecated Toggle: ${showDeprecated}, Item isDeprecated: ${isDeprecated}, ShouldBeVisible: ${shouldBeVisible}`);

            if (shouldBeVisible) {
                item.style.display = ''; // Show item
            } else {
                item.style.display = 'none'; // Hide item
            }
        });
    }

    // Event listener for the concept search input (modified to call the new filter function)
    if (edgarConceptSearchInput) {
        edgarConceptSearchInput.addEventListener('input', filterConceptListDisplay);
    }

    // Event listener for the new "Show Deprecated" toggle
    const edgarShowDeprecatedToggle = document.getElementById('edgarShowDeprecatedToggle');
    if (edgarShowDeprecatedToggle) {
        edgarShowDeprecatedToggle.addEventListener('change', filterConceptListDisplay);
    }

    async function fetchAndDisplaySingleConceptJson(cik, taxonomy, conceptName, conceptLabel) {
        const statusDiv = document.getElementById('edgarStatusDiv');
        const conceptJsonOutputEl = document.getElementById('edgarConceptJsonOutput');
        const conceptDataContainerEl = document.getElementById('edgarConceptDataContainer');
        const rawJsonTabButton = document.getElementById('raw-json-view-tab');
        const fyInputEl = document.getElementById('edgarFyInput'); // Get the input element

        if (!conceptJsonOutputEl || !conceptDataContainerEl) { console.error("JSON display elements missing"); return; }

        const fiscalYearQuery = parseFiscalYearInput(fyInputEl.value); // Use new parser
        // For status message, create a readable string from the query
        const fyQueryString = fiscalYearQuery.map(q => q.type === 'year' ? q.year : `${q.start}-${q.end}`).join(', ');

        if (statusDiv) { statusDiv.textContent = `Fetching & filtering JSON for "${conceptLabel}" (End Date Year(s): ${fyQueryString})...`; statusDiv.className = 'alert alert-info mt-3'; statusDiv.style.display = 'block';}
        conceptJsonOutputEl.textContent = 'Loading...';
        currentDisplayableRawData = null; 

        try {
            const rawData = await fetchConceptRawData(cik, taxonomy, conceptName);
            const filteredData = filterConceptDataByFiscalYears(rawData, fiscalYearQuery); // Pass parsed query
            currentDisplayableRawData = filteredData; 
            
            conceptJsonOutputEl.textContent = JSON.stringify(currentDisplayableRawData, null, 2);
            if (window.hljs) hljs.highlightElement(conceptJsonOutputEl);
            
            if (statusDiv) { statusDiv.textContent = `Successfully fetched & filtered JSON for "${conceptLabel}".`; statusDiv.className = 'alert alert-success mt-3';}
            if (rawJsonTabButton) bootstrap.Tab.getOrCreateInstance(rawJsonTabButton).show();
        } catch (error) {
            console.error('Fetch Single Concept JSON Error:', error);
            const errorFyYears = fiscalYearQuery.map(q => q.type === 'year' ? q.year : `${q.start}-${q.end}`);
            currentDisplayableRawData = { error: error.message, forConcept: conceptLabel, requestedFiscalPeriods: errorFyYears };
            conceptJsonOutputEl.textContent = `Error fetching/filtering data for "${conceptLabel}": ${error.message}`;
            if (statusDiv) { statusDiv.textContent = `Error for "${conceptLabel}": ${error.message}`; statusDiv.className = 'alert alert-danger mt-3';}
            if (rawJsonTabButton) bootstrap.Tab.getOrCreateInstance(rawJsonTabButton).show(); 
        }
    }
    
    async function fetchConceptRawData(cik, taxonomy, conceptName) {
        const response = await fetch(`/api/v3/edgar/company-concept/${cik}/${taxonomy}/${encodeURIComponent(conceptName)}`);
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: response.statusText }));
            let errorMessage = errorData.detail || `HTTP error! status: ${response.status} for concept ${conceptName}`;
            // Add CIK, taxonomy to error for better debugging
            errorMessage += ` (CIK: ${cik}, Taxonomy: ${taxonomy})`;
            console.error(`fetchConceptRawData error: ${errorMessage} for URL /api/v3/edgar/company-concept/${cik}/${taxonomy}/${encodeURIComponent(conceptName)}`);
            throw new Error(errorMessage);
        }
        return await response.json();
    }

    const fetchSelectedEdgarDataBtn = document.getElementById('fetchSelectedEdgarDataBtn');
    if (fetchSelectedEdgarDataBtn) {
        fetchSelectedEdgarDataBtn.addEventListener('click', async function() {
            const tabularViewTabButton = document.getElementById('tabular-view-tab');
            if (tabularViewTabButton) {
                bootstrap.Tab.getOrCreateInstance(tabularViewTabButton).show();
            }
            
            const multiFetchControls = document.getElementById('edgarMultiFetchControls');
            if (multiFetchControls) multiFetchControls.style.display = 'block'; 

            const statusDiv = document.getElementById('edgarStatusDiv');
            const conceptListUl = document.getElementById('edgarConceptList');
            const fyInputEl = document.getElementById('edgarFyInput'); // Get the input element
            const customTableContainer = document.getElementById('edgarCustomTableContainer'); 

            const selectedCheckboxes = conceptListUl.querySelectorAll('input[type="checkbox"]:checked');
            if (selectedCheckboxes.length === 0) { 
                if (statusDiv) {
                    statusDiv.textContent = 'Please select at least one data concept for the table.';
                    statusDiv.className = 'alert alert-warning mt-3';
                    statusDiv.style.display = 'block';
                }
                return; 
            }

            const selectedConcepts = Array.from(selectedCheckboxes).map(cb => ({
                cik: cb.dataset.cik,
                taxonomy: cb.dataset.taxonomy,
                conceptName: cb.dataset.conceptName,
                label: cb.dataset.label
            }));
            
            const fiscalYearQuery = parseFiscalYearInput(fyInputEl.value); // Use new parser
             // For status message, create a readable string from the query
            const fyQueryString = fiscalYearQuery.map(q => q.type === 'year' ? q.year : `${q.start}-${q.end}`).join(', ');

            if (statusDiv) { statusDiv.textContent = `Fetching & filtering data for ${selectedConcepts.length} concept(s) (End Date Year(s): ${fyQueryString})...`; statusDiv.className = 'alert alert-info mt-3'; statusDiv.style.display = 'block';}
            
            currentDisplayableRawData = null; 

            try {
                // fetchAllSelectedConceptDataAndBuildTable will now use fiscalYearQuery for filtering
                const filteredDataResults = await fetchAllSelectedConceptDataAndBuildTable(selectedConcepts, fiscalYearQuery);
                currentDisplayableRawData = filteredDataResults; 
                
                if (tabularViewTabButton) bootstrap.Tab.getOrCreateInstance(tabularViewTabButton).show();
                 if (statusDiv) {
                    statusDiv.textContent = `Data processed for ${selectedConcepts.length} concept(s). View in table or JSON tab.`;
                    statusDiv.className = 'alert alert-success mt-3';
                }
            } catch (error) {
                console.error("Multi-concept fetch/table build ERROR:", error);
                const errorFyYears = fiscalYearQuery.map(q => q.type === 'year' ? q.year : `${q.start}-${q.end}`);
                currentDisplayableRawData = { error: `Failed to fetch/build table: ${error.message}`, requestedFiscalPeriods: errorFyYears }; 
                if (statusDiv) { 
                    statusDiv.textContent = `Error: ${error.message}`;
                    statusDiv.className = 'alert alert-danger mt-3'; 
                }
            }
        });
    }

    async function fetchAllSelectedConceptDataAndBuildTable(selectedConcepts, fiscalYearQuery) { // Now accepts fiscalYearQuery
        const allConceptsDataPromises = selectedConcepts.map(concept => {
            return fetchConceptRawData(concept.cik, concept.taxonomy, concept.conceptName)
                .then(data => ({ ...data, _rawConceptLabel: concept.label, _conceptName: concept.conceptName, _taxonomy: concept.taxonomy }))
                .catch(error => ({ error: `Failed to load data for ${concept.label}: ${error.message}`, _rawConceptLabel: concept.label, _conceptName: concept.conceptName, _taxonomy: concept.taxonomy }));
        });
        
        let rawResults = [];
        try {
            rawResults = await Promise.all(allConceptsDataPromises);
        } catch (promiseAllError) {
            console.error("Error in Promise.all during multi-fetch:", promiseAllError);
            rawResults = selectedConcepts.map(c => ({ error: `Batch fetch error for ${c.label}.`, _rawConceptLabel: c.label, _conceptName: c.conceptName, _taxonomy: c.taxonomy }));
        }

        const fiscalYearFilteredResults = rawResults.map(conceptData => 
            filterConceptDataByFiscalYears(conceptData, fiscalYearQuery) // Pass parsed query
        );
        
        populateCustomEdgarTable(fiscalYearFilteredResults, fiscalYearQuery); // Pass fiscalYearQuery for potential use (e.g. messages)
        
        return fiscalYearFilteredResults; 
    }

    function populateCustomEdgarTable(allConceptsData, fiscalYearQuery) {
        const tableContainer = document.getElementById('edgarCustomTableContainer');
        const tableElement = document.getElementById('edgarCustomDataTable');
        const cleanupToggle = document.getElementById('edgarCleanupToggle');
        const applyCleanup = cleanupToggle ? cleanupToggle.checked : true;
        const filtersContainer = document.getElementById('edgarTableFilters'); // Get the new filters container
        const q4FillerToggle = document.getElementById('edgar10qFillerToggle'); // Get Q4 filler toggle
        const applyQ4Filler = q4FillerToggle ? q4FillerToggle.checked : false;

        if (!tableElement || !tableContainer || !filtersContainer) { 
            console.error("Table UI or filters container missing"); 
            if(tableContainer) tableContainer.style.display = 'none';
            if(filtersContainer) filtersContainer.style.display = 'none';
            return; 
        }
        
        if (!Array.isArray(allConceptsData)) {
            console.error("populateCustomEdgarTable: allConceptsData is not an array.", allConceptsData);
            const statusDivError = document.getElementById('edgarStatusDiv');
            if (statusDivError) {
                statusDivError.textContent = 'Error: Cannot display table. Data is in an unexpected format.';
                statusDivError.className = 'alert alert-danger mt-3';
                statusDivError.style.display = 'block';
            }
            if (edgarDataTableInstance) { edgarDataTableInstance.clear().draw(); 
            } else { $(tableElement).find('tbody').empty().html('<tr><td colspan="9">Error: Data format issue.</td></tr>'); }
            filtersContainer.style.display = 'none'; 
            tableContainer.style.display = 'block'; 
            return;
        }

        let tableDataForDataTable = [];
        let conceptHasVisibleRowsOverall = false;

        allConceptsData.forEach(conceptDataWithMeta => {
            if (conceptDataWithMeta.error || !conceptDataWithMeta.units) {
                return;
            }
            const baseConceptLabel = conceptDataWithMeta._rawConceptLabel || conceptDataWithMeta._conceptLabel; 

            for (const unitKey in conceptDataWithMeta.units) {
                if (Object.hasOwnProperty.call(conceptDataWithMeta.units, unitKey)) {
                    let unitEntries = conceptDataWithMeta.units[unitKey];
                    if (!Array.isArray(unitEntries) || unitEntries.length === 0) continue;

                    let entriesWithContext = unitEntries.map(e => ({...e, _displayLabel: baseConceptLabel, unit: e.unit || unitKey }));
                    
                    let flaggedEntries = flagCumulativeRecords(entriesWithContext);
                    flaggedEntries = flagDuplicatesAndRestatements(flaggedEntries);

                    let processedForCleanup = flaggedEntries;
                    if (applyCleanup) {
                        processedForCleanup = flaggedEntries.filter(e => 
                            !e._isCumulative && 
                            !e._isRedundantDuplicate && 
                            !e._isSupersededRestatement
                        );
                    }

                    let entriesToDisplay = processedForCleanup;
                    if (applyCleanup && applyQ4Filler) { 
                        entriesToDisplay = generateInferredQ4Data(processedForCleanup); 
                    }
                    
                    const fpActualOrder = { 'FY': 1, 'Q4': 2, 'Q3': 3, 'Q2': 4, 'Q1': 5 };
                    entriesToDisplay.sort((a, b) => {
                        const dateA_end = new Date(String(a.end) + 'T00:00:00Z');
                        const dateB_end = new Date(String(b.end) + 'T00:00:00Z');
                        if (dateA_end > dateB_end) return -1; 
                        if (dateA_end < dateB_end) return 1;

                        let orderA = fpActualOrder[a.fp_actual] || 99;
                        let orderB = fpActualOrder[b.fp_actual] || 99;
                        if (a._isInferredQ4) orderA = fpActualOrder['Q4']; 
                        if (b._isInferredQ4) orderB = fpActualOrder['Q4'];
                        if (orderA !== orderB) return orderA - orderB;
                        
                        const dateA_start = new Date(String(a.start) + 'T00:00:00Z');
                        const dateB_start = new Date(String(b.start) + 'T00:00:00Z');
                        if (dateA_start < dateB_start) return -1; 
                        if (dateA_start > dateB_start) return 1;

                        return 0; 
                    });

                    if (entriesToDisplay.length > 0) conceptHasVisibleRowsOverall = true;
                    tableDataForDataTable.push(...entriesToDisplay);
                }
            }
        });
        
        if (edgarDataTableInstance) {
            edgarDataTableInstance.destroy();
            $(tableElement).find('thead').empty();
            $(tableElement).find('tbody').empty(); 
            edgarDataTableInstance = null;
        }
        filtersContainer.innerHTML = ''; 

        if (!conceptHasVisibleRowsOverall && tableDataForDataTable.length === 0) {
            filtersContainer.style.display = 'none'; 
            tableContainer.style.display = 'block'; 
            const statusDiv = document.getElementById('edgarStatusDiv');
            if (statusDiv) {
                const fyQueryStringForMsg = Array.isArray(fiscalYearQuery) ? fiscalYearQuery.map(q => q.type === 'year' ? q.year : `${q.start}-${q.end}`).join(', ') : "selected period";
                statusDiv.textContent = `No data found for the selected concepts for End Date Year(s): ${fyQueryStringForMsg}${applyCleanup ? ' after clean-up and filtering' : ' (raw data also empty or fully filtered)'}.`;
                statusDiv.className = 'alert alert-warning mt-3';
                statusDiv.style.display = 'block';
            }
            edgarDataTableInstance = $(tableElement).DataTable({ 
                data: [], 
                columns: [ 
                    { title: 'Concept' }, { title: 'Unit' }, { title: 'FP' }, { title: 'FP Actual' }, 
                    { title: 'Date of Filing' }, { title: 'Form' }, 
                    { title: 'Start Date' }, { title: 'End Date' }, { title: 'Value' }
                ],
                destroy: true, searching: true, paging: true, lengthChange: true,
                lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, "All"]],
                pageLength: -1, 
                language: { emptyTable: "No data available for the selected criteria." }
             });
            return; 
        }
        
        const conceptValues = [...new Set(tableDataForDataTable.map(item => item._displayLabel))].sort();

        const conceptFilterCol = document.createElement('div');
        conceptFilterCol.className = 'col-md-4'; 
        conceptFilterCol.innerHTML = `
            <label for="edgarConceptFilterSelect" class="form-label form-label-sm">Filter Concept:</label>
            <select id="edgarConceptFilterSelect" class="form-select form-select-sm">
                <option value="">All Concepts</option>
                ${conceptValues.map(val => `<option value="${val.replace(/"/g, '&quot;')}">${val}</option>`).join('')}
            </select>
        `;
        filtersContainer.appendChild(conceptFilterCol);

        // NEW: Period Type Filter
        const periodTypeFilterCol = document.createElement('div');
        periodTypeFilterCol.className = 'col-md-3'; // Adjust class as needed
        periodTypeFilterCol.innerHTML = `
            <label for="edgarPeriodTypeFilterSelect" class="form-label form-label-sm">Filter Period Type:</label>
            <select id="edgarPeriodTypeFilterSelect" class="form-select form-select-sm">
                <option value="">All Period Types</option>
                <option value="FY">Annual (FY)</option>
                <option value="Q">Quarterly (Q1-Q4)</option>
            </select>
        `;
        filtersContainer.appendChild(periodTypeFilterCol);
        
        filtersContainer.style.display = 'flex'; 
        tableContainer.style.display = 'block';
        const statusDiv = document.getElementById('edgarStatusDiv');
        if (statusDiv) statusDiv.style.display = 'none';

        edgarDataTableInstance = $(tableElement).DataTable({
            data: tableDataForDataTable,
            columns: [
                { 
                    data: '_displayLabel', title: 'Concept',
                    render: function(data, type, row) {
                        if (type === 'display') {
                            let html = '';
                            if (row._isInferredQ4) html += '<span class="badge bg-danger me-1" title="Inferred Filler Data">F</span>';
                            if (row._isRestatement) html += '<span class="badge bg-primary me-1" title="Restated Value (Latest)">R</span>';
                            if (!applyCleanup) {
                                if (row._isCumulative) html += '<span class="badge bg-secondary me-1" title="Cumulative Record">C</span>';
                                if (row._isRedundantDuplicate) html += '<span class="badge bg-light text-dark border me-1" title="Duplicate - Same Value">D</span>';
                                if (row._isSupersededRestatement) html += '<span class="badge bg-info text-dark me-1" title="Duplicate - Superseded">D</span>';
                            }
                            return html + (data || 'N/A'); 
                        }
                        return data || ''; 
                    }
                },
                { data: 'unit', title: 'Unit', defaultContent: 'N/A' },
                { data: 'fp', title: 'FP', defaultContent: 'N/A' },
                { data: 'fp_actual', title: 'FP Actual', defaultContent: 'N/A',
                    render: function(data, type, row) {
                        if (type === 'display') {
                            if (data !== row.fp) {
                                return `<span class="text-danger fw-bold" title="Original FP: ${row.fp}">${data}</span>`;
                            }
                            return data;
                        }
                        return data;
                    }
                },
                { data: 'filed', title: 'Date of Filing', defaultContent: 'N/A'  },
                { data: 'form', title: 'Form', defaultContent: 'N/A'  },
                { data: 'start', title: 'Start Date', defaultContent: 'N/A' },
                { data: 'end', title: 'End Date', defaultContent: 'N/A' },
                {
                    data: 'val', title: 'Value',
                    render: function(data, type, row) {
                        if (type === 'display') {
                            const numericValue = parseFloat(data);
                            if (!isNaN(numericValue)) {
                                return numericValue.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 4 });
                            }
                            return data !== undefined && data !== null ? data : 'N/A'; 
                        }
                        const num = parseFloat(data);
                        return isNaN(num) ? (type === 'sort' || type === 'type' ? Number.NEGATIVE_INFINITY : data) : num;
                    },
                    className: 'text-end' 
                }
            ],
            destroy: true, searching: true, paging: true, lengthChange: true,
            lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, "All"]],
            pageLength: -1, 
            order: [[7, 'desc'], [6, 'asc']], 
            rowCallback: function(row, data, index) {
                if (!applyCleanup) {
                    if (data._isCumulative) $(row).addClass('cumulative-row');
                    if (data._isRedundantDuplicate) $(row).addClass('redundant-row');
                    if (data._isSupersededRestatement) $(row).addClass('superseded-row');
                }
            },
            initComplete: function () {
                const api = this.api();
                $(api.table().footer()).empty(); 

                const exportButtonElement = document.createElement('button');
                exportButtonElement.id = 'exportEdgarCustomTableBtn';
                exportButtonElement.className = 'btn btn-sm btn-success ms-2'; 
                exportButtonElement.textContent = 'Export Table to XLS';
                exportButtonElement.style.display = 'inline-block';
                exportButtonElement.addEventListener('click', handleExportEdgarCustomTable);

                const chartButtonElement = document.createElement('button');
                chartButtonElement.id = 'showEdgarChartBtn';
                chartButtonElement.className = 'btn btn-sm btn-info ms-2';
                chartButtonElement.textContent = 'Barchart';
                chartButtonElement.style.display = 'inline-block'; // Show when table is ready
                chartButtonElement.addEventListener('click', displayEdgarBarChart);

                const filterDiv = $(tableElement).closest('.dataTables_wrapper').find('.dataTables_filter');
                if (filterDiv.length > 0) {
                    filterDiv.css('display', 'flex').css('align-items', 'center').css('justify-content', 'flex-end'); 
                    filterDiv.append(chartButtonElement); // Add chart button first
                    filterDiv.append(exportButtonElement);
                } else {
                    console.warn('[populateCustomEdgarTable] DataTables filter div not found. Cannot place export/chart button.');
                    const wrapper = $(tableElement).closest('.dataTables_wrapper');
                    wrapper.prepend(exportButtonElement); 
                    wrapper.prepend(chartButtonElement); // Add chart button
                }
            }
        });

        document.getElementById('edgarConceptFilterSelect').addEventListener('change', function() {
            const rawValue = this.value;
            const searchRegex = rawValue ? '^' + escapeRegex(rawValue) + '$' : '';
            edgarDataTableInstance.column(0).search(searchRegex, true, false).draw();
        });

        // NEW: Event listener for Period Type Filter
        document.getElementById('edgarPeriodTypeFilterSelect').addEventListener('change', function() {
            const selectedValue = this.value;
            let searchRegex = '';
            if (selectedValue === 'FY') {
                searchRegex = '^FY$';
            } else if (selectedValue === 'Q') {
                searchRegex = '^Q[1-4]$';
            }
            // Column 3 is 'FP Actual'
            edgarDataTableInstance.column(3).search(searchRegex, true, false).draw(); 
        });
    }

    function clearEdgarResults() {
        currentDisplayableRawData = null;
        if (edgarDataTableInstance) {
            edgarDataTableInstance.destroy();
            const tableElement = document.getElementById('edgarCustomDataTable');
            $(tableElement).find('thead').empty();
            $(tableElement).find('tbody').empty();
            $(tableElement).find('tfoot').empty(); // Clear tfoot as well
            edgarDataTableInstance = null;
        }
        if (edgarBarChartInstance) {
            edgarBarChartInstance.destroy();
            edgarBarChartInstance = null;
        }
        const chartButton = document.getElementById('showEdgarChartBtn');
        if (chartButton) {
            // The button is now dynamically created, so direct hiding might not be needed if its parent (filterDiv) is cleared.
            // However, if it was a static button, this would be: chartButton.style.display = 'none';
        }

        const statusDiv = document.getElementById('edgarStatusDiv'); if(statusDiv) { statusDiv.style.display = 'none'; statusDiv.textContent = ''; }
        const resultsDiv = document.getElementById('edgarResultsDiv'); if(resultsDiv) resultsDiv.style.display = 'none';
        const jsonOutputCik = document.getElementById('edgarJsonOutput'); if(jsonOutputCik) jsonOutputCik.textContent = '';
        const conceptListContainer = document.getElementById('edgarConceptListContainer'); if(conceptListContainer) conceptListContainer.style.display = 'none';
        const conceptListUl = document.getElementById('edgarConceptList'); if(conceptListUl) conceptListUl.innerHTML = '';
        const conceptSearchInput = document.getElementById('edgarConceptSearchInput'); if(conceptSearchInput) {conceptSearchInput.value = ''; conceptSearchInput.style.display = 'none';} // Clear and hide search input
        const conceptDataContainer = document.getElementById('edgarConceptDataContainer'); if(conceptDataContainer) conceptDataContainer.style.display = 'none';
        const conceptJsonOutput = document.getElementById('edgarConceptJsonOutput'); if(conceptJsonOutput) {conceptJsonOutput.textContent = ''; if(window.hljs) {conceptJsonOutput.className = 'json'; hljs.highlightElement(conceptJsonOutput);} } // Ensure hljs is re-applied on clear if content type changes
        const multiFetchControls = document.getElementById('edgarMultiFetchControls'); if(multiFetchControls) multiFetchControls.style.display = 'none';
        const fyInput = document.getElementById('edgarFyInput'); if(fyInput) fyInput.value = '';
        const customTableContainer = document.getElementById('edgarCustomTableContainer'); if(customTableContainer) customTableContainer.style.display = 'none';
        const edgarOutputSubTabs = document.getElementById('edgarOutputSubTabs'); if(edgarOutputSubTabs) edgarOutputSubTabs.style.display = 'none';
        
        // Clear and hide new dropdown filters container (which includes the export button now)
        const filtersContainer = document.getElementById('edgarTableFilters');
        if (filtersContainer) {
            filtersContainer.innerHTML = '';
            filtersContainer.style.display = 'none';
        }

        const tabularViewTabButton = document.getElementById('tabular-view-tab'); if (tabularViewTabButton) { bootstrap.Tab.getOrCreateInstance(tabularViewTabButton).show(); }
        currentEdgarExportContext = { cik: null, ticker: null, companyTitle: null };
    }

    function displayEdgarError(message, isWarning = false) {
        if (edgarDataTableInstance) { 
            edgarDataTableInstance.destroy(); 
            const tableElement = document.getElementById('edgarCustomDataTable');
            if(tableElement){
                 $(tableElement).find('thead').empty(); 
                 $(tableElement).find('tbody').empty();
            }
            edgarDataTableInstance = null; 
        }
        if (edgarBarChartInstance) {
            edgarBarChartInstance.destroy();
            edgarBarChartInstance = null;
        }
        const chartButton = document.getElementById('showEdgarChartBtn');
        if (chartButton) {
             // As above, dynamic button handling.
        }
        // Clear and hide new dropdown filters container
        const filtersContainer = document.getElementById('edgarTableFilters');
        if (filtersContainer) {
            filtersContainer.innerHTML = '';
            filtersContainer.style.display = 'none';
        }
        const statusDiv = document.getElementById('edgarStatusDiv');
        document.getElementById('edgarConceptListContainer').style.display = 'none';
        document.getElementById('edgarMultiFetchControls').style.display = 'none';
        document.getElementById('edgarOutputSubTabs').style.display = 'none';
        document.getElementById('edgarCustomTableContainer').style.display = 'none';
        document.getElementById('edgarConceptDataContainer').style.display = 'none';
        if (statusDiv) { statusDiv.textContent = message; statusDiv.className = `alert ${isWarning ? 'alert-warning' : 'alert-danger'} mt-3`; statusDiv.style.display = 'block';}
    }

    function displayEdgarCompanyInfo(data) {
        const conceptListHeaderSpan = document.getElementById('edgarConceptListHeaderSpan');
        const resultsDiv = document.getElementById('edgarResultsDiv');
        const statusDiv = document.getElementById('edgarStatusDiv');

        if (conceptListHeaderSpan) {
            if (data && data.ticker && data.title) {
                conceptListHeaderSpan.textContent = `${data.ticker} / ${data.title} - Available Data Concepts`;
            } else {
                conceptListHeaderSpan.textContent = 'Available Data Concepts';
            }
        }

        // Hide the old raw JSON display
        if (resultsDiv) {
            resultsDiv.style.display = 'none';
        }
        // Clear any previous raw JSON content if the edgarJsonOutput element exists
        const jsonOutput = document.getElementById('edgarJsonOutput');
        if (jsonOutput) {
            jsonOutput.textContent = ''; 
        }

        // Ensure status div is hidden as company info is now part of the header or processed
        if (statusDiv) {
            // Check if the status div is currently showing a CIK fetching message, 
            // and if so, defer hiding it slightly or let the next step (concept fetching) update it.
            // For now, we'll hide it if it wasn't already explicitly set to show an error/warning.
            if (!statusDiv.classList.contains('alert-danger') && !statusDiv.classList.contains('alert-warning')) {
                 // If it was an info message like "Fetching CIK...", it will be replaced by "Fetching concepts..."
                 // So, no explicit hide here might be better, or the next status update handles it.
            }
        }
    }

    // Setup Tab Event Listeners
    const rawJsonViewTabEl = document.getElementById('raw-json-view-tab');
    if (rawJsonViewTabEl) {
        rawJsonViewTabEl.addEventListener('shown.bs.tab', function (event) {
            const conceptDataContainerEl = document.getElementById('edgarConceptDataContainer');
            const conceptJsonOutputEl = document.getElementById('edgarConceptJsonOutput');
            const customTableContainerEl = document.getElementById('edgarCustomTableContainer');

            if (!conceptDataContainerEl || !conceptJsonOutputEl) {
                console.error("Raw JSON tab: UI elements missing.");
                return;
            }

            if (currentDisplayableRawData) {
                try {
                    conceptJsonOutputEl.textContent = JSON.stringify(currentDisplayableRawData, null, 2);
                    if (window.hljs && conceptJsonOutputEl.textContent) { hljs.highlightElement(conceptJsonOutputEl); }
                } catch (e) {
                    conceptJsonOutputEl.textContent = 'Error stringifying data for display. See console.';
                    console.error("Error stringifying currentDisplayableRawData for display:", e, currentDisplayableRawData);
                }
            } else {
                conceptJsonOutputEl.textContent = 'No data currently available for JSON view. (currentDisplayableRawData is null/undefined)';
            }
            conceptDataContainerEl.style.display = 'block';
            if (customTableContainerEl) customTableContainerEl.style.display = 'none';
        });
    }

    const tabularViewTabEl = document.getElementById('tabular-view-tab');
    if (tabularViewTabEl) {
        tabularViewTabEl.addEventListener('shown.bs.tab', function (event) {
            const conceptDataContainerEl = document.getElementById('edgarConceptDataContainer');
            const customTableContainerEl = document.getElementById('edgarCustomTableContainer');
            const filtersContainer = document.getElementById('edgarTableFilters'); // Get filters container

            if (conceptDataContainerEl) conceptDataContainerEl.style.display = 'none';
            
            if (customTableContainerEl && filtersContainer) { // Check both table and filter containers
                // Check if there's a DataTable instance and it has rows, or if there's content in tbody directly
                const isDataTableInitialized = $.fn.DataTable.isDataTable('#edgarCustomDataTable');
                const hasDataTableRows = isDataTableInitialized && edgarDataTableInstance && edgarDataTableInstance.rows().count() > 0;
                
                // Check if tbody has children and isn't just the "empty table" message from DataTables
                const tbody = customTableContainerEl.querySelector('#edgarCustomDataTable tbody');
                const hasDirectTbodyContent = tbody && tbody.hasChildNodes() && !(tbody.childNodes.length === 1 && tbody.firstChild.classList && tbody.firstChild.classList.contains('dataTables_empty'));

                if (hasDataTableRows || hasDirectTbodyContent) { 
                    customTableContainerEl.style.display = 'block'; 
                    filtersContainer.style.display = 'flex'; // Show filters if table has content
                } else if (isDataTableInitialized && !hasDataTableRows) { 
                     // If DT is initialized but shows "No data", still show table container (for the message) and filters
                    customTableContainerEl.style.display = 'block'; 
                    filtersContainer.style.display = 'flex';
                }
                 else if (!isDataTableInitialized && !hasDirectTbodyContent) { // No DT, no direct content
                     customTableContainerEl.style.display = 'none';
                     filtersContainer.style.display = 'none';
                }


                if (edgarDataTableInstance) {
                    setTimeout(() => {
                        try { edgarDataTableInstance.columns.adjust().draw(false); } catch(e) { console.warn("Error adjusting columns on tab show:", e); }
                    }, 50); // Small delay for tab transition
                }
            }
        });
    }

    // --- XLS Export Function ---
    // This function is used by the EDGAR export functionality
    function exportTableToXLS(tableId, filename) {
        const table = document.getElementById(tableId);
        if (!table || !table.rows || table.rows.length === 0) {
            console.error('Export Error: Table not found or empty for export:', tableId);
            alert('No data available to export.');
            return;
        }

        const tableClone = table.cloneNode(true);
        // Remove any interactive elements or classes not needed for export if any (e.g. tooltips)
        // For this table, it's fairly clean.

        let tableHTML = tableClone.outerHTML;

        const template = `
            <html xmlns:o="urn:schemas-microsoft-com:office:office"
                  xmlns:x="urn:schemas-microsoft-com:office:excel"
                  xmlns="http://www.w3.org/TR/REC-html40">
            <head>
                <meta charset="UTF-8">
                <!--[if gte mso 9]>
                <xml>
                    <x:ExcelWorkbook>
                        <x:ExcelWorksheets>
                            <x:ExcelWorksheet>
                                <x:Name>Sheet1</x:Name>
                                <x:WorksheetOptions><x:DisplayGridlines/></x:WorksheetOptions>
                            </x:ExcelWorksheet>
                        </x:ExcelWorksheets>
                    </x:ExcelWorkbook>
                </xml>
                <![endif]-->
                <style>
                    /* Basic styling for Excel */
                    table, th, td { border: 1px solid black; border-collapse: collapse; }
                    th, td { padding: 5px; text-align: left; }
                    th { background-color: #f2f2f2; }
                </style>
            </head>
            <body>
                ${tableHTML}
            </body>
            </html>`;

        const dataType = 'application/vnd.ms-excel';
        const blob = new Blob([template], { type: dataType });
        const downloadLink = document.createElement("a");

        document.body.appendChild(downloadLink); // Required for Firefox

        if (navigator.msSaveOrOpenBlob) { // For IE / Edge
            navigator.msSaveOrOpenBlob(blob, filename);
        } else {
            downloadLink.href = URL.createObjectURL(blob);
            downloadLink.download = filename;
            downloadLink.click();
            URL.revokeObjectURL(downloadLink.href); // Clean up
        }
        document.body.removeChild(downloadLink);
    }

    // Helper function for exporting table to XLS (accepts table node)
    function exportTableToXLSNode(tableNode, filename) {
        if (!tableNode || !tableNode.rows || tableNode.rows.length === 0) {
            alert('No data to export.'); return;
        }
        let tableHTML = tableNode.outerHTML;
        const template = `
            <html xmlns:o="urn:schemas-microsoft-com:office:office"
                  xmlns:x="urn:schemas-microsoft-com:office:excel"
                  xmlns="http://www.w3.org/TR/REC-html40">
            <head><meta charset="UTF-8">
                <!--[if gte mso 9]><xml>
                    <x:ExcelWorkbook><x:ExcelWorksheets><x:ExcelWorksheet>
                    <x:Name>Sheet1</x:Name>
                    <x:WorksheetOptions><x:DisplayGridlines/></x:WorksheetOptions>
                    </x:ExcelWorksheet></x:ExcelWorksheets></x:ExcelWorkbook>
                </xml><![endif]-->
                <style> table, th, td { border: 1px solid black; border-collapse: collapse; } th, td { padding: 5px; text-align: left; } th { background-color: #f2f2f2; } </style>
            </head><body>${tableHTML}</body></html>`;
        const dataType = 'application/vnd.ms-excel';
        const blob = new Blob([template], { type: dataType });
        const downloadLink = document.createElement("a");
        document.body.appendChild(downloadLink);
        if (navigator.msSaveOrOpenBlob) {
            navigator.msSaveOrOpenBlob(blob, filename);
        } else {
            downloadLink.href = URL.createObjectURL(blob);
            downloadLink.download = filename;
            downloadLink.click();
            URL.revokeObjectURL(downloadLink.href);
        }
        document.body.removeChild(downloadLink);
    }

    // New function to generate inferred Q4 data
    function generateInferredQ4Data(cleanedUnitEntries) {
        if (!Array.isArray(cleanedUnitEntries) || cleanedUnitEntries.length === 0) return [];

        // Initialize resultEntries with fp_actual set to original fp, and a unique ID for easier updates
        let resultEntries = cleanedUnitEntries.map((e, index) => ({ 
            ...e, 
            fp_actual: e.fp, // Default to original, will be updated
            _uniqueId: index // Helper for updating specific entries
        }));

        const isValidDateString = (dateStr) => dateStr && dateStr !== 'N/A' && dateStr !== 'null' && dateStr !== 'undefined';
        const parseDateSafe = (dateStr) => {
            if (!isValidDateString(dateStr)) return null;
            try { return new Date(dateStr + 'T00:00:00Z'); } 
            catch (e) { return null; }
        };
        const getDurationInDays = (startStr, endStr) => {
            const startDate = parseDateSafe(startStr);
            const endDate = parseDateSafe(endStr);
            if (!startDate || !endDate || startDate > endDate) return -1;
            return (endDate - startDate) / (1000 * 60 * 60 * 24);
        };
        const formatDateISO = (dateObj) => {
            if (!dateObj) return 'N/A';
            return dateObj.toISOString().split('T')[0];
        };

        // 1. Identify Potential True Annual Reports
        let potentialAnnuals = resultEntries.filter(e => {
            if (e._isRedundantDuplicate || e._isSupersededRestatement) return false;
            const duration = getDurationInDays(e.start, e.end);
            return duration >= 330 && duration <= 400; 
        });

        potentialAnnuals.sort((a, b) => { 
            const startA = parseDateSafe(a.start); const startB = parseDateSafe(b.start);
            const endA = parseDateSafe(a.end); const endB = parseDateSafe(b.end);
            if (!startA && !startB) return 0; if (!startA) return 1; if (!startB) return -1;
            if (startA < startB) return -1; if (startA > startB) return 1;
            if (!endA && !endB) return 0; if (!endA) return 1; if (!endB) return -1;
            if (endA < endB) return -1; if (endA > endB) return 1;
            return 0;
        });

        const trueAnnualReports = [];
        let lastAddedAnnualReportEndDate = null;
        for (const pa of potentialAnnuals) {
            const paStart = parseDateSafe(pa.start);
            if (!paStart) continue;
            if (trueAnnualReports.length === 0 || (lastAddedAnnualReportEndDate && paStart > lastAddedAnnualReportEndDate)) {
                trueAnnualReports.push(pa);
                lastAddedAnnualReportEndDate = parseDateSafe(pa.end);
            }
        }
        trueAnnualReports.sort((a,b) => parseDateSafe(b.end) - parseDateSafe(a.end)); 
        console.log('[Q4 Filler] Identified True Annual Reports (preliminary, non-overlapping):', trueAnnualReports.length);

        // Update fp_actual for these true annual reports
        trueAnnualReports.forEach(annualReport => {
            const indexInResult = resultEntries.findIndex(re => re._uniqueId === annualReport._uniqueId);
            if (indexInResult !== -1) {
                if (resultEntries[indexInResult].fp !== 'FY') {
                     console.log(`[Q4 Filler] Re-classifying entry (ACC: ${annualReport.accn}, Start: ${annualReport.start}) from ${resultEntries[indexInResult].fp} to FY (actual)`);
                }
                resultEntries[indexInResult].fp_actual = 'FY';
            }
        });

        // --- Main Loop for each True Annual Report to find quarters and infer Q4 ---
        trueAnnualReports.forEach(annualReport => {
            const annualReportStartDate = parseDateSafe(annualReport.start);
            const annualReportEndDate = parseDateSafe(annualReport.end);
            if (!annualReportStartDate || !annualReportEndDate) return;

            const effectiveFyForDisplay = annualReportEndDate.getUTCFullYear();
            console.log(`[Q4 Filler] Processing Annual Report (fp_actual: FY) ending ${formatDateISO(annualReportEndDate)} (Effective FY: ${effectiveFyForDisplay})`);

            // 2. Find/Re-classify Explicit Q4 for this annualReport period
            let explicitQ4Found = null;
            const q4Candidates = resultEntries.filter(e => {
                if (e._uniqueId === annualReport._uniqueId) return false; // Cannot be the annual report itself
                if (e._isRedundantDuplicate || e._isSupersededRestatement) return false;
                const qDuration = getDurationInDays(e.start, e.end);
                if (!(qDuration >= 70 && qDuration <= 110)) return false;
                const entryEnd = parseDateSafe(e.end);
                if (!entryEnd || entryEnd.getTime() !== annualReportEndDate.getTime()) return false;
                const entryStart = parseDateSafe(e.start);
                if (!entryStart) return false;
                const expectedQ4StartRough = new Date(annualReportEndDate);
                expectedQ4StartRough.setUTCMonth(expectedQ4StartRough.getUTCMonth() - 3.5);
                return entryStart >= expectedQ4StartRough && entryStart < annualReportEndDate;
            });
            if (q4Candidates.length > 0) {
                q4Candidates.sort((a,b) => parseDateSafe(b.filed) - parseDateSafe(a.filed)); // latest filed
                explicitQ4Found = q4Candidates[0];
                const indexInResult = resultEntries.findIndex(re => re._uniqueId === explicitQ4Found._uniqueId);
                if (indexInResult !== -1) {
                    if (resultEntries[indexInResult].fp !== 'Q4') {
                        console.log(`[Q4 Filler] Re-classifying entry (ACC: ${explicitQ4Found.accn}, Start: ${explicitQ4Found.start}) from ${resultEntries[indexInResult].fp} to Q4 (actual) for FY ending ${formatDateISO(annualReportEndDate)}`);
                    }
                    resultEntries[indexInResult].fp_actual = 'Q4';
                }
                console.log(`[Q4 Filler] Identified existing explicit Q4 (fp_actual: Q4) for FY ending ${formatDateISO(annualReportEndDate)}.`);
                // return; // Continue to classify Q1,Q2,Q3 even if explicit Q4 found, for their fp_actual
            }

            // 3. Find/Re-classify Q1, Q2, Q3
            let q1Found = null, q2Found = null, q3Found = null;

            const findAndClassifyQuarter = (targetQuarterLabel, expectedStartDateObj, prevQuarterEndDateObj) => {
                let candidates = resultEntries.filter(e => {
                    if (e._uniqueId === annualReport._uniqueId || (explicitQ4Found && e._uniqueId === explicitQ4Found._uniqueId)) return false;
                    if (e._isRedundantDuplicate || e._isSupersededRestatement || e._isCumulative) return false;
                    const qDuration = getDurationInDays(e.start, e.end);
                    if (!(qDuration >= 70 && qDuration <= 110)) return false;
                    const entryStart = parseDateSafe(e.start); const entryEnd = parseDateSafe(e.end);
                    if (!entryStart || !entryEnd || entryEnd >= annualReportEndDate) return false;

                    if (targetQuarterLabel === 'Q1') {
                        if (!expectedStartDateObj || entryStart.getTime() !== expectedStartDateObj.getTime()) return false;
                    } else if (prevQuarterEndDateObj) { 
                        const dayAfterPrev = new Date(prevQuarterEndDateObj); dayAfterPrev.setUTCDate(dayAfterPrev.getUTCDate() + 1);
                        const sevenDaysAfter = new Date(dayAfterPrev); sevenDaysAfter.setUTCDate(sevenDaysAfter.getUTCDate() + 7);
                        if (!(entryStart >= dayAfterPrev && entryStart <= sevenDaysAfter)) return false;
                    }
                    return entryStart < entryEnd;
                });
                
                if (candidates.length === 0) return null;
                candidates.sort((a,b) => {
                    const fpA = String(a.fp).toUpperCase(); const fpB = String(b.fp).toUpperCase();
                    if (fpA === targetQuarterLabel && fpB !== targetQuarterLabel) return -1;
                    if (fpA !== targetQuarterLabel && fpB === targetQuarterLabel) return 1;
                    return parseDateSafe(b.filed) - parseDateSafe(a.filed); // then by latest filed
                }); 
                
                const foundQuarter = candidates[0];
                const indexInResult = resultEntries.findIndex(re => re._uniqueId === foundQuarter._uniqueId);
                if (indexInResult !== -1) {
                    if (resultEntries[indexInResult].fp !== targetQuarterLabel) {
                         console.log(`[Q4 Filler] Re-classifying entry (ACC: ${foundQuarter.accn}, Start: ${foundQuarter.start}) from ${resultEntries[indexInResult].fp} to ${targetQuarterLabel} (actual) for FY ending ${formatDateISO(annualReportEndDate)}`);
                    }
                    resultEntries[indexInResult].fp_actual = targetQuarterLabel;
                }
                return foundQuarter;
            };

            q1Found = findAndClassifyQuarter('Q1', annualReportStartDate, null);
            if (q1Found) {
                const q1End = parseDateSafe(q1Found.end);
                if (q1End && (!explicitQ4Found || parseDateSafe(explicitQ4Found.start) > q1End)) { // Q2 must start before explicit Q4
                     q2Found = findAndClassifyQuarter('Q2', null, q1End);
                }
            }
            if (q2Found) {
                const q2End = parseDateSafe(q2Found.end);
                if (q2End && (!explicitQ4Found || parseDateSafe(explicitQ4Found.start) > q2End)) { // Q3 must start before explicit Q4
                    q3Found = findAndClassifyQuarter('Q3', null, q2End);
                }
            }
            
            // Simple distinctness check - primarily for cases where the same entry might be picked if Q_fp tags are missing
            // This might need more robust handling if the same data row is picked for multiple quarters despite different parameters to findAndClassifyQuarter
            const foundQuarters = [q1Found, q2Found, q3Found].filter(Boolean);
            const uniqueFoundQuarters = [...new Set(foundQuarters.map(q => q._uniqueId))];
            if (foundQuarters.length !== uniqueFoundQuarters.length) {
                console.warn(`[Q4 Filler] Duplicate quarters selected for FY ending ${formatDateISO(annualReportEndDate)}. Q1/Q2/Q3 fp_actual might be unreliable.`, {q1:q1Found?q1Found._uniqueId:null, q2:q2Found?q2Found._uniqueId:null, q3:q3Found?q3Found._uniqueId:null});
                // Resetting to null if not distinct to prevent wrong Q4 calc. A better recovery might be needed.
                if (q1Found && q2Found && q1Found._uniqueId === q2Found._uniqueId) q2Found = null;
                if (q2Found && q3Found && q2Found._uniqueId === q3Found._uniqueId) q3Found = null;
                if (q1Found && q3Found && q1Found._uniqueId === q3Found._uniqueId) q3Found = null; // q1 vs q3 just in case
            }

            console.log(`[Q4 Filler] For Annual Report ending ${formatDateISO(annualReportEndDate)} (fp_actual: FY): Q1_actual=${q1Found ? 'Q1' :'N'}, Q2_actual=${q2Found ? 'Q2' :'N'}, Q3_actual=${q3Found ? 'Q3' :'N'}`);

            // 4. Infer Q4 (only if no explicit Q4 was found and classified)
            if (!explicitQ4Found && q1Found && q2Found && q3Found && annualReport) {
                try {
                    const q1Val = parseFloat(q1Found.val);
                    const q2Val = parseFloat(q2Found.val);
                    const q3Val = parseFloat(q3Found.val);
                    const fyVal = parseFloat(annualReport.val);

                    if (![q1Val, q2Val, q3Val, fyVal].some(isNaN)) {
                        const q4Val = fyVal - (q1Val + q2Val + q3Val);
                        let q4StartDateObj = parseDateSafe(q3Found.end);
                        if (q4StartDateObj) q4StartDateObj.setUTCDate(q4StartDateObj.getUTCDate() + 1);
                        
                        const q4InferredEntry = {
                            ...annualReport, 
                            _displayLabel: annualReport._displayLabel || 'Inferred Data',
                            unit: annualReport.unit,
                            fp: '', // Original fp for an inferred entry
                            fp_actual: 'Q4', // Actual fp is Q4
                            val: q4Val.toFixed(4), 
                            start: formatDateISO(q4StartDateObj),
                            end: annualReport.end, 
                            fy: effectiveFyForDisplay, 
                            _isInferredQ4: true,
                            _uniqueId: `inferred_${annualReport.accn}_${annualReport.start}_${annualReport.end}`, // Unique ID for inferred
                            _isCumulative: false, _isRedundantDuplicate: false, _isSupersededRestatement: false, _isRestatement: false
                        };

                        const alreadyExists = resultEntries.find(e => e._isInferredQ4 && e._uniqueId === q4InferredEntry._uniqueId );
                        if (!alreadyExists) {
                            resultEntries.push(q4InferredEntry);
                            console.log(`[Q4 Filler] Inferred Q4 for period ${q4InferredEntry.start} to ${q4InferredEntry.end} (Effective FY ${effectiveFyForDisplay}): Val ${q4InferredEntry.val}`);
                        } else {
                            console.log(`[Q4 Filler] Inferred Q4 (Effective FY ${effectiveFyForDisplay}) already added. Skipping duplicate.`);
                        }
                    } else {
                        console.warn(`[Q4 Filler] Values for calculation are NaN for Effective FY ${effectiveFyForDisplay}.`);
                    }
                } catch (e) {
                    console.warn(`[Q4 Filler] Error during Q4 calculation for Effective FY ${effectiveFyForDisplay}:`, e);
                }
            } else if (!explicitQ4Found) {
                console.log(`[Q4 Filler] Missing Q1,Q2,or Q3 for Annual Report ending ${formatDateISO(annualReportEndDate)} AND no explicit Q4. Cannot infer Q4.`);
            }
        });
        return resultEntries;
    }

    function displayEdgarBarChart() {
        if (!edgarDataTableInstance) {
            alert('No table data available to chart.');
            return;
        }

        const tableData = edgarDataTableInstance.rows({ search: 'applied' }).data().toArray();

        if (!tableData || tableData.length === 0) {
            alert('No data in the table to chart.');
            return;
        }

        const conceptColors = [
            { background: 'rgba(54, 162, 235, 0.7)', border: 'rgba(54, 162, 235, 1)' }, // Blue
            { background: 'rgba(255, 99, 132, 0.7)', border: 'rgba(255, 99, 132, 1)' },  // Red
            { background: 'rgba(75, 192, 192, 0.7)', border: 'rgba(75, 192, 192, 1)' },  // Teal
            { background: 'rgba(255, 206, 86, 0.7)', border: 'rgba(255, 206, 86, 1)' },  // Yellow
            { background: 'rgba(153, 102, 255, 0.7)', border: 'rgba(153, 102, 255, 1)'}, // Purple
            { background: 'rgba(255, 159, 64, 0.7)', border: 'rgba(255, 159, 64, 1)' },  // Orange
            { background: 'rgba(201, 203, 207, 0.7)', border: 'rgba(201, 203, 207, 1)' }   // Grey
        ];

        const dataByConcept = {};
        const allEndDates = new Set();

        tableData.forEach(row => {
            const conceptLabel = row._displayLabel; // This should be the clean concept name
            const endDate = row.end;
            const value = parseFloat(row.val);
            const fpActual = row.fp_actual || 'N/A'; // Default if fp_actual is missing

            if (conceptLabel && endDate && !isNaN(value)) {
                if (!dataByConcept[conceptLabel]) {
                    dataByConcept[conceptLabel] = [];
                }
                dataByConcept[conceptLabel].push({ endDate: endDate, value: value, fp_actual: fpActual });
                allEndDates.add(endDate);
            }
        });

        const sortedEndDates = Array.from(allEndDates).sort((a, b) => new Date(a) - new Date(b));

        const datasets = [];
        let colorIndex = 0; 

        for (const conceptLabel in dataByConcept) {
            if (Object.hasOwnProperty.call(dataByConcept, conceptLabel)) {
                const conceptValues = dataByConcept[conceptLabel].sort((a,b) => new Date(a.endDate) - new Date(b.endDate));
                
                const dataPoints = [];
                const fiscalPeriods = []; // Store fiscal period metadata here

                sortedEndDates.forEach(date => {
                    const point = conceptValues.find(cv => cv.endDate === date);
                    if (point) {
                        dataPoints.push(point.value);
                        fiscalPeriods.push(point.fp_actual);
                    } else {
                        dataPoints.push(null);
                        fiscalPeriods.push(null);
                    }
                });

                const colorInfo = conceptColors[colorIndex % conceptColors.length];
                colorIndex++;

                datasets.push({
                    label: conceptLabel,
                    data: dataPoints, // Simple array of numbers/nulls
                    fiscalPeriods: fiscalPeriods, // Custom property for metadata
                    backgroundColor: colorInfo.background,
                    borderColor: colorInfo.border,
                    borderWidth: 1
                });
            }
        }

        if (datasets.length === 0) {
            alert('No chartable data found. Ensure concepts have numerical values and end dates.');
            return;
        }

        const ctx = document.getElementById('edgarBarChartCanvas').getContext('2d');

        if (edgarBarChartInstance) {
            edgarBarChartInstance.destroy();
        }

        edgarBarChartInstance = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: sortedEndDates,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false, // Disable all animations
                hover: { 
                    mode: 'nearest', 
                    intersect: true, 
                    animationDuration: 0 // Disable animation on hover
                },
                responsiveAnimationDuration: 0, // Disable responsive animation
                plugins: {
                    legend: {
                        position: 'top',
                    },
                    title: {
                        display: true,
                        text: 'EDGAR Concept Values by End Date'
                    },
                    tooltip: {
                        animation: false, // Disable tooltip animation
                        callbacks: {
                            label: function(context) {
                                let label = context.dataset.label || '';
                                if (label) {
                                    label += ': ';
                                }
                                if (context.parsed.y !== null) {
                                    label += context.parsed.y.toLocaleString();
                                }
                                // Add fiscal period to the tooltip from our custom array
                                if (context.dataset.fiscalPeriods && context.dataset.fiscalPeriods[context.dataIndex]) {
                                    label += ` (${context.dataset.fiscalPeriods[context.dataIndex]})`;
                                }
                                return label;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'End Date'
                        },
                        stacked: false, 
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Value'
                        },
                        beginAtZero: true,
                        stacked: false, 
                        ticks: {
                            callback: function(value, index, values) {
                                return value.toLocaleString();
                            }
                        }
                    }
                },
                parsing: {
                     yAxisKey: 'y' // Tell Chart.js to find the value in the 'y' property of our data objects
                }
            }
        });

        const modal = new bootstrap.Modal(edgarChartModal);

        // Remove the custom color key logic as it's no longer needed
        const colorKeyDiv = document.getElementById('edgarChartColorKey');
        if (colorKeyDiv) {
            colorKeyDiv.innerHTML = ''; 
        }

        modal.show();
    }
}); 