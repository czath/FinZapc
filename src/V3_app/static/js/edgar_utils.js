document.addEventListener('DOMContentLoaded', function () {
    let currentDisplayableRawData = null; // Holds the latest fetched raw data (single object or array)
    const edgarCleanupToggle = document.getElementById('edgarCleanupToggle');
    const edgarConceptSearchInput = document.getElementById('edgarConceptSearchInput'); // Get search input
    let edgarDataTableInstance = null; // For DataTables

    // Helper function to escape regex special characters
    function escapeRegex(string) {
        return string.replace(/[-\/\\\\^$*+?.()|[\\]{}]/g, '\\\\$&');
    }

    // Attempt to attach export button listener early
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
                    if (colIdx === 7 && typeof cellData !== 'undefined') { // Value column
                        const numericVal = parseFloat(cellData);
                        if (!isNaN(numericVal)) {
                            displayData = numericVal.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 4 });
                        } else {
                            displayData = cellData;
                        }
                    }
                    tr.insertCell().textContent = displayData !== undefined ? displayData : '';
                });
            });
            exportTableToXLSNode(tempTable, filename);
        });
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
                        statusDiv.style.display = 'block';
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
                            if (concept.label && concept.description) {
                                conceptsWithDetails.push({
                                    label: concept.label,
                                    description: concept.description,
                                    conceptName: conceptName, 
                                    taxonomy: taxonomy 
                                });
                            }
                        }
                    }
                }
            }
        }

        conceptsWithDetails.sort((a, b) => a.label.localeCompare(b.label));

        const companyTicker = currentEdgarExportContext.ticker || 'Company';
        const companyTitle = currentEdgarExportContext.companyTitle || 'N/A';

        if (conceptsWithDetails.length > 0) {
            conceptListHeaderSpan.textContent = `${companyTicker} (${companyTitle}) - ${conceptsWithDetails.length} Available Data Concepts`;
            
            displayConceptList(conceptsWithDetails, cik); // This will show the concept list container and other relevant UI
            
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
            conceptListUl.innerHTML = ''; // Ensure list is empty
            
            // Hide elements that depend on concepts being present (handled by displayConceptList with empty array)
            displayConceptList([], cik); 
        }
    }

    function displayConceptList(conceptsWithDetails, cik) {
        const conceptListUl = document.getElementById('edgarConceptList');
        const multiFetchControls = document.getElementById('edgarMultiFetchControls');
        const edgarOutputSubTabs = document.getElementById('edgarOutputSubTabs');
        const exportEdgarCustomTableBtn = document.getElementById('exportEdgarCustomTableBtn'); 
        const conceptSearchInput = document.getElementById('edgarConceptSearchInput');
        const conceptListContainer = document.getElementById('edgarConceptListContainer'); // Added to ensure it is controlled

        if (!conceptListUl || !conceptListContainer) { // Added check for container
            console.error("displayConceptList: Core UI elements missing.");
            return;
        }
        conceptListUl.innerHTML = '';

        if (conceptsWithDetails.length > 0) {
            conceptListContainer.style.display = 'block'; // Ensure container is visible
            if (multiFetchControls) multiFetchControls.style.display = 'block';
            if (edgarOutputSubTabs) edgarOutputSubTabs.style.display = 'flex';
            if (conceptSearchInput) conceptSearchInput.style.display = 'block';
        } else {
            // If no concepts, ensure the main container is visible (for header) but list specific items are hidden
            conceptListContainer.style.display = 'block'; 
            if (multiFetchControls) multiFetchControls.style.display = 'none';
            if (edgarOutputSubTabs) edgarOutputSubTabs.style.display = 'none';
            if (exportEdgarCustomTableBtn) exportEdgarCustomTableBtn.style.display = 'none';
            if (conceptSearchInput) conceptSearchInput.style.display = 'none';
        }

        conceptsWithDetails.forEach(conceptDetail => {
            const listItem = document.createElement('li');
            listItem.className = 'list-group-item';

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'form-check-input me-2';
            checkbox.value = conceptDetail.conceptName;
            checkbox.dataset.cik = cik;
            checkbox.dataset.taxonomy = conceptDetail.taxonomy;
            checkbox.dataset.conceptName = conceptDetail.conceptName;
            checkbox.dataset.label = conceptDetail.label;
            checkbox.dataset.description = conceptDetail.description;
            
            const labelElement = document.createElement('label');
            labelElement.className = 'form-check-label';
            labelElement.style.cursor = 'pointer';

            const strong = document.createElement('strong');
            strong.textContent = conceptDetail.label;
            labelElement.appendChild(strong);
            
            const small = document.createElement('small');
            small.className = 'd-block text-muted';
            small.textContent = `(${conceptDetail.taxonomy} - ${conceptDetail.conceptName}): ${conceptDetail.description}`;
            labelElement.appendChild(small);

            listItem.appendChild(checkbox);
            listItem.appendChild(labelElement);

            labelElement.addEventListener('click', () => {
                const rawJsonTabButton = document.getElementById('raw-json-view-tab');
                if (rawJsonTabButton) {
                    // Store data on the tab button for the event listener to pick up
                    rawJsonTabButton.dataset.cik = cik;
                    rawJsonTabButton.dataset.taxonomy = conceptDetail.taxonomy;
                    rawJsonTabButton.dataset.conceptName = conceptDetail.conceptName;
                    rawJsonTabButton.dataset.label = conceptDetail.label;
                    bootstrap.Tab.getOrCreateInstance(rawJsonTabButton).show();
                    // fetchAndDisplayConceptData is now called by the 'shown.bs.tab' event
                }
            });

            listItem.addEventListener('click', (event) => {
                if (event.target !== checkbox && event.target !== labelElement && !labelElement.contains(event.target)) {
                    checkbox.checked = !checkbox.checked;
                }
            });

            conceptListUl.appendChild(listItem);
        });
    }

    // Event listener for the concept search input
    if (edgarConceptSearchInput) {
        edgarConceptSearchInput.addEventListener('input', function() {
            const searchTerm = this.value.toLowerCase().trim();
            const conceptListUl = document.getElementById('edgarConceptList');
            if (!conceptListUl) return;

            const listItems = conceptListUl.getElementsByTagName('li');
            Array.from(listItems).forEach(item => {
                const checkbox = item.querySelector('input[type="checkbox"]');
                let textToSearch = '';
                if (checkbox) {
                    // Use data attributes for a more robust search source
                    const label = checkbox.dataset.label || '';
                    const conceptName = checkbox.dataset.conceptName || '';
                    const description = checkbox.dataset.description || '';
                    const taxonomy = checkbox.dataset.taxonomy || '';
                    textToSearch = `${label} ${conceptName} ${description} ${taxonomy}`.toLowerCase();
                } else {
                    // Fallback to item text content if somehow checkbox or data attributes are missing
                    textToSearch = item.textContent.toLowerCase();
                }
                
                if (textToSearch.includes(searchTerm)) {
                    item.style.display = ''; // Show item
                } else {
                    item.style.display = 'none'; // Hide item
                }
            });
        });
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
        const exportBtn = document.getElementById('exportEdgarCustomTableBtn');
        const cleanupToggle = document.getElementById('edgarCleanupToggle');
        const applyCleanup = cleanupToggle ? cleanupToggle.checked : true;
        const filtersContainer = document.getElementById('edgarTableFilters'); // Get the new filters container

        if (!tableElement || !tableContainer || !exportBtn || !filtersContainer) { 
            console.error("Table UI or filters container missing"); 
            // Potentially hide all related UI elements if critical ones are missing
            if(tableContainer) tableContainer.style.display = 'none';
            if(exportBtn) exportBtn.style.display = 'none';
            if(filtersContainer) filtersContainer.style.display = 'none';
            return; 
        }
        
        // Ensure allConceptsData is an array before proceeding to map/forEach etc.
        if (!Array.isArray(allConceptsData)) {
            console.error("populateCustomEdgarTable: allConceptsData is not an array.", allConceptsData);
            const statusDivError = document.getElementById('edgarStatusDiv');
            if (statusDivError) {
                statusDivError.textContent = 'Error: Cannot display table. Data is in an unexpected format.';
                statusDivError.className = 'alert alert-danger mt-3';
                statusDivError.style.display = 'block';
            }
            // Attempt to clear/reset DataTable if it exists and data is bad
            if (edgarDataTableInstance) { edgarDataTableInstance.clear().draw(); 
            } else { $(tableElement).find('tbody').empty().html('<tr><td colspan="8">Error: Data format issue.</td></tr>'); }
            filtersContainer.style.display = 'none'; // Hide filters if data error
            tableContainer.style.display = 'block'; // Show table container for error message display
            exportBtn.style.display = 'none';
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

                    let entriesToDisplay = flaggedEntries;
                    if (applyCleanup) {
                        entriesToDisplay = flaggedEntries.filter(e => 
                            !e._isCumulative && !e._isRedundantDuplicate && !e._isSupersededRestatement
                        );
                    }
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
        filtersContainer.innerHTML = ''; // Clear previous filters before adding new ones

        if (!conceptHasVisibleRowsOverall && tableDataForDataTable.length === 0) {
            filtersContainer.style.display = 'none'; // Hide filters if no data
            tableContainer.style.display = 'block'; // Show table for "no data" message
            exportBtn.style.display = 'none';
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
                    { title: 'Concept' }, { title: 'Unit' }, { title: 'FP' }, 
                    { title: 'Date of Filing' }, { title: 'Form' }, 
                    { title: 'Start Date' }, { title: 'End Date' }, { title: 'Value' }
                ],
                destroy: true,
                searching: true, 
                paging: true, 
                lengthChange: true,
                lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, "All"]],
                pageLength: 10, 
                language: { emptyTable: "No data available for the selected criteria." }
             });
            return; 
        }
        
        // Create and append dropdown filters
        const conceptValues = [...new Set(tableDataForDataTable.map(item => item._displayLabel))].sort();
        const formValues = [...new Set(tableDataForDataTable.map(item => item.form))].sort();

        const conceptFilterCol = document.createElement('div');
        conceptFilterCol.className = 'col-md-4'; // Bootstrap column class
        conceptFilterCol.innerHTML = `
            <label for="edgarConceptFilterSelect" class="form-label form-label-sm">Filter Concept:</label>
            <select id="edgarConceptFilterSelect" class="form-select form-select-sm">
                <option value="">All Concepts</option>
                ${conceptValues.map(val => `<option value="${val.replace(/"/g, '&quot;')}">${val}</option>`).join('')}
            </select>
        `;
        filtersContainer.appendChild(conceptFilterCol);

        const formFilterCol = document.createElement('div');
        formFilterCol.className = 'col-md-3'; // Bootstrap column class
        formFilterCol.innerHTML = `
            <label for="edgarFormFilterSelect" class="form-label form-label-sm">Filter Form:</label>
            <select id="edgarFormFilterSelect" class="form-select form-select-sm">
                <option value="">All Forms</option>
                ${formValues.map(val => `<option value="${val.replace(/"/g, '&quot;')}">${val}</option>`).join('')}
            </select>
        `;
        filtersContainer.appendChild(formFilterCol);
        filtersContainer.style.display = 'flex'; // Show the filters container rows using flex
        
        tableContainer.style.display = 'block';
        exportBtn.style.display = 'inline-block';
        const statusDiv = document.getElementById('edgarStatusDiv');
        if (statusDiv) statusDiv.style.display = 'none';

        edgarDataTableInstance = $(tableElement).DataTable({
            data: tableDataForDataTable,
            columns: [
                { 
                    data: '_displayLabel', 
                    title: 'Concept',
                    render: function(data, type, row) {
                        if (type === 'display') {
                            let html = '';
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
                { data: 'filed', title: 'Date of Filing', defaultContent: 'N/A'  },
                { data: 'form', title: 'Form', defaultContent: 'N/A'  },
                { data: 'start', title: 'Start Date', defaultContent: 'N/A' },
                { data: 'end', title: 'End Date', defaultContent: 'N/A' },
                {
                    data: 'val',
                    title: 'Value',
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
            destroy: true,
            searching: true, 
            paging: true, 
            lengthChange: true,
            lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, "All"]],
            pageLength: 10, 
            order: [[6, 'desc']], 
            rowCallback: function(row, data, index) {
                if (!applyCleanup) {
                    if (data._isCumulative) $(row).addClass('cumulative-row');
                    if (data._isRedundantDuplicate) $(row).addClass('redundant-row');
                    if (data._isSupersededRestatement) $(row).addClass('superseded-row');
                }
            },
            initComplete: function () {
                const api = this.api();
                $(api.table().footer()).empty(); // Ensure tfoot is empty, if it exists (it shouldn't)
            }
        });

        // Add event listeners for the new select dropdowns
        document.getElementById('edgarConceptFilterSelect').addEventListener('change', function() {
            const rawValue = this.value;
            const searchRegex = rawValue ? '^' + escapeRegex(rawValue) + '$' : '';
            edgarDataTableInstance.column(0).search(searchRegex, true, false).draw();
        });
        document.getElementById('edgarFormFilterSelect').addEventListener('change', function() {
            const rawValue = this.value;
            const searchRegex = rawValue ? '^' + escapeRegex(rawValue) + '$' : '';
            edgarDataTableInstance.column(4).search(searchRegex, true, false).draw();
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
        const exportCustomBtn = document.getElementById('exportEdgarCustomTableBtn'); if(exportCustomBtn) exportCustomBtn.style.display = 'none';
        const edgarOutputSubTabs = document.getElementById('edgarOutputSubTabs'); if(edgarOutputSubTabs) edgarOutputSubTabs.style.display = 'none';
        
        // Clear and hide new dropdown filters container
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
}); 