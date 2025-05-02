document.addEventListener('DOMContentLoaded', function() {
    console.log("AnalyticsPostTransformModule: DOMContentLoaded event fired.");

    // --- State Variables ---
    let postTransformFieldEnabledStatus = {};
    let postTransformNumericFieldFormats = {};
    let postTransformFieldInfoTips = {};

    // --- Storage Key Constants ---
    const POST_TRANSFORM_FIELD_ENABLED_STORAGE_KEY = 'analyticsPostTransformFieldEnabled';
    const POST_TRANSFORM_NUMERIC_FORMAT_STORAGE_KEY = 'analyticsPostTransformNumericFormats';
    const POST_TRANSFORM_FIELD_INFO_TIPS_STORAGE_KEY = 'analyticsPostTransformFieldInfoTips';

    // --- Element References ---
    const container = document.getElementById('post-transform-field-config-container');
    const tableBody = document.getElementById('post-transform-field-config-tbody');
    const searchInput = document.getElementById('post-transform-field-search');

    // --- Core Functions ---

    function loadPostTransformEnabledStatusFromStorage() {
        const rawData = localStorage.getItem(POST_TRANSFORM_FIELD_ENABLED_STORAGE_KEY);
        if (rawData) {
            try {
                const parsed = JSON.parse(rawData);
                if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                    postTransformFieldEnabledStatus = parsed;
                    console.log("[PostTransform] Loaded enabled status:", postTransformFieldEnabledStatus);
                } else {
                    console.warn(`[PostTransform] Invalid enabled status data in localStorage. Expected object.`);
                    postTransformFieldEnabledStatus = {};
                }
            } catch (e) {
                console.error(`[PostTransform] Error parsing enabled status from localStorage:`, e);
                postTransformFieldEnabledStatus = {};
            }
        } else {
            postTransformFieldEnabledStatus = {}; // Initialize empty if not found
        }
    }

    function savePostTransformEnabledStatusToStorage() {
        try {
            localStorage.setItem(POST_TRANSFORM_FIELD_ENABLED_STORAGE_KEY, JSON.stringify(postTransformFieldEnabledStatus));
            console.log("[PostTransform] Saved enabled status:", postTransformFieldEnabledStatus);
        } catch (e) {
            console.error("[PostTransform] Error saving enabled status to localStorage:", e);
        }
    }

    function loadPostTransformNumericFormatsFromStorage() {
        const rawData = localStorage.getItem(POST_TRANSFORM_NUMERIC_FORMAT_STORAGE_KEY);
        if (rawData) {
            try {
                const parsed = JSON.parse(rawData);
                if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                    postTransformNumericFieldFormats = parsed;
                    console.log("[PostTransform] Loaded numeric formats:", postTransformNumericFieldFormats);
                } else {
                    console.warn(`[PostTransform] Invalid numeric formats data in localStorage. Expected object.`);
                    postTransformNumericFieldFormats = {};
                }
            } catch (e) {
                console.error(`[PostTransform] Error parsing numeric formats from localStorage:`, e);
                postTransformNumericFieldFormats = {};
            }
        } else {
            postTransformNumericFieldFormats = {}; // Initialize empty if not found
        }
    }

    function savePostTransformNumericFormatsToStorage() {
        try {
            localStorage.setItem(POST_TRANSFORM_NUMERIC_FORMAT_STORAGE_KEY, JSON.stringify(postTransformNumericFieldFormats));
            console.log("[PostTransform] Saved numeric formats:", postTransformNumericFieldFormats);
        } catch (e) {
            console.error("[PostTransform] Error saving numeric formats to localStorage:", e);
        }
    }

    function loadPostTransformInfoTipsFromStorage() {
        const rawData = localStorage.getItem(POST_TRANSFORM_FIELD_INFO_TIPS_STORAGE_KEY);
        if (rawData) {
            try {
                const parsed = JSON.parse(rawData);
                if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                    postTransformFieldInfoTips = parsed;
                    console.log("[PostTransform] Loaded info tips:", postTransformFieldInfoTips);
                } else {
                    console.warn(`[PostTransform] Invalid info tips data in localStorage. Expected object.`);
                    postTransformFieldInfoTips = {};
                }
            } catch (e) {
                console.error(`[PostTransform] Error parsing info tips from localStorage:`, e);
                postTransformFieldInfoTips = {};
            }
        } else {
            postTransformFieldInfoTips = {}; // Initialize empty if not found
        }
    }

    function savePostTransformInfoTipsToStorage() {
        try {
            localStorage.setItem(POST_TRANSFORM_FIELD_INFO_TIPS_STORAGE_KEY, JSON.stringify(postTransformFieldInfoTips));
            console.log("[PostTransform] Saved info tips:", postTransformFieldInfoTips);
        } catch (e) {
            console.error("[PostTransform] Error saving info tips to localStorage:", e);
        }
    }

    function renderPostTransformFieldConfigUI() {
        console.log("[PostTransform] Rendering Post-Transform Field Config UI...");
        if (!tableBody) {
            console.error("[PostTransform] Table body element (#post-transform-field-config-tbody) not found.");
            return;
        }

        // --- Get Data from Main Module (Assume it's exposed) ---
        const mainModule = window.AnalyticsMainModule;
        const finalFieldMetadata = typeof mainModule?.getFinalFieldMetadata === 'function' ? mainModule.getFinalFieldMetadata() : {};
        const finalAvailableFields = typeof mainModule?.getFinalAvailableFields === 'function' ? mainModule.getFinalAvailableFields() : [];
        const availableFormatOptions = mainModule?.formatOptions || { default: 'Default' }; // Format dropdown options
        const openModalFn = mainModule?.openNumericFormatModal; // Function to open the format modal
        const fieldMetadata = finalFieldMetadata || {}; // Use variable from getter
        const fields = finalAvailableFields || [];   // Use variable from getter
        // --- End Get Data ---

        // --- Get PRE-transform status (for inheritance) ---
        // <<< FIX: Initialize Post-Transform Status for New/Missing Fields >>>
        let statusChanged = false;
        const currentPostTransformStatus = postTransformFieldEnabledStatus; // Get current state
        if (Array.isArray(fields)) {
            fields.forEach(fieldName => {
                if (!currentPostTransformStatus.hasOwnProperty(fieldName)) {
                    console.log(`[PostTransform UI Render] Initializing enabled status for new/missing field '${fieldName}' to true.`);
                    currentPostTransformStatus[fieldName] = true; // Default new fields to true
                    statusChanged = true;
                }
            });
        }
        if (statusChanged) {
            postTransformFieldEnabledStatus = currentPostTransformStatus; // Update module state
            savePostTransformEnabledStatusToStorage(); // Save the updated state
            console.log("[PostTransform UI Render] Saved updated enabled status with defaults.");
        }
        // <<< END FIX >>>

        const preTransformEnabledStatus = typeof mainModule?.getFieldEnabledStatus === 'function' ? mainModule.getFieldEnabledStatus() : {};

        // --- Get PRE-transform format/tips (for inheritance) --- CORRECTED GETTER CALLS ---
        const preTransformFormats = (typeof mainModule?.getNumericFieldFormats === 'function') ? mainModule.getNumericFieldFormats() : {}; // Call explicit getter
        const preTransformTips = (typeof mainModule?.getFieldInfoTips === 'function') ? mainModule.getFieldInfoTips() : {};         // Call explicit getter
        console.log("[PostTransform UI] Received preTransformTips object:", JSON.parse(JSON.stringify(preTransformTips)));
        // --- END CORRECTION ---

        tableBody.innerHTML = ''; // Clear existing rows

        if (!fields || fields.length === 0) {
            console.log("[PostTransform] No final fields data available to render config.");
            tableBody.innerHTML = '<tr><td colspan="4" class="text-muted small text-center">Run transformations to generate fields.</td></tr>'; // Adjusted colspan
            return;
        }

        if (!mainModule) {
            console.warn("[PostTransform] window.AnalyticsMainModule not found. Cannot access pre-transform settings or modal function.");
            // Render without inheritance/modal functionality, maybe add a warning?
        }

        console.log(`[PostTransform] Rendering config for ${fields.length} fields.`);

        fields.forEach(fieldName => {
            const row = tableBody.insertRow();
            row.dataset.fieldName = fieldName; // Store field name for event handlers
            const fieldMeta = fieldMetadata[fieldName] || {};
            const isNumeric = fieldMeta.type === 'numeric';

            // --- Determine Inheritance Status --- //
            let isEnabledInherited = false;
            let isFormatInherited = false;
            let isTipInherited = false;
            // ---

            // --- Corrected Inheritance Logic for Enabled Status ---
            let isEnabled;
            if (postTransformFieldEnabledStatus.hasOwnProperty(fieldName)) {
                isEnabled = postTransformFieldEnabledStatus[fieldName];
                // isEnabledInherited remains false
            } else if (preTransformEnabledStatus.hasOwnProperty(fieldName)) {
                isEnabled = preTransformEnabledStatus[fieldName];
                isEnabledInherited = true; // Inherited!
            } else {
                isEnabled = true;
                // isEnabledInherited remains false (default for new field)
            }
            // --- End Corrected Logic ---

            // --- Inheritance Logic for Format/Tip (Uses pre-transform as fallback) ---
            const inheritedFormat = preTransformFormats[fieldName];
            const inheritedTip = preTransformTips[fieldName];

            let currentFormat = postTransformNumericFieldFormats.hasOwnProperty(fieldName)
                ? postTransformNumericFieldFormats[fieldName]
                : (inheritedFormat || 'default');
            if (!postTransformNumericFieldFormats.hasOwnProperty(fieldName) && inheritedFormat) {
                isFormatInherited = true; // Inherited Format!
            }

            // --- Tip Inheritance Logic (Refined Check) ---
            let currentTip = '';
            const postTipValue = postTransformFieldInfoTips[fieldName];
            const hasPostTip = postTransformFieldInfoTips.hasOwnProperty(fieldName);
            const postTipIsEmpty = (postTipValue === '');
            const inheritedTipValue = inheritedTip || '';
            const inheritedTipIsNotEmpty = (inheritedTipValue !== '');

            if (hasPostTip && !postTipIsEmpty) {
                currentTip = postTipValue;
                // isTipInherited remains false
            } else if (inheritedTipIsNotEmpty) {
                currentTip = inheritedTipValue;
                isTipInherited = true; // Inherited Tip!
            } // Else: currentTip remains '', isTipInherited remains false
            // --- END Tip Inheritance Logic ---

            // --- REMOVE Helper function (we'll use innerHTML) ---
            // const createInheritanceIndicator = () => { ... };
            // ---
            const inheritanceIconHTML = '<i class="bi bi-arrow-down-left-square text-primary small ms-1 align-middle" title="Value inherited from pre-transform settings" data-bs-toggle="tooltip" data-bs-placement="top"></i>';

            // Cell 1: Field Name (using final metadata)
            const nameCell = row.insertCell();
            nameCell.textContent = fieldName;
            if (fieldMeta.isSynthetic) {
                const badge = document.createElement('span');
                badge.className = 'badge bg-info-subtle text-info-emphasis rounded-pill ms-2 fw-normal';
                badge.textContent = 'Synthetic';
                badge.title = 'Field created by a transformation rule';
                nameCell.appendChild(badge);
            }

            // Cell 2: Enabled Toggle
            const enableCell = row.insertCell();
            enableCell.className = 'text-center';
            const enableCellContent = document.createElement('div'); // Parent flex container
            enableCellContent.className = 'd-flex align-items-center justify-content-center'; // Center content
            const enableSwitchContainer = document.createElement('div');
            enableSwitchContainer.className = 'form-check form-switch'; // Switch itself
            const enableInput = document.createElement('input');
            enableInput.type = 'checkbox';
            enableInput.className = 'form-check-input';
            enableInput.role = 'switch';
            enableInput.checked = isEnabled;
            enableInput.id = `post-transform-enable-${fieldName.replace(/\W/g, '_')}`;

            const enableIndicatorPlaceholder = document.createElement('span');
            enableIndicatorPlaceholder.className = 'inheritance-indicator';
            enableIndicatorPlaceholder.style.minWidth = '20px'; // Fixed width for placeholder
            enableIndicatorPlaceholder.style.display = 'inline-block'; // Ensure it takes space
            enableIndicatorPlaceholder.style.textAlign = 'center';
            if (isEnabledInherited) {
                enableIndicatorPlaceholder.innerHTML = inheritanceIconHTML;
            }

            // Create and add Reset Button for Enabled
            const inheritedEnabled = preTransformEnabledStatus.hasOwnProperty(fieldName) ? preTransformEnabledStatus[fieldName] : true;
            const resetEnabledBtn = document.createElement('button');
            resetEnabledBtn.className = 'btn btn-sm btn-link text-primary p-0 ms-1 align-middle';
            resetEnabledBtn.innerHTML = '<i class="bi bi-arrow-counterclockwise"></i>';
            resetEnabledBtn.title = 'Reset to inherited value';
            const showInitialResetEnabled = (isEnabled !== inheritedEnabled && postTransformFieldEnabledStatus.hasOwnProperty(fieldName));
            resetEnabledBtn.style.visibility = showInitialResetEnabled ? 'visible' : 'hidden';
            resetEnabledBtn.style.pointerEvents = showInitialResetEnabled ? 'auto' : 'none';
            resetEnabledBtn.addEventListener('click', () => {
                console.log(`[RESET DEBUG] Resetting enabled status for ${fieldName}.`);
                console.log(`[RESET DEBUG] Before delete/save. Current postTransformEnabledStatus[${fieldName}]:`, postTransformFieldEnabledStatus[fieldName]);
                delete postTransformFieldEnabledStatus[fieldName];
                savePostTransformEnabledStatusToStorage();
                // Log before calling markModified
                console.log(`[RESET DEBUG] Before calling _markScenarioAsModified. ActiveName: ${localStorage.getItem('activeAnalyticsConfigurationName')}, ModifiedFlag: ${localStorage.getItem('analyticsScenarioModified')}`);
                // Mark scenario as modified FIRST
                 if (window.AnalyticsConfigManager?._markScenarioAsModified) {
                      window.AnalyticsConfigManager._markScenarioAsModified();
                      console.log(`[RESET DEBUG] Called _markScenarioAsModified.`);
                 } else { console.error("[RESET DEBUG] _markScenarioAsModified not found!"); }
                renderPostTransformFieldConfigUI(); // Re-render table AFTER marking
                console.log(`[RESET DEBUG] After renderPostTransformFieldConfigUI.`);
            });

            enableInput.addEventListener('change', (e) => {
                const field = row.dataset.fieldName;
                const checked = e.target.checked;
                console.log(`[PostTransform] Enable toggled for ${field}: ${checked}`);
                postTransformFieldEnabledStatus[field] = checked;
                savePostTransformEnabledStatusToStorage();
                // Clear inheritance indicator placeholder when user makes a change
                enableIndicatorPlaceholder.innerHTML = '';
                // Show/hide reset button by VISIBILITY
                const showReset = (checked !== inheritedEnabled);
                resetEnabledBtn.style.visibility = showReset ? 'visible' : 'hidden';
                resetEnabledBtn.style.pointerEvents = showReset ? 'auto' : 'none';
                // <<< Explicitly re-attach modification listeners >>>
                if (window.AnalyticsConfigManager?.initializeModificationDetection) {
                     window.AnalyticsConfigManager.initializeModificationDetection();
                }
                 // Mark scenario as modified
                 if (window.AnalyticsConfigManager?._markScenarioAsModified) {
                      window.AnalyticsConfigManager._markScenarioAsModified();
                 }
            });
            enableSwitchContainer.appendChild(enableInput);
            enableCellContent.appendChild(enableSwitchContainer); // Add switch
            enableCellContent.appendChild(enableIndicatorPlaceholder); // Add placeholder
            enableCellContent.appendChild(resetEnabledBtn); // Add reset button
            enableCell.appendChild(enableCellContent); // Add container to cell

            // Cell 3: Format Select/Button
            const formatCell = row.insertCell();
            const formatCellContent = document.createElement('div'); // Parent flex container
            formatCellContent.className = 'd-flex align-items-center';
            const formatControlWrapper = document.createElement('div'); // Wrapper for the select/text
            formatControlWrapper.className = 'flex-grow-1'; // Allow control to take space

            if (isNumeric) {
                const formatSelect = document.createElement('select');
                formatSelect.className = 'form-select form-select-sm';
                formatSelect.title = `Current format: ${currentFormat}`;
                const openModalFn = mainModule?.openNumericFormatModal;

                for (const key in availableFormatOptions) {
                    if (key === 'configure') {
                        continue; // Don't add the 'Custom...' option
                    }
                    const option = document.createElement('option');
                    option.value = key;
                    option.textContent = availableFormatOptions[key];
                    if (key === currentFormat || (key === 'custom' && !availableFormatOptions[currentFormat])) {
                        option.selected = true;
                    }
                    formatSelect.appendChild(option);
                }
                 if (currentFormat !== 'default' && !availableFormatOptions[currentFormat] && currentFormat !== 'configure') {
                     const customOption = document.createElement('option');
                     customOption.value = currentFormat;
                     customOption.textContent = `Custom (${currentFormat.substring(0,15)}...)`;
                     customOption.selected = true;
                     formatSelect.appendChild(customOption);
                 }

                formatSelect.addEventListener('change', (e) => {
                    const field = row.dataset.fieldName;
                    const selectedFormat = e.target.value;
                    const actualInheritedFormat = inheritedFormat || 'default'; // Get actual inherited format
                    console.log(`[PostTransform] Format changed for ${field}: ${selectedFormat}`);
                    if (selectedFormat === 'configure') {
                        if (typeof openModalFn === 'function') {
                            openModalFn(field, currentFormat, 'postTransform');
                        }
                        e.target.value = currentFormat; // Reset select visually
                    } else {
                        postTransformNumericFieldFormats[field] = selectedFormat;
                        savePostTransformNumericFormatsToStorage();
                        formatSelect.title = `Current format: ${selectedFormat}`;
                        const customOpt = formatSelect.querySelector(`option[value^="custom:"]`);
                        if(customOpt && customOpt.value !== selectedFormat) {
                             formatSelect.removeChild(customOpt);
                        }
                        if (!availableFormatOptions[selectedFormat] && selectedFormat !== 'configure') {
                            const newCustomOption = document.createElement('option');
                            newCustomOption.value = selectedFormat;
                            newCustomOption.textContent = `Custom (${selectedFormat.substring(0,15)}...)`;
                            newCustomOption.selected = true;
                            if (!formatSelect.querySelector(`option[value="${selectedFormat}"]`)) {
                                formatSelect.appendChild(newCustomOption);
                            }
                        }
                         // Clear inheritance indicator placeholder when user makes a change
                         formatIndicatorPlaceholder.innerHTML = '';
                         // Show/hide reset button by VISIBILITY
                         const showResetFormat = (selectedFormat !== actualInheritedFormat);
                         resetFormatBtn.style.visibility = showResetFormat ? 'visible' : 'hidden';
                         resetFormatBtn.style.pointerEvents = showResetFormat ? 'auto' : 'none';
                         // <<< Explicitly re-attach modification listeners >>>
                         if (window.AnalyticsConfigManager?.initializeModificationDetection) {
                             window.AnalyticsConfigManager.initializeModificationDetection();
                         }
                          // Mark scenario as modified
                         if (window.AnalyticsConfigManager?._markScenarioAsModified) {
                              window.AnalyticsConfigManager._markScenarioAsModified();
                         }
                    }
                });
                formatControlWrapper.appendChild(formatSelect);
            } else {
                const naSpan = document.createElement('span');
                naSpan.className = 'text-muted small';
                naSpan.textContent = 'N/A';
                formatControlWrapper.appendChild(naSpan);
            }

            const formatIndicatorPlaceholder = document.createElement('span');
            formatIndicatorPlaceholder.className = 'inheritance-indicator';
            formatIndicatorPlaceholder.style.minWidth = '20px';
            formatIndicatorPlaceholder.style.display = 'inline-block';
            formatIndicatorPlaceholder.style.textAlign = 'center';
            if (isFormatInherited) {
                formatIndicatorPlaceholder.innerHTML = inheritanceIconHTML;
            }

            // Create and add Reset Button for Format
            const resetFormatBtn = document.createElement('button');
            resetFormatBtn.className = 'btn btn-sm btn-link text-primary p-0 ms-1 align-middle';
            resetFormatBtn.innerHTML = '<i class="bi bi-arrow-counterclockwise"></i>';
            resetFormatBtn.title = 'Reset to inherited value';
            // Set initial visibility
            let showInitialResetFormat = false;
            if (isNumeric) {
                 const actualInheritedFormat = inheritedFormat || 'default';
                 showInitialResetFormat = (currentFormat !== actualInheritedFormat && postTransformNumericFieldFormats.hasOwnProperty(fieldName));
                 resetFormatBtn.addEventListener('click', () => {
                    console.log(`[RESET DEBUG] Resetting format for ${fieldName}.`);
                    console.log(`[RESET DEBUG] Before delete/save. Current postTransformNumericFieldFormats[${fieldName}]:`, postTransformNumericFieldFormats[fieldName]);
                    delete postTransformNumericFieldFormats[fieldName];
                    savePostTransformNumericFormatsToStorage();
                    // Log before calling markModified
                    console.log(`[RESET DEBUG] Before calling _markScenarioAsModified. ActiveName: ${localStorage.getItem('activeAnalyticsConfigurationName')}, ModifiedFlag: ${localStorage.getItem('analyticsScenarioModified')}`);
                    // Mark scenario as modified FIRST
                    if (window.AnalyticsConfigManager?._markScenarioAsModified) {
                         window.AnalyticsConfigManager._markScenarioAsModified();
                         console.log(`[RESET DEBUG] Called _markScenarioAsModified.`);
                    } else { console.error("[RESET DEBUG] _markScenarioAsModified not found!"); }
                    renderPostTransformFieldConfigUI(); // Re-render table AFTER marking
                    console.log(`[RESET DEBUG] After renderPostTransformFieldConfigUI.`);
                });
            }
            resetFormatBtn.style.visibility = showInitialResetFormat ? 'visible' : 'hidden';
            resetFormatBtn.style.pointerEvents = showInitialResetFormat ? 'auto' : 'none';

            formatCellContent.appendChild(formatControlWrapper); // Add control
            formatCellContent.appendChild(formatIndicatorPlaceholder); // Add placeholder
            formatCellContent.appendChild(resetFormatBtn); // Add reset button
            formatCell.appendChild(formatCellContent); // Add container to cell

            // Cell 4: Info Tip
            const tipCell = row.insertCell();
            tipCell.style.position = 'relative';
            const tipCellContent = document.createElement('div'); // Parent flex container
            tipCellContent.className = 'd-flex align-items-center';
            const tipControlWrapper = document.createElement('div'); // Wrapper for input group
            tipControlWrapper.className = 'input-group input-group-sm flex-grow-1'; // Grow input group
            const tipInput = document.createElement('input');
            tipInput.type = 'text';
            tipInput.className = 'form-control form-control-sm field-info-input';
            tipInput.placeholder = 'Add hover tip...';
            tipInput.value = currentTip;
            tipInput.title = currentTip || 'No info tip set';

            const tipIndicatorPlaceholder = document.createElement('span');
            tipIndicatorPlaceholder.className = 'inheritance-indicator';
            tipIndicatorPlaceholder.style.minWidth = '20px';
            tipIndicatorPlaceholder.style.display = 'inline-block';
            tipIndicatorPlaceholder.style.textAlign = 'center';
            tipIndicatorPlaceholder.classList.add('ms-1');
            if (isTipInherited) {
                tipIndicatorPlaceholder.innerHTML = inheritanceIconHTML;
            }

            // Create and add Reset Button for Tip
            const actualInheritedTip = inheritedTip || ''; // Get actual inherited tip
            const resetTipBtn = document.createElement('button');
            resetTipBtn.className = 'btn btn-sm btn-link text-primary p-0 ms-1 align-middle';
            resetTipBtn.innerHTML = '<i class="bi bi-arrow-counterclockwise"></i>';
            resetTipBtn.title = 'Reset to inherited value';
            // Set initial visibility
            const showInitialResetTip = (currentTip !== actualInheritedTip && postTransformFieldInfoTips.hasOwnProperty(fieldName));
            resetTipBtn.style.visibility = showInitialResetTip ? 'visible' : 'hidden';
            resetTipBtn.style.pointerEvents = showInitialResetTip ? 'auto' : 'none';
            resetTipBtn.addEventListener('click', () => {
                console.log(`[RESET DEBUG] Resetting tip for ${fieldName}.`);
                console.log(`[RESET DEBUG] Before delete/save. Current postTransformFieldInfoTips[${fieldName}]:`, postTransformFieldInfoTips[fieldName]);
                delete postTransformFieldInfoTips[fieldName];
                savePostTransformInfoTipsToStorage();
                 // Log before calling markModified
                console.log(`[RESET DEBUG] Before calling _markScenarioAsModified. ActiveName: ${localStorage.getItem('activeAnalyticsConfigurationName')}, ModifiedFlag: ${localStorage.getItem('analyticsScenarioModified')}`);
                 // Mark scenario as modified FIRST
                if (window.AnalyticsConfigManager?._markScenarioAsModified) {
                     window.AnalyticsConfigManager._markScenarioAsModified();
                     console.log(`[RESET DEBUG] Called _markScenarioAsModified.`);
                } else { console.error("[RESET DEBUG] _markScenarioAsModified not found!"); }
                renderPostTransformFieldConfigUI(); // Re-render table AFTER marking
                console.log(`[RESET DEBUG] After renderPostTransformFieldConfigUI.`);
            });

            let saveTimeout;
            tipInput.addEventListener('input', (e) => {
                const field = row.dataset.fieldName;
                const value = e.target.value;
                tipInput.title = value || 'No info tip set';
                clearTimeout(saveTimeout);
                saveTimeout = setTimeout(() => {
                    console.log(`[PostTransform] Info tip updated for ${field}: ${value}`);
                    postTransformFieldInfoTips[field] = value;
                    savePostTransformInfoTipsToStorage();
                     // Mark scenario as modified (also needed here on manual input)
                     // No need to call here if called immediately below?
                }, 750);
                // Clear inheritance indicator placeholder when user makes a change
                tipIndicatorPlaceholder.innerHTML = '';
                // Show/hide reset button by VISIBILITY
                const showResetTip = (value !== actualInheritedTip);
                resetTipBtn.style.visibility = showResetTip ? 'visible' : 'hidden';
                resetTipBtn.style.pointerEvents = showResetTip ? 'auto' : 'none';
                // Mark scenario as modified (immediate attempt on input)
                 if (window.AnalyticsConfigManager?._markScenarioAsModified) {
                      window.AnalyticsConfigManager._markScenarioAsModified();
                 }
            });
            tipControlWrapper.appendChild(tipInput);
            tipCellContent.appendChild(tipControlWrapper); // Add input group wrapper
            tipCellContent.appendChild(tipIndicatorPlaceholder); // Add placeholder
            tipCellContent.appendChild(resetTipBtn); // Add reset button
            tipCell.appendChild(tipCellContent);
        });

        console.log("[PostTransform] Finished rendering UI.");
        // Apply search/sort listeners if needed (separate function?)
        applySearchListener();

        // Initialize tooltips for the newly added indicators
        initializePostTransformTooltips(); // Call specific initializer
        // TODO: Apply sort listeners if required
    }

    // --- Search Functionality ---
    function applySearchListener() {
        if (!searchInput || !tableBody) return;

        let searchTimeout;
        searchInput.addEventListener('input', () => {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => {
                const searchTerm = searchInput.value.toLowerCase().trim();
                const rows = tableBody.querySelectorAll('tr');
                console.log(`[PostTransform] Searching for: ${searchTerm}`);
                rows.forEach(row => {
                    const fieldName = row.dataset.fieldName?.toLowerCase() || '';
                    if (fieldName.includes(searchTerm)) {
                        row.style.display = '';
                    } else {
                        row.style.display = 'none';
                    }
                });
            }, 300); // Debounce search
        });
    }
    // --- End Search Functionality ---

    function loadPostTransformFieldSettings(enabledStatus, numericFormats, infoTips) {
        console.log("[PostTransform] loadPostTransformFieldSettings called with:", { enabledStatus, numericFormats, infoTips });

        // Enabled Status: Overwrite or merge? Let's overwrite for direct load.
        if (enabledStatus && typeof enabledStatus === 'object' && !Array.isArray(enabledStatus)) {
            postTransformFieldEnabledStatus = { ...enabledStatus }; // Create copy
        } else {
            console.warn("[PostTransform Load] Invalid enabledStatus provided, resetting.");
            postTransformFieldEnabledStatus = {};
        }

        // Numeric Formats: Overwrite
        if (numericFormats && typeof numericFormats === 'object' && !Array.isArray(numericFormats)) {
            postTransformNumericFieldFormats = { ...numericFormats }; // Create copy
        } else {
            console.warn("[PostTransform Load] Invalid numericFormats provided, resetting.");
            postTransformNumericFieldFormats = {};
        }

        // Info Tips: Overwrite
        if (infoTips && typeof infoTips === 'object' && !Array.isArray(infoTips)) {
            postTransformFieldInfoTips = { ...infoTips }; // Create copy
        } else {
            console.warn("[PostTransform Load] Invalid infoTips provided, resetting.");
            postTransformFieldInfoTips = {};
        }

        // --- Clear the UI instead of rendering ---
        const tableBody = document.getElementById('post-transform-field-config-tbody');
        if (tableBody) {
            tableBody.innerHTML = '<tr><td colspan="4" class="text-muted small text-center">Scenario loaded. Run transformations to view/configure post-transform fields.</td></tr>'; // Adjusted colspan
        } else {
            console.error("[PostTransform Load] Cannot find table body to clear UI.");
        }
        // --- End Clear UI ---

        console.log("[PostTransform] Post-transform field settings state loaded directly. UI cleared.");
    }

    // --- NEW: Tooltip Initializer for this specific table ---
    function initializePostTransformTooltips() {
        const tooltipTriggerList = document.querySelectorAll('#post-transform-field-config-container [data-bs-toggle="tooltip"]');
        // Dispose existing tooltips in this container first
        tooltipTriggerList.forEach(tooltipTriggerEl => {
            const existingTooltip = bootstrap.Tooltip.getInstance(tooltipTriggerEl);
            if (existingTooltip) {
                existingTooltip.dispose();
            }
        });
        // Initialize new tooltips
        const newTooltipList = [...tooltipTriggerList].map(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl));
        console.log(`[PostTransform] Initialized ${newTooltipList.length} tooltips in post-transform config.`);
    }
    // ---

    // --- Initialization Function ---
    function initializePostTransform() {
        console.log("[PostTransform] Initializing...");
        loadPostTransformEnabledStatusFromStorage();
        loadPostTransformNumericFormatsFromStorage();
        loadPostTransformInfoTipsFromStorage();
        // Initial render might show placeholder until data is ready
        renderPostTransformFieldConfigUI(); 
        console.log("[PostTransform] Initialization complete.");
    }

    // --- Expose Module API ---
    // Ensure the global object exists
    window.AnalyticsPostTransformModule = window.AnalyticsPostTransformModule || {};
    window.AnalyticsPostTransformModule.renderPostTransformFieldConfigUI = renderPostTransformFieldConfigUI;
    window.AnalyticsPostTransformModule.loadPostTransformFieldSettings = loadPostTransformFieldSettings;
    // Add getters needed for Phase 4 (final data rendering) AND Scenario Saving
    window.AnalyticsPostTransformModule.getPostTransformEnabledStatus = () => ({ ...postTransformFieldEnabledStatus }); // Return shallow copy
    window.AnalyticsPostTransformModule.getPostTransformNumericFormats = () => ({ ...postTransformNumericFieldFormats }); // <<< ADDED Getter (shallow copy)
    window.AnalyticsPostTransformModule.getPostTransformInfoTips = () => ({ ...postTransformFieldInfoTips }); // <<< ADDED Getter (shallow copy)
    // Keep existing getters if they were there before
    window.AnalyticsPostTransformModule.getPostTransformFormat = (fieldName) => postTransformNumericFieldFormats[fieldName] || 'default';
    window.AnalyticsPostTransformModule.getPostTransformInfoTip = (fieldName) => postTransformFieldInfoTips[fieldName] || '';

    console.log("AnalyticsPostTransformModule API exposed.");

    // --- Run Initialization ---
    initializePostTransform();

}); 