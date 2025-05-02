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

            // --- Helper function to create indicator ---
            const createInheritanceIndicator = () => {
                const indicator = document.createElement('i');
                indicator.className = 'bi bi-arrow-down-left-square text-muted small ms-1 align-middle'; // Added align-middle
                indicator.title = 'Value inherited from pre-transform settings';
                indicator.dataset.bsToggle = 'tooltip';
                indicator.dataset.bsPlacement = 'top';
                return indicator;
            };
            // ---

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
            const enableSwitchContainer = document.createElement('div'); // Wrapper for switch and indicator
            enableSwitchContainer.className = 'd-inline-flex align-items-center'; // Use flex to align switch and icon
            const enableSwitch = document.createElement('div');
            enableSwitch.className = 'form-check form-switch'; // Removed d-inline-block
            const enableInput = document.createElement('input');
            enableInput.type = 'checkbox';
            enableInput.className = 'form-check-input';
            enableInput.role = 'switch';
            enableInput.checked = isEnabled;
            enableInput.id = `post-transform-enable-${fieldName.replace(/\W/g, '_')}`;
            enableInput.addEventListener('change', (e) => {
                const field = row.dataset.fieldName;
                const checked = e.target.checked;
                console.log(`[PostTransform] Enable toggled for ${field}: ${checked}`);
                postTransformFieldEnabledStatus[field] = checked;
                savePostTransformEnabledStatusToStorage();
                // Remove inheritance indicator if it exists when user makes a change
                const indicator = enableSwitchContainer.querySelector('.bi-arrow-down-left-square');
                if (indicator) indicator.remove();
                // <<< Explicitly re-attach modification listeners >>>
                if (window.AnalyticsConfigManager?.initializeModificationDetection) {
                    window.AnalyticsConfigManager.initializeModificationDetection();
                }
            });
            enableSwitch.appendChild(enableInput);
            enableSwitchContainer.appendChild(enableSwitch);
            if (isEnabledInherited) {
                enableSwitchContainer.appendChild(createInheritanceIndicator());
            }
            enableCell.appendChild(enableSwitchContainer); // Append the container

            // Cell 3: Format Select/Button
            const formatCell = row.insertCell();
            const formatContentWrapper = document.createElement('div'); // Wrapper for content and indicator
            formatContentWrapper.className = 'd-flex align-items-center'; // Align select/text and icon
            if (isNumeric) {
                const formatSelect = document.createElement('select');
                formatSelect.className = 'form-select form-select-sm';
                formatSelect.title = `Current format: ${currentFormat}`;
                const openModalFn = mainModule?.openNumericFormatModal;

                for (const key in availableFormatOptions) {
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
                         // Remove inheritance indicator if it exists when user makes a change
                        const indicator = formatContentWrapper.querySelector('.bi-arrow-down-left-square');
                        if (indicator) indicator.remove();
                        // <<< Explicitly re-attach modification listeners >>>
                        if (window.AnalyticsConfigManager?.initializeModificationDetection) {
                            window.AnalyticsConfigManager.initializeModificationDetection();
                        }
                    }
                });
                formatContentWrapper.appendChild(formatSelect);
                if (isFormatInherited) {
                    formatContentWrapper.appendChild(createInheritanceIndicator());
                }
            } else {
                const naSpan = document.createElement('span');
                naSpan.className = 'text-muted small';
                naSpan.textContent = 'N/A';
                formatContentWrapper.appendChild(naSpan);
                // No indicator for N/A
            }
            formatCell.appendChild(formatContentWrapper);

            // Cell 4: Info Tip
            const tipCell = row.insertCell();
            tipCell.style.position = 'relative';
            const tipContentWrapper = document.createElement('div'); // Wrapper for input and indicator
            tipContentWrapper.className = 'd-flex align-items-center'; // Align input and icon
            const tipGroup = document.createElement('div');
            tipGroup.className = 'input-group input-group-sm flex-grow-1'; // Allow input group to grow
            const tipInput = document.createElement('input');
            tipInput.type = 'text';
            tipInput.className = 'form-control form-control-sm field-info-input';
            tipInput.placeholder = 'Add hover tip...';
            tipInput.value = currentTip;
            tipInput.title = currentTip || 'No info tip set';
            let saveTimeout;
            tipInput.addEventListener('input', (e) => {
                clearTimeout(saveTimeout);
                const field = row.dataset.fieldName;
                const value = e.target.value;
                tipInput.title = value || 'No info tip set'; 
                saveTimeout = setTimeout(() => {
                    console.log(`[PostTransform] Info tip updated for ${field}: ${value}`);
                    postTransformFieldInfoTips[field] = value; 
                    savePostTransformInfoTipsToStorage(); 
                }, 750); 
                // Remove inheritance indicator if it exists when user makes a change
                const indicator = tipContentWrapper.querySelector('.bi-arrow-down-left-square');
                if (indicator) indicator.remove();
                // <<< Explicitly re-attach modification listeners >>>
                if (window.AnalyticsConfigManager?.initializeModificationDetection) {
                    window.AnalyticsConfigManager.initializeModificationDetection();
                }
            });
            tipGroup.appendChild(tipInput);
            tipContentWrapper.appendChild(tipGroup); // Add input group to wrapper
            if (isTipInherited) {
                tipContentWrapper.appendChild(createInheritanceIndicator());
            }
            tipCell.appendChild(tipContentWrapper);
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

        // Re-render the UI to reflect loaded settings
        renderPostTransformFieldConfigUI();
        console.log("[PostTransform] Post-transform field settings loaded directly and UI updated.");
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
    // Add getters needed for Phase 4 (final data rendering)
    window.AnalyticsPostTransformModule.getPostTransformEnabledStatus = () => ({ ...postTransformFieldEnabledStatus }); // Return shallow copy
    window.AnalyticsPostTransformModule.getPostTransformFormat = (fieldName) => postTransformNumericFieldFormats[fieldName] || 'default';
    window.AnalyticsPostTransformModule.getPostTransformInfoTip = (fieldName) => postTransformFieldInfoTips[fieldName] || '';

    console.log("AnalyticsPostTransformModule API exposed.");

    // --- Run Initialization ---
    initializePostTransform();

}); 