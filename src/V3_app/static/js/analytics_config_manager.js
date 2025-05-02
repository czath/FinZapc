// === Analytics Configuration Manager ===
// Manages saving, loading, and activating named configurations (filters, field settings, rules).
// Interacts with the original localStorage keys used by analytics.js and analytics_transform.js.

window.AnalyticsConfigManager = (function() {
    console.log("AnalyticsConfigManager initializing...");

    // --- Storage Keys ---
    // Key for storing the object containing ALL named configurations
    const CONFIGURATIONS_STORAGE_KEY = 'analyticsConfigurations'; 
    // Key for the *name* of the currently active config (relevant for UI indication later, not direct loading)
    const ACTIVE_CONFIG_NAME_STORAGE_KEY = 'activeAnalyticsConfigurationName';
    const DEFAULT_CONFIG_NAME = 'Default'; // A default name

    // Original localStorage keys used by analytics.js and analytics_transform.js
    // These are the keys this module reads FROM when saving the current state, 
    // and writes TO when activating a saved configuration.
    const OLD_FILTER_STORAGE_KEY = 'analyticsAnalyticsFilters';
    const OLD_FIELD_ENABLED_STORAGE_KEY = 'analyticsAnalyticsFieldEnabled';
    const OLD_FIELD_NUMERIC_FORMAT_STORAGE_KEY = 'analyticsNumericFieldFormats';
    const OLD_FIELD_INFO_TIPS_STORAGE_KEY = 'analyticsFieldInfoTips';
    const OLD_RULES_STORAGE_KEY = 'analyticsTransformationRules'; 
    const MODIFIED_FLAG_STORAGE_KEY = 'analyticsScenarioModified'; // <<< NEW

    // --- NEW: Post-Transform Storage Keys ---
    const POST_TRANSFORM_FIELD_ENABLED_STORAGE_KEY = 'analyticsPostTransformFieldEnabled';
    const POST_TRANSFORM_NUMERIC_FORMAT_STORAGE_KEY = 'analyticsPostTransformNumericFormats';
    const POST_TRANSFORM_FIELD_INFO_TIPS_STORAGE_KEY = 'analyticsPostTransformFieldInfoTips';
    // --- END NEW ---

    // --- Module-level variable to hold validated data from import ---
    // let stagedImportData = null; // No longer needed at module level
    // let stagedImportFilename = null; // No longer needed at module level
    // let importScenarioNameModalInstance = null; // Modal removed
    let reloadScenarioModalInstance = null; // <<< For Bootstrap modal instance
    let scenarioToReload = null; // <<< Store name temporarily for modal actions

    // --- Module-level variables for listener references ---
    let prepPaneClickListener = null;
    let prepFieldConfigChangeListener = null;
    let prepFieldConfigClickListener = null;
    let transformPaneClickListener = null;
    let transformPaneChangeListener = null;
    let transformModalSaveListener = null;
    let formatModalSaveListener = null;
    let postTransformChangeListener = null;
    let postTransformInputListener = null;
    // ---

    // --- Core Utility Functions ---

    /**
     * Retrieves the main configurations object from localStorage.
     * Handles parsing errors and returns an empty object if not found or invalid.
     * @returns {object} The parsed configurations object (e.g., { "Default": {...}, "Scenario A": {...} })
     */
    function _getConfigurationsObject() {
        const rawData = localStorage.getItem(CONFIGURATIONS_STORAGE_KEY);
        if (!rawData) {
            return {}; // Not found, return empty object
        }
        try {
            const parsed = JSON.parse(rawData);
            if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                return parsed;
            } else {
                console.warn(`[ConfigManager] Invalid data found for key "${CONFIGURATIONS_STORAGE_KEY}". Expected object.`);
                return {}; // Invalid structure
            }
        } catch (e) {
            console.error(`[ConfigManager] Error parsing JSON from key "${CONFIGURATIONS_STORAGE_KEY}":`, e);
            return {}; // Error parsing
        }
    }

    /**
     * Saves the provided configurations object back to localStorage.
     * @param {object} configs - The configurations object to save.
     */
    function _saveConfigurationsObject(configs) {
        if (!configs || typeof configs !== 'object' || Array.isArray(configs)) {
            console.error("[ConfigManager] Attempted to save invalid configurations object:", configs);
            return;
        }
        try {
            localStorage.setItem(CONFIGURATIONS_STORAGE_KEY, JSON.stringify(configs));
        } catch (e) {
            console.error(`[ConfigManager] Error saving to key "${CONFIGURATIONS_STORAGE_KEY}":`, e);
            // Handle potential storage quota errors?
        }
    }

    /**
     * Gets a list of all saved configuration names.
     * @returns {string[]} An array of configuration names, sorted alphabetically.
     */
    function getAllConfigurationNames() {
        const configs = _getConfigurationsObject();
        return Object.keys(configs).sort();
    }

    /**
     * Retrieves the full configuration data object for a specific name.
     * @param {string} name - The name of the configuration to retrieve.
     * @returns {object | null} The configuration data object, or null if not found.
     */
    function getConfiguration(name) {
        const configs = _getConfigurationsObject();
        return configs[name] || null;
    }

    /**
     * Deletes a specific configuration by name.
     * @param {string} name - The name of the configuration to delete.
     * @returns {boolean} True if deletion was successful, false otherwise.
     */
    function deleteConfiguration(name) {
        if (typeof name !== 'string' || !name.trim()) {
            console.error("[ConfigManager] Attempted to delete configuration with invalid name:", name);
            return false;
        }
        const trimmedName = name.trim();
        // Optional: Prevent deleting a specific name like 'Default'?
        // if (trimmedName === DEFAULT_CONFIG_NAME) {
        //     console.warn(`[ConfigManager] Cannot delete the "${DEFAULT_CONFIG_NAME}" configuration.`);
        //     alert(`Cannot delete the "${DEFAULT_CONFIG_NAME}" configuration.`); 
        //     return false;
        // }

        const configs = _getConfigurationsObject();
        if (configs.hasOwnProperty(trimmedName)) {
            delete configs[trimmedName];
            _saveConfigurationsObject(configs);
            console.log(`[ConfigManager] Configuration "${trimmedName}" deleted.`);
            return true;
        } else {
            console.warn(`[ConfigManager] Configuration "${trimmedName}" not found for deletion.`);
            return false;
        }
    }

    /**
     * Reads the current application settings (filters, field settings, rules) 
     * directly from their original localStorage keys.
     * @returns {object} An object containing the current settings, structured for saving.
     *                   Returns default empty structures if keys are not found or invalid.
     */
    function _readCurrentAppSettings() {
        console.log("[ConfigManager] Reading current app settings from original localStorage keys...");
        const currentSettings = {
            fieldSettings: { enabled: {}, formats: {}, tips: {} },
            filters: [],
            rules: [],
            postTransformFieldSettings: { enabled: {}, formats: {}, tips: {} } // NEW: Add structure
        };

        // Read Filters
        try {
            const rawFilters = localStorage.getItem(OLD_FILTER_STORAGE_KEY);
            if (rawFilters) {
                const parsed = JSON.parse(rawFilters);
                if (Array.isArray(parsed)) {
                    currentSettings.filters = parsed;
                } else {
                     console.warn(`[ConfigManager] Invalid filters data in "${OLD_FILTER_STORAGE_KEY}". Expected array.`);
                }
            }
        } catch (e) { console.error(`[ConfigManager] Error reading/parsing filters from "${OLD_FILTER_STORAGE_KEY}":`, e); }

        // Read Field Enabled Status
        try {
            const rawEnabled = localStorage.getItem(OLD_FIELD_ENABLED_STORAGE_KEY);
            if (rawEnabled) {
                const parsed = JSON.parse(rawEnabled);
                 if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                    currentSettings.fieldSettings.enabled = parsed;
                 } else {
                     console.warn(`[ConfigManager] Invalid enabled status data in "${OLD_FIELD_ENABLED_STORAGE_KEY}". Expected object.`);
                 }
            }
        } catch (e) { console.error(`[ConfigManager] Error reading/parsing enabled status from "${OLD_FIELD_ENABLED_STORAGE_KEY}":`, e); }

        // Read Field Numeric Formats
        try {
            const rawFormats = localStorage.getItem(OLD_FIELD_NUMERIC_FORMAT_STORAGE_KEY);
            if (rawFormats) {
                const parsed = JSON.parse(rawFormats);
                 if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                    currentSettings.fieldSettings.formats = parsed;
                 } else {
                    console.warn(`[ConfigManager] Invalid numeric formats data in "${OLD_FIELD_NUMERIC_FORMAT_STORAGE_KEY}". Expected object.`);
                 }
            }
        } catch (e) { console.error(`[ConfigManager] Error reading/parsing numeric formats from "${OLD_FIELD_NUMERIC_FORMAT_STORAGE_KEY}":`, e); }

        // Read Field Info Tips
        try {
            const rawTips = localStorage.getItem(OLD_FIELD_INFO_TIPS_STORAGE_KEY);
            if (rawTips) {
                const parsed = JSON.parse(rawTips);
                if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                    currentSettings.fieldSettings.tips = parsed;
                } else {
                    console.warn(`[ConfigManager] Invalid info tips data in "${OLD_FIELD_INFO_TIPS_STORAGE_KEY}". Expected object.`);
                }
            }
        } catch (e) { console.error(`[ConfigManager] Error reading/parsing info tips from "${OLD_FIELD_INFO_TIPS_STORAGE_KEY}":`, e); }

        // Read Transformation Rules
        try {
            const rawRules = localStorage.getItem(OLD_RULES_STORAGE_KEY);
            if (rawRules) {
                const parsed = JSON.parse(rawRules);
                if (Array.isArray(parsed)) {
                    currentSettings.rules = parsed;
                } else {
                    console.warn(`[ConfigManager] Invalid rules data in "${OLD_RULES_STORAGE_KEY}". Expected array.`);
                }
            }
        } catch (e) { console.error(`[ConfigManager] Error reading/parsing rules from "${OLD_RULES_STORAGE_KEY}":`, e); }

        // --- NEW: Read Post-Transform Settings ---
        try {
            const rawPostEnabled = localStorage.getItem(POST_TRANSFORM_FIELD_ENABLED_STORAGE_KEY);
            if (rawPostEnabled) {
                const parsed = JSON.parse(rawPostEnabled);
                 if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                    currentSettings.postTransformFieldSettings.enabled = parsed;
                 } else {
                     console.warn(`[ConfigManager] Invalid post-transform enabled status data in "${POST_TRANSFORM_FIELD_ENABLED_STORAGE_KEY}". Expected object.`);
                 }
            }
        } catch (e) { console.error(`[ConfigManager] Error reading/parsing post-transform enabled status from "${POST_TRANSFORM_FIELD_ENABLED_STORAGE_KEY}":`, e); }

        try {
            const rawPostFormats = localStorage.getItem(POST_TRANSFORM_NUMERIC_FORMAT_STORAGE_KEY);
            if (rawPostFormats) {
                const parsed = JSON.parse(rawPostFormats);
                 if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                    currentSettings.postTransformFieldSettings.formats = parsed;
                 } else {
                    console.warn(`[ConfigManager] Invalid post-transform numeric formats data in "${POST_TRANSFORM_NUMERIC_FORMAT_STORAGE_KEY}". Expected object.`);
                 }
            }
        } catch (e) { console.error(`[ConfigManager] Error reading/parsing post-transform numeric formats from "${POST_TRANSFORM_NUMERIC_FORMAT_STORAGE_KEY}":`, e); }

        try {
            const rawPostTips = localStorage.getItem(POST_TRANSFORM_FIELD_INFO_TIPS_STORAGE_KEY);
            if (rawPostTips) {
                const parsed = JSON.parse(rawPostTips);
                 if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                    currentSettings.postTransformFieldSettings.tips = parsed;
                 } else {
                    console.warn(`[ConfigManager] Invalid post-transform info tips data in "${POST_TRANSFORM_FIELD_INFO_TIPS_STORAGE_KEY}". Expected object.`);
                 }
            }
        } catch (e) { console.error(`[ConfigManager] Error reading/parsing post-transform info tips from "${POST_TRANSFORM_FIELD_INFO_TIPS_STORAGE_KEY}":`, e); }
        // --- END NEW: Read Post-Transform Settings ---

        console.log("[ConfigManager] Current app settings read:", currentSettings);
        return currentSettings;
    }

    /**
     * Saves the current application settings (read from original keys) 
     * as a named configuration in the new storage structure.
     * @param {string} name - The name to save the configuration under.
     * @returns {boolean} True if saving was successful, false otherwise.
     */
    function saveCurrentAppSettingsAs(name) {
        if (typeof name !== 'string' || !name.trim()) {
            console.error("[ConfigManager] Attempted to save configuration with invalid name:", name);
            return false;
        }
        const trimmedName = name.trim();
        
        // Optional: Confirmation for overwrite?
        const configs = _getConfigurationsObject();
        // if (configs.hasOwnProperty(trimmedName)) {
        //     if (!confirm(`Configuration named "${trimmedName}" already exists. Overwrite?`)) {
        //         console.log("[ConfigManager] Save cancelled by user.");
        //         return false;
        //     }
        // }

        const currentSettings = _readCurrentAppSettings();
        configs[trimmedName] = currentSettings;
        _saveConfigurationsObject(configs);
        console.log(`[ConfigManager] Current app settings saved as configuration "${trimmedName}".`);
        return true;
    }

    /**
     * Activates a saved configuration by writing its contents back to the
     * original localStorage keys used by the application.
     * REQUIRES PAGE REFRESH afterwards to take effect.
     * @param {string} name - The name of the configuration to activate.
     * @returns {boolean} True if activation write was successful, false otherwise.
     */
    function activateConfiguration(name) {
         if (typeof name !== 'string' || !name.trim()) {
            console.error("[ConfigManager] Attempted to activate configuration with invalid name:", name);
            return false;
        }
        const trimmedName = name.trim();
        console.log(`[ConfigManager] Activating configuration "${trimmedName}"...`);
        
        const configData = getConfiguration(trimmedName);
        if (!configData) {
            console.error(`[ConfigManager] Configuration "${trimmedName}" not found. Cannot activate.`);
            return false;
        }

        try {
            // Ensure the loaded config has the expected structure, providing defaults
            const filters = Array.isArray(configData.filters) ? configData.filters : [];
            const fieldSettings = (configData.fieldSettings && typeof configData.fieldSettings === 'object') ? configData.fieldSettings : {};
            const enabled = (fieldSettings.enabled && typeof fieldSettings.enabled === 'object') ? fieldSettings.enabled : {};
            const formats = (fieldSettings.formats && typeof fieldSettings.formats === 'object') ? fieldSettings.formats : {};
            const tips = (fieldSettings.tips && typeof fieldSettings.tips === 'object') ? fieldSettings.tips : {};
            const rules = Array.isArray(configData.rules) ? configData.rules : [];

            // --- NEW: Extract post-transform settings with defaults ---
            const postTransformSettings = configData.postTransformFieldSettings || {};
            const postEnabled = postTransformSettings.enabled || {};
            const postFormats = postTransformSettings.formats || {};
            const postTips = postTransformSettings.tips || {};
            // --- END NEW ---

            // Write back to OLD keys
            localStorage.setItem(OLD_FILTER_STORAGE_KEY, JSON.stringify(filters));
            localStorage.setItem(OLD_FIELD_ENABLED_STORAGE_KEY, JSON.stringify(enabled));
            localStorage.setItem(OLD_FIELD_NUMERIC_FORMAT_STORAGE_KEY, JSON.stringify(formats));
            localStorage.setItem(OLD_FIELD_INFO_TIPS_STORAGE_KEY, JSON.stringify(tips));
            localStorage.setItem(OLD_RULES_STORAGE_KEY, JSON.stringify(rules));
            
            // <<< NEW: Reset modified flag when activating a scenario >>>
            localStorage.setItem(MODIFIED_FLAG_STORAGE_KEY, 'false'); 

            // --- NEW: Write post-transform settings to NEW keys ---
            localStorage.setItem(POST_TRANSFORM_FIELD_ENABLED_STORAGE_KEY, JSON.stringify(postEnabled));
            localStorage.setItem(POST_TRANSFORM_NUMERIC_FORMAT_STORAGE_KEY, JSON.stringify(postFormats));
            localStorage.setItem(POST_TRANSFORM_FIELD_INFO_TIPS_STORAGE_KEY, JSON.stringify(postTips));
            // --- END NEW ---
            
            console.log(`[ConfigManager] Configuration "${trimmedName}" written to original localStorage keys. Refresh page to apply.`);
            return true;

        } catch (e) {
            console.error(`[ConfigManager] Error writing configuration "${trimmedName}" to original localStorage keys:`, e);
            return false;
        }
    }

    // --- NEW: Function to mark current settings as modified ---
    function _markScenarioAsModified() {
        console.log("[_markScenarioAsModified DEBUG] Function called."); // Log function entry
        const currentActiveName = localStorage.getItem(ACTIVE_CONFIG_NAME_STORAGE_KEY);
        console.log(`[_markScenarioAsModified DEBUG] currentActiveName: ${currentActiveName}`); // Log active name

        // Only mark as modified if there *is* an active scenario loaded
        if (currentActiveName) {
            const currentModifiedState = localStorage.getItem(MODIFIED_FLAG_STORAGE_KEY) === 'true';
            console.log(`[_markScenarioAsModified DEBUG] currentModifiedState: ${currentModifiedState}`); // Log current flag state

            if (!currentModifiedState) { // Only update if not already marked as modified
                console.log("[_markScenarioAsModified DEBUG] Change detected & not already modified. Setting flag to true and refreshing table.");
                localStorage.setItem(MODIFIED_FLAG_STORAGE_KEY, 'true');
                // Refresh the table to show the status change immediately
                populateScenarioTable();
            } else {
                console.log("[_markScenarioAsModified DEBUG] Scenario already marked as modified. No action taken.");
            }
        } else {
            console.log("[_markScenarioAsModified DEBUG] No active scenario name found. Cannot mark as modified.");
        }
    }

    // --- NEW: Function to Apply Scenario Settings Directly --- 
    function applyScenarioSettingsDirectly(name) {
        if (typeof name !== 'string' || !name.trim()) {
            console.error("[ConfigManager] ApplyDirect: Invalid scenario name:", name);
            updateStatus("Error: Cannot load scenario with invalid name.", true);
            return false;
        }
        const trimmedName = name.trim();
        console.log(`[ConfigManager] Applying scenario settings directly for: ${trimmedName}`);

        const configData = getConfiguration(trimmedName);
        if (!configData) {
            console.error(`[ConfigManager] ApplyDirect: Scenario \"${trimmedName}\" not found.`);
            updateStatus(`Error: Scenario \"${trimmedName}\" not found.`, true);
            return false;
        }

        try {
            // Extract data with defaults
            const filters = Array.isArray(configData.filters) ? configData.filters : [];
            const fieldSettings = (configData.fieldSettings && typeof configData.fieldSettings === 'object') ? configData.fieldSettings : {};
            const enabledStatus = (fieldSettings.enabled && typeof fieldSettings.enabled === 'object') ? fieldSettings.enabled : {};
            const numericFormats = (fieldSettings.formats && typeof fieldSettings.formats === 'object') ? fieldSettings.formats : {};
            const infoTips = (fieldSettings.tips && typeof fieldSettings.tips === 'object') ? fieldSettings.tips : {};
            const rules = Array.isArray(configData.rules) ? configData.rules : [];

            // --- NEW: Extract post-transform settings --- 
            const postTransformSettings = configData.postTransformFieldSettings || {};
            const postEnabled = postTransformSettings.enabled || {};
            const postFormats = postTransformSettings.formats || {};
            const postTips = postTransformSettings.tips || {};
            // --- END NEW ---

            // --- Call Main Analytics Module Setters ---
            const mainModule = window.AnalyticsMainModule;
            if (mainModule) {
                if (typeof mainModule.loadFieldSettings === 'function') {
                    mainModule.loadFieldSettings(enabledStatus, numericFormats, infoTips);
                } else {
                    console.error("[ConfigManager] ApplyDirect: loadFieldSettings function not found on AnalyticsMainModule.");
                }
                if (typeof mainModule.loadFilters === 'function') {
                    mainModule.loadFilters(filters); // Load filters AFTER field settings so UI renders correctly
                } else {
                     console.error("[ConfigManager] ApplyDirect: loadFilters function not found on AnalyticsMainModule.");
                }

                // --- NEW: Call Post-Transform Loader --- 
                if (window.AnalyticsPostTransformModule && typeof window.AnalyticsPostTransformModule.loadPostTransformFieldSettings === 'function') {
                    window.AnalyticsPostTransformModule.loadPostTransformFieldSettings(postEnabled, postFormats, postTips);
                } else {
                    console.error("[ConfigManager] ApplyDirect: AnalyticsPostTransformModule or loadPostTransformFieldSettings function not found.");
                }
                // --- END NEW ---
            } else {
                console.error("[ConfigManager] ApplyDirect: AnalyticsMainModule not found.");
                updateStatus("Error: Analytics core module not available.", true);
                return false; // Stop if main module isn't there
            }

            // --- Call Transform Module Setter ---
            const transformModule = window.AnalyticsTransformModule;
            if (transformModule) {
                if (typeof transformModule.loadTransformationRules === 'function') {
                    transformModule.loadTransformationRules(rules);
                } else {
                    console.error("[ConfigManager] ApplyDirect: loadTransformationRules function not found on AnalyticsTransformModule.");
                }
            } else {
                // This might be less critical than the main module missing
                console.warn("[ConfigManager] ApplyDirect: AnalyticsTransformModule not found.");
            }

            // Update active scenario tracking and modified flag
            localStorage.setItem(ACTIVE_CONFIG_NAME_STORAGE_KEY, trimmedName);
            localStorage.setItem(MODIFIED_FLAG_STORAGE_KEY, 'false');

            // Refresh the scenario table UI
            populateScenarioTable();

            updateStatus(`Scenario \"${trimmedName}\" loaded successfully.`);
            console.log(`[ConfigManager] ApplyDirect: Scenario \"${trimmedName}\" applied successfully.`);
            return true;

        } catch (error) {
            console.error(`[ConfigManager] ApplyDirect: Error applying scenario \"${trimmedName}\":`, error);
            updateStatus(`Error loading scenario \"${trimmedName}\": ${error.message}`, true);
            return false;
        }
    }
    // --- END NEW Apply Scenario Settings Directly --- 

    // --- UI Interaction Functions ---

    /**
     * Populates the scenarios table in the UI.
     */
    function populateScenarioTable() {
        // <<< Add initial state logging >>>
        console.log(`[ConfigManager DEBUG] populateScenarioTable: Initial activeName = '${localStorage.getItem(ACTIVE_CONFIG_NAME_STORAGE_KEY)}', Initial modifiedFlag = '${localStorage.getItem(MODIFIED_FLAG_STORAGE_KEY)}'`);

        const tableBody = document.querySelector('#analytics-scenarios-table tbody');
        const configStatus = document.getElementById('analytics-config-status'); 
        
        if (!tableBody) {
            console.error("[ConfigManager] Scenario table body not found.");
            return;
        }

        tableBody.innerHTML = ''; 
        const scenarioNames = getAllConfigurationNames();
        let currentActiveNameInMemory = localStorage.getItem(ACTIVE_CONFIG_NAME_STORAGE_KEY); 
        let isModified = localStorage.getItem(MODIFIED_FLAG_STORAGE_KEY) === 'true'; // <<< Initial read
        
        // Ensure a default active name exists if none is set and default exists
        if (!currentActiveNameInMemory && scenarioNames.includes(DEFAULT_CONFIG_NAME)) {
            currentActiveNameInMemory = DEFAULT_CONFIG_NAME;
            localStorage.setItem(ACTIVE_CONFIG_NAME_STORAGE_KEY, currentActiveNameInMemory);
            localStorage.setItem(MODIFIED_FLAG_STORAGE_KEY, 'false'); 
            isModified = false; // <<< Update local variable too
        } else if (!currentActiveNameInMemory && scenarioNames.length > 0) {
            // If default doesn't exist, pick the first one
            currentActiveNameInMemory = scenarioNames[0]; 
            localStorage.setItem(ACTIVE_CONFIG_NAME_STORAGE_KEY, currentActiveNameInMemory);
            localStorage.setItem(MODIFIED_FLAG_STORAGE_KEY, 'false'); 
             isModified = false; // <<< Update local variable too
        } else if (!currentActiveNameInMemory && scenarioNames.length === 0) {
             currentActiveNameInMemory = null;
             localStorage.removeItem(MODIFIED_FLAG_STORAGE_KEY); 
             isModified = false; // <<< Update local variable too
        }
        // <<< Note: No need to re-read isModified from localStorage, just update the local variable >>>

        // Determine showUnsavedRow AFTER potential default assignment and flag reset
        const showUnsavedRow = (!currentActiveNameInMemory || isModified); 

        // Prepend "Unsaved Changes" row if needed
        if (showUnsavedRow) {
            const unsavedRow = tableBody.insertRow(0); // Insert at the top
            
            const nameCell = unsavedRow.insertCell();
            nameCell.innerHTML = '<i>(Unsaved Changes)</i>';
            nameCell.colSpan = 1; 

            const actionsCell = unsavedRow.insertCell();
            actionsCell.className = 'text-end';
            
            const saveBtn = document.createElement('button');
            saveBtn.className = 'btn btn-sm btn-success py-0 px-1'; 
            saveBtn.innerHTML = '<i class="bi bi-save"></i>';
            saveBtn.title = 'Save current changes as new scenario...';
            saveBtn.addEventListener('click', handleSaveCurrentAs);
            actionsCell.appendChild(saveBtn);
        }

        // Populate existing saved scenarios
        if (scenarioNames.length === 0 && !showUnsavedRow) { 
            // <<< Explicitly add "No scenarios" row >>>
            const noScenarioRow = tableBody.insertRow();
            const cell = noScenarioRow.insertCell();
            cell.colSpan = 2;
            cell.className = 'text-center text-muted small';
            cell.textContent = 'No scenarios saved yet. Use the save icon above if needed.';
            return; // Stop here if no saved scenarios and no unsaved changes row needed
        } 

        // --- Loop through and add actual saved scenario rows ---
        scenarioNames.forEach(name => {
            const row = tableBody.insertRow(); // Appends to end by default
            row.dataset.scenarioName = name; 

            // Name Column (with status indicator)
            const nameCell = row.insertCell();
            nameCell.textContent = name;
            // Status indicator logic only applies if this scenario IS the active one
            if (name === currentActiveNameInMemory) { 
                const statusSpan = document.createElement('span');
                statusSpan.className = 'badge rounded-pill ms-2 fw-normal'; 
                if (isModified) {
                     statusSpan.textContent = 'Last';
                     statusSpan.classList.add('bg-warning-subtle', 'text-warning-emphasis'); 
                } else {
                     statusSpan.textContent = 'Active';
                     statusSpan.classList.add('bg-success-subtle', 'text-success-emphasis'); 
                }
                nameCell.appendChild(statusSpan);
            }

            // Actions Column
            const actionsCell = row.insertCell();
            actionsCell.className = 'text-end';
            // Export Button
            const exportBtn = document.createElement('button');
            exportBtn.className = 'btn btn-sm btn-outline-secondary py-0 px-1 me-1';
            exportBtn.innerHTML = '<i class="bi bi-box-arrow-down"></i>';
            exportBtn.title = `Export '${name}'`;
            exportBtn.addEventListener('click', () => handleExportScenario(name));
            actionsCell.appendChild(exportBtn);
            // Delete Button
            const deleteBtn = document.createElement('button');
            deleteBtn.className = 'btn btn-sm btn-outline-danger py-0 px-1';
            deleteBtn.innerHTML = '<i class="bi bi-trash"></i>';
            deleteBtn.title = `Delete '${name}'`;
            deleteBtn.addEventListener('click', () => handleDeleteScenario(name));
            actionsCell.appendChild(deleteBtn);

            // Add Reload Button (conditionally disabled)
            const reloadBtn = document.createElement('button');
            reloadBtn.className = 'btn btn-sm btn-outline-primary py-0 px-1 ms-1'; 
            reloadBtn.innerHTML = '<i class="bi bi-play-circle"></i>';
            reloadBtn.title = `Load scenario '${name}'`;
            reloadBtn.addEventListener('click', () => handleReloadScenario(name)); 
            // Disable reload if scenario is currently Active (not modified)
            if (name === currentActiveNameInMemory && !isModified) {
                reloadBtn.disabled = true;
                reloadBtn.title = `Scenario '${name}' is already active`;
            }
            actionsCell.appendChild(reloadBtn);
        });
    }
    
    /**
     * Handles the click event for the activate button.
     */
    function handleActivateScenario(selectedName) {
        const currentActiveNameInMemory = localStorage.getItem(ACTIVE_CONFIG_NAME_STORAGE_KEY);

        if (selectedName === currentActiveNameInMemory) {
            console.log("[ConfigManager] Selected scenario is already the active one.");
            updateStatus("This scenario is already active.");
            return; // Do nothing if already active
        }

        // Confirmation dialog
        if (confirm(`Activate scenario "${selectedName}"?\n\nThe page will refresh to load the selected settings.`)) {
            console.log(`[ConfigManager] User confirmed activation for "${selectedName}"`);
            // Write the selected config to the original LS keys
            const success = activateConfiguration(selectedName);
            if (success) {
                // Update the active name marker in LS
                localStorage.setItem(ACTIVE_CONFIG_NAME_STORAGE_KEY, selectedName);
                // Update status briefly before refresh
                 updateStatus(`Scenario "${selectedName}" activated. Refreshing page...`, false, true); // isError=false, persist=true
                // Automatically refresh the page
                setTimeout(() => { window.location.reload(); }, 500); // Short delay for status visibility
            } else {
                updateStatus(`Error activating scenario "${selectedName}". Check console.`, true);
            }
        } else {
            console.log("[ConfigManager] User cancelled activation.");
            updateStatus("Activation cancelled.");
        }
    }

    /**
     * Handles the click event for the "Save Current As..." button.
     */
    function handleSaveCurrentAs() {
        const newName = prompt("Enter a name for this scenario:");
        if (newName && newName.trim()) {
            const trimmedName = newName.trim();
            const configs = _getConfigurationsObject();
            if (configs.hasOwnProperty(trimmedName)) {
                if (!confirm(`Scenario named "${trimmedName}" already exists. Overwrite?`)) {
                    updateStatus("Save cancelled.");
                    return;
                }
            }
            const success = saveCurrentAppSettingsAs(trimmedName);
            if (success) {
                updateStatus(`Scenario "${trimmedName}" saved successfully.`);
                localStorage.setItem(ACTIVE_CONFIG_NAME_STORAGE_KEY, trimmedName);
                localStorage.setItem(MODIFIED_FLAG_STORAGE_KEY, 'false'); 
                populateScenarioTable(); // Refresh table to show new/updated entry
            } else {
                 updateStatus(`Error saving scenario "${trimmedName}". Check console.`, true);
            }
        } else if (newName !== null) { // User didn't cancel, but entered empty name
            updateStatus("Scenario name cannot be empty.", true);
        } else { // User cancelled prompt
             updateStatus("Save cancelled.");
        }
    }

    /**
     * Handles the click event for a scenario's Delete button.
     */
    function handleDeleteScenario(name) {
        const currentActiveNameInMemory = localStorage.getItem(ACTIVE_CONFIG_NAME_STORAGE_KEY);
        if (name === currentActiveNameInMemory) {
             updateStatus(`Cannot delete the currently active scenario "${name}".`, true);
             alert(`Cannot delete the currently active scenario "${name}".`);
             return;
        }

        if (confirm(`Are you sure you want to delete the scenario "${name}"?`)) {
            const success = deleteConfiguration(name);
            if (success) {
                updateStatus(`Scenario "${name}" deleted.`);
                populateScenarioTable(); // Refresh table
            } else {
                updateStatus(`Error deleting scenario "${name}". Check console.`, true);
            }
        } else {
             updateStatus("Deletion cancelled.");
        }
    }

    /**
     * Handles the click event for a scenario's Export button.
     */
    function handleExportScenario(name) {
        console.log(`[ConfigManager] Exporting scenario "${name}"...`);
        const configData = getConfiguration(name);
        if (!configData) {
            updateStatus(`Error exporting: Scenario "${name}" not found.`, true);
            return;
        }

        try {
            const jsonString = JSON.stringify(configData, null, 2); // Pretty print JSON
            const blob = new Blob([jsonString], { type: "application/json" });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            // Sanitize name for filename
            const safeName = name.replace(/[^a-z0-9\-_\.]/gi, '_');
            a.download = `analytics_scenario_${safeName}.json`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
            updateStatus(`Scenario "${name}" exported.`);
        } catch (e) {
            console.error(`[ConfigManager] Error exporting scenario "${name}":`, e);
            updateStatus(`Error exporting scenario "${name}". Check console.`, true);
        }
    }

    /**
     * Handles the setup and file reading for scenario import.
     */
    function setupImportHandling() {
        const fileInput = document.getElementById('analytics-scenario-import-input');
        const dropZone = document.getElementById('analytics-scenario-import-drop-zone');
        const importStatusEl = document.getElementById('analytics-import-status');
        const dropZoneP = dropZone ? dropZone.querySelector('p') : null; 
        const dropZoneText = dropZoneP ? dropZoneP.querySelector('span.status-text') : null;
        const dropZoneIcon = dropZoneP ? dropZoneP.querySelector('span.status-icon') : null;
        // const saveImportedBtn = document.getElementById('analytics-scenario-save-imported-btn'); // Button removed

        if (!fileInput || !dropZone || !importStatusEl || !dropZoneP || !dropZoneText || !dropZoneIcon) { // Removed saveBtn check
            console.error("[ConfigManager] Import UI elements not found (fileInput, dropZone, statusEl, p, text, icon).");
            return;
        }

        // Initial state: No save button to disable

        // Trigger file input from drop zone click
        dropZone.addEventListener('click', () => fileInput.click());

        // Handle file selection via input
        fileInput.addEventListener('change', (event) => {
            if (event.target.files && event.target.files.length > 0) {
                handleImportFile(event.target.files[0]);
            }
        });

        // --- Corrected Drag and Drop Handling ---
        dropZone.addEventListener('dragover', (event) => {
            event.preventDefault(); 
            // No staged data to clear 
            dropZone.classList.add('dragover'); 
        });
        dropZone.addEventListener('dragleave', () => {
            dropZone.classList.remove('dragover'); 
        });
        dropZone.addEventListener('drop', (event) => {
            event.preventDefault();
            dropZone.classList.remove('dragover'); 
            // No staged data to clear 
            if (event.dataTransfer.files && event.dataTransfer.files.length > 0) {
                const file = event.dataTransfer.files[0];
                fileInput.files = event.dataTransfer.files; 
                handleImportFile(file);
                event.dataTransfer.clearData();
            }
        });
        // --- End Corrected Drag and Drop Handling ---

        // File validation and IMMEDIATE save logic (modified)
        function handleImportFile(file) {
            // Clear previous UI state immediately
            // resetDropZoneAppearanceAfterDelay(false, 0); // Reset instantly // <<< REMOVED: Don't reset immediately, let new status show
            importStatusEl.textContent = ''; // Still clear external status
            importStatusEl.className = 'small text-muted';
            
            if (!file) {
                 dropZoneText.textContent = 'Drag & drop .json file here, or click to select';
                 dropZoneIcon.className = 'status-icon bi bi-cloud-arrow-up me-2 align-middle text-muted';
                 fileInput.value = ''; 
                 return;
            }

            if (!file.name.toLowerCase().endsWith('.json')) { 
                // importStatusEl.textContent = `Invalid file type: ${file.name}. Please use a .json file.`; // <<< COMMENTED OUT
                // importStatusEl.className = 'small text-danger'; // <<< COMMENTED OUT
                dropZoneText.textContent = `Invalid file type: ${file.name} (.json only)`; // <<< UPDATED: Show inside
                dropZoneIcon.className = 'status-icon bi bi-x-octagon-fill me-2 align-middle text-danger'; // <<< UPDATED: Show error icon
                // resetDropZoneAppearanceAfterDelay(); // <<< REMOVED: Keep error visible
                fileInput.value = ''; 
                return;
            }

            const reader = new FileReader();
            reader.onload = function(event) {
                let validatedData = null; // Temporary holder within this scope
                try {
                    const content = event.target.result;
                    validatedData = JSON.parse(content); // Store parsed data temporarily

                    // Basic validation of structure
                    if (!validatedData || typeof validatedData !== 'object' || 
                        !validatedData.hasOwnProperty('filters') || 
                        !validatedData.hasOwnProperty('rules') || 
                        !validatedData.hasOwnProperty('fieldSettings')) {
                        throw new Error("Invalid scenario file structure. Missing required keys (filters, rules, fieldSettings).");
                    }
                    
                    // --- Validation Success: Derive Name, Check Overwrite, Save --- 
                    const filename = file.name;
                    // <<< Remove detailed Debug Logs >>>
                    const trimmedFilename = filename.trim();
                    
                    // <<< Replace regex with endsWith/slice >>>
                    let nameAfterSuffixReplace = trimmedFilename;
                    if (trimmedFilename.toLowerCase().endsWith('.json')) {
                        nameAfterSuffixReplace = trimmedFilename.slice(0, -5); // Remove last 5 characters
                    }
                    const finalSaveName = nameAfterSuffixReplace.replace(/[^a-z0-9\-_\.]/gi, '_').substring(0, 50) || 'Imported_Scenario';
                    
                    // Use finalSaveName going forward
                    const saveName = finalSaveName;

                    if (!saveName) { // Should be unlikely but check anyway
                         throw new Error("Could not derive a valid scenario name from the filename.");
                    }

                    const configs = _getConfigurationsObject();
                    if (configs.hasOwnProperty(saveName)) {
                        if (!confirm(`Scenario named "${saveName}" already exists (from file ${filename}). Overwrite?`)) {
                            // importStatusEl.textContent = "Import cancelled by user (overwrite declined)."; // <<< COMMENTED OUT
                            // importStatusEl.className = 'small text-warning'; // <<< COMMENTED OUT
                            dropZoneText.textContent = `Import cancelled: Scenario '${saveName}' exists.`; // <<< UPDATED: Show inside
                            dropZoneIcon.className = 'status-icon bi bi-exclamation-triangle-fill me-2 align-middle text-warning'; // <<< UPDATED: Warning icon
                            // resetDropZoneAppearanceAfterDelay(); // <<< REMOVED: Keep warning visible
                            fileInput.value = ''; // Clear input as import is cancelled
                            return; // Stop processing
                        }
                        // User confirmed overwrite
                    }

                    // Save the validated data immediately
                    configs[saveName] = validatedData;
                    _saveConfigurationsObject(configs);

                    // <<< Update status message to use ONLY saveName >>>
                    // importStatusEl.textContent = `Scenario "${saveName}" imported successfully.`; // <<< COMMENTED OUT
                    // importStatusEl.className = 'small text-success'; // <<< COMMENTED OUT
                    // Use saveName (no suffix) for drop zone text
                    dropZoneText.textContent = `Imported: ${saveName}`; // <<< Kept as is
                    dropZoneIcon.className = 'status-icon bi bi-check-circle-fill me-2 align-middle text-success'; // <<< Kept as is
                    populateScenarioTable(); // Refresh the table
                    // resetDropZoneAppearanceAfterDelay(true); // Reset drop zone appearance after delay (success state) // <<< REMOVED: Keep success visible until next action
                    
                    // If this is the first scenario imported, make it active
                    if (Object.keys(_getConfigurationsObject()).length === 1) {
                        localStorage.setItem(ACTIVE_CONFIG_NAME_STORAGE_KEY, saveName);
                        populateScenarioTable(); // Refresh again to show active status
                    }
                    // ------------------------------------------------------------------

                } catch (e) {
                    // --- Validation/Parse/Save Failure --- 
                    console.error("[ConfigManager] Error processing/saving imported file:", e);
                    // importStatusEl.textContent = `Error importing file: ${e.message}`; // <<< COMMENTED OUT
                    // importStatusEl.className = 'small text-danger'; // <<< COMMENTED OUT
                    dropZoneText.textContent = `Import Error: ${e.message}`; // <<< UPDATED: Show error inside
                    dropZoneIcon.className = 'status-icon bi bi-exclamation-triangle-fill me-2 align-middle text-danger'; // <<< UPDATED: Error icon
                    // resetDropZoneAppearanceAfterDelay(); // <<< REMOVED: Keep error visible
                } finally {
                     // Clear file input regardless of success/failure in this path
                     if(fileInput) fileInput.value = ''; 
                }
            };
            reader.onerror = function(event) {
                // --- File Reading Failure --- 
                console.error("[ConfigManager] File reading error:", event);
                // importStatusEl.textContent = `Error reading file: ${file.name}`; // <<< COMMENTED OUT
                // importStatusEl.className = 'small text-danger'; // <<< COMMENTED OUT
                dropZoneText.textContent = `Error reading file: ${file.name}`; // <<< UPDATED: Show error inside
                dropZoneIcon.className = 'status-icon bi bi-exclamation-triangle-fill me-2 align-middle text-danger'; // <<< UPDATED: Error icon
                // resetDropZoneAppearanceAfterDelay(); // <<< REMOVED: Keep error visible
                fileInput.value = ''; // Reset input on reader error
            };

            // --- Update UI Before Reading ---
            importStatusEl.textContent = `Reading file ${file.name}...`;
            importStatusEl.className = 'small text-info';
            dropZoneText.textContent = `Processing: ${file.name}`;
            dropZoneIcon.className = 'status-icon spinner-border spinner-border-sm me-2 align-middle text-info'; 
            reader.readAsText(file);
        }

        // Function to reset drop zone appearance after a delay
        function resetDropZoneAppearanceAfterDelay(isSuccess = false, delay = 3000) { 
             // THIS FUNCTION IS NO LONGER CALLED IN MOST CASES
             // It was previously used to revert the appearance after success/error.
             // Now, status messages persist inside the drop zone until the next action.
             // Keeping the function definition in case it's needed elsewhere, but removing calls.
             console.warn('[ConfigManager] resetDropZoneAppearanceAfterDelay called, but its calls have been removed for persistent status.');
             /*
             setTimeout(() => {
                // Reset text and icon back to the initial prompt
                dropZoneText.textContent = 'Drag & drop .json file here, or click to select';
                dropZoneIcon.className = 'status-icon bi bi-cloud-arrow-up me-2 align-middle text-muted'; 
             }, delay); 
             */
        }

        // Helper to clear staged data and reset related UI - REMOVED as staging is removed
        // function clearStagedImport() { ... }

        // --- REMOVED Handler for the 'Save Imported Scenario' button ---
        // function handleSaveStagedImport() { ... }

        // --- REMOVED Handler for the Save button INSIDE the modal ---
        // function handleModalSaveScenario() { ... }

        // Remove listener for the modal save button
        // const modalSaveBtn = document.getElementById('import-scenario-save-btn');
        // No saveImportedBtn listener needed
    }

    /**
     * Updates the status message area in the configuration tab.
     * @param {string} message - The message to display.
     * @param {boolean} isError - If true, style as an error message.
     * @param {boolean} persist - If true, don't auto-clear the message.
     */
    function updateStatus(message, isError = false, persist = false) {
        const statusEl = document.getElementById('analytics-config-status');
        if (statusEl) {
            statusEl.textContent = message;
            statusEl.className = isError ? 'small text-danger ms-3' : 'small text-success ms-3';
            
            // Clear message after a delay unless persist is true
            if (!persist) {
                setTimeout(() => {
                    if (statusEl.textContent === message) { // Only clear if message hasn't changed
                        statusEl.textContent = '';
                        statusEl.className = 'small text-muted ms-3';
                    }
                }, 5000); // Clear after 5 seconds
            }
        }
    }

    /**
     * Initializes UI elements and event listeners for the config manager.
     */
    function initializeUI() {
        populateScenarioTable();
        setupImportHandling(); // Sets up drop zone, file input listeners

        // Add listener for "Save Current As..."
        const saveAsBtn = document.getElementById('analytics-config-save-as-btn');
        if (saveAsBtn) {
            saveAsBtn.addEventListener('click', handleSaveCurrentAs);
        }

        // Add listeners for Reload Modal Buttons 
        const reloadModalDiscardBtn = document.getElementById('reload-scenario-discard-btn');
        const reloadModalSaveBtn = document.getElementById('reload-scenario-save-btn');
        const reloadModalLoadBtn = document.getElementById('reload-scenario-load-btn'); // <<< Get Load button

        if (reloadModalDiscardBtn) {
            reloadModalDiscardBtn.addEventListener('click', handleModalDiscardAndReload);
        } else {
            console.error("[ConfigManager] Reload Modal Discard button not found!");
        }
        if (reloadModalSaveBtn) {
            reloadModalSaveBtn.addEventListener('click', handleModalSaveAndReload);
        } else {
             console.error("[ConfigManager] Reload Modal Save button not found!");
        }
        // <<< Add listener for Load button >>>
         if (reloadModalLoadBtn) {
            reloadModalLoadBtn.addEventListener('click', handleModalLoadScenario);
        } else {
             console.error("[ConfigManager] Reload Modal Load button not found!");
        }

        // Note: Listeners for activate, delete, export are added dynamically in populateScenarioTable
    }

    // --- NEW: Add listeners to detect changes in other tabs ---
    function initializeModificationDetection() {
        console.log("[Modification DEBUG] Initializing/Re-initializing modification detection listeners...");

        // --- Remove existing listeners first --- //
        const configSubTabPane = document.getElementById('config-subtab-pane');
        const fieldConfigContainer = configSubTabPane ? configSubTabPane.querySelector('#field-config-container') : null;
        const transformTabPane = document.getElementById('transform-tab-pane');
        const transformModal = document.getElementById('transformationRuleModal');
        const formatModal = document.getElementById('numericFormatModal');
        const postTransformConfigContainer = document.getElementById('post-transform-field-config-container');

        if (fieldConfigContainer && prepFieldConfigChangeListener) {
            fieldConfigContainer.removeEventListener('change', prepFieldConfigChangeListener);
             console.log("[Modification DEBUG] Removed old prepFieldConfigChangeListener.");
        }
        if (fieldConfigContainer && prepFieldConfigClickListener) {
            fieldConfigContainer.removeEventListener('click', prepFieldConfigClickListener);
             console.log("[Modification DEBUG] Removed old prepFieldConfigClickListener.");
        }
        if (transformTabPane && transformPaneClickListener) {
            transformTabPane.removeEventListener('click', transformPaneClickListener);
             console.log("[Modification DEBUG] Removed old transformPaneClickListener.");
        }
         if (transformTabPane && transformPaneChangeListener) {
            transformTabPane.removeEventListener('change', transformPaneChangeListener);
             console.log("[Modification DEBUG] Removed old transformPaneChangeListener.");
        }
        if (transformModal && transformModalSaveListener) {
            const saveRuleBtn = transformModal.querySelector('#save-transformation-rule-btn');
            if (saveRuleBtn) saveRuleBtn.removeEventListener('click', transformModalSaveListener);
             console.log("[Modification DEBUG] Removed old transformModalSaveListener.");
        }
        if (formatModal && formatModalSaveListener) {
            const saveFormatBtn = formatModal.querySelector('#save-format-btn');
            if (saveFormatBtn) saveFormatBtn.removeEventListener('click', formatModalSaveListener);
             console.log("[Modification DEBUG] Removed old formatModalSaveListener.");
        }
        if (postTransformConfigContainer && postTransformChangeListener) {
            postTransformConfigContainer.removeEventListener('change', postTransformChangeListener);
             console.log("[Modification DEBUG] Removed old postTransformChangeListener.");
        }
        if (postTransformConfigContainer && postTransformInputListener) {
            postTransformConfigContainer.removeEventListener('input', postTransformInputListener);
             console.log("[Modification DEBUG] Removed old postTransformInputListener.");
        }
        // --- End Remove existing listeners ---

        // Re-assign listener functions and attach
        if (configSubTabPane && !prepPaneClickListener) {
            prepPaneClickListener = function(event) {
                if (event.target.matches('#apply-filters-btn, #reset-filters-btn, #add-filter-btn, .remove-filter-btn')) {
                    console.log("[Modification DEBUG] Filter button clicked. Calling _markScenarioAsModified...");
                    _markScenarioAsModified();
                }
            };
            configSubTabPane.addEventListener('click', prepPaneClickListener);
            console.log("[Modification DEBUG] Attached prepPaneClickListener to configSubTabPane.");
        }

        if (fieldConfigContainer) {
            prepFieldConfigChangeListener = function(event) {
                console.log("[Modification DEBUG] Change event detected in PRE-transform fieldConfigContainer.");
                const target = event.target;
                console.log("[Modification DEBUG] Event Target TagName:", target.tagName, "ClassNames:", target.className);
                if (target.matches('input[type="checkbox"].form-check-input') ||
                    target.matches('select.format-select') ||
                    target.matches('input.field-info-input'))
                {
                    console.log(`[Modification DEBUG] Matched PRE-transform config change on: ${target.tagName}.${target.className}. Calling _markScenarioAsModified...`);
                    if (!event.target.closest('.modal')) {
                         _markScenarioAsModified();
                    } else {
                         console.log("[Modification DEBUG] Ignoring change inside a modal.");
                    }
                }
            };
            fieldConfigContainer.addEventListener('change', prepFieldConfigChangeListener);
             console.log("[Modification DEBUG] Attached prepFieldConfigChangeListener to fieldConfigContainer.");

            prepFieldConfigClickListener = function(event) {
                if (event.target.closest('.configure-format-btn')) {
                    // No modification here, just logging
                }
             };
            fieldConfigContainer.addEventListener('click', prepFieldConfigClickListener);
             console.log("[Modification DEBUG] Attached prepFieldConfigClickListener to fieldConfigContainer.");
        } else {
             console.warn("[Modification DEBUG] Pre-transform #field-config-container not found. Cannot attach listeners.");
        }

        if (transformTabPane) {
            transformPaneClickListener = function(event) {
                if (event.target.matches('#apply-transformations-btn, #save-transform-rules-btn, #load-transform-rules-btn, .delete-rule-btn, .move-rule-btn')) {
                    console.log("[Modification DEBUG] Transform control button clicked. Calling _markScenarioAsModified...");
                    _markScenarioAsModified();
                }
            };
            transformTabPane.addEventListener('click', transformPaneClickListener);
             console.log("[Modification DEBUG] Attached transformPaneClickListener to transformTabPane.");

            transformPaneChangeListener = function(event) {
                if (event.target.matches('.list-group-item input[type="checkbox"].form-check-input')) {
                    console.log("[Modification DEBUG] Transform rule enable toggle changed. Calling _markScenarioAsModified...");
                    _markScenarioAsModified();
                }
            };
            transformTabPane.addEventListener('change', transformPaneChangeListener);
             console.log("[Modification DEBUG] Attached transformPaneChangeListener to transformTabPane.");
        }

        if (transformModal) {
            const saveRuleBtn = transformModal.querySelector('#save-transformation-rule-btn');
            if (saveRuleBtn) {
                transformModalSaveListener = () => {
                    console.log("[Modification DEBUG] Transform modal save clicked. Calling _markScenarioAsModified...");
                    _markScenarioAsModified();
                };
                saveRuleBtn.addEventListener('click', transformModalSaveListener);
                 console.log("[Modification DEBUG] Attached transformModalSaveListener to transformModal.");
            }
        }

        if (formatModal) {
            const saveFormatBtn = formatModal.querySelector('#save-format-btn');
            if (saveFormatBtn) {
                formatModalSaveListener = () => {
                     console.log("[Modification DEBUG] Format modal save clicked. Calling _markScenarioAsModified...");
                    _markScenarioAsModified();
                };
                saveFormatBtn.addEventListener('click', formatModalSaveListener);
                 console.log("[Modification DEBUG] Attached formatModalSaveListener to formatModal.");
            }
        }

        if (postTransformConfigContainer) {
            console.log("[Modification DEBUG] Attaching listeners to postTransformConfigContainer...");
            postTransformChangeListener = function(event) {
                console.log("[Modification DEBUG] Change event detected in POST-transform container.");
                const target = event.target;
                 if (target.matches('input[type="checkbox"].form-check-input') || target.matches('select.form-select')) {
                     console.log(`[Modification DEBUG] Matched POST-transform config change (enable/format) on: ${target.tagName}.${target.className}. Calling _markScenarioAsModified...`);
                     _markScenarioAsModified();
                 }
            };
            postTransformConfigContainer.addEventListener('change', postTransformChangeListener);
             console.log("[Modification DEBUG] Attached postTransformChangeListener to postTransformConfigContainer.");

            postTransformInputListener = function(event) {
                 console.log("[Modification DEBUG] Input event detected in POST-transform container.");
                const target = event.target;
                if (target.matches('input.field-info-input')) {
                    console.log(`[Modification DEBUG] Matched POST-transform info tip input change. Calling _markScenarioAsModified...`);
                    _markScenarioAsModified();
                }
            };
            postTransformConfigContainer.addEventListener('input', postTransformInputListener);
             console.log("[Modification DEBUG] Attached postTransformInputListener to postTransformConfigContainer.");
        } else {
            console.warn("[ConfigManager] Post-transform config container not found. Cannot attach modification listeners.");
        }

        console.log("[ConfigManager] Modification detection listeners attached/re-attached.");
    }

    // Handler for Reload Button Click
    function handleReloadScenario(name) {
        console.log(`[ConfigManager] Reload clicked for: ${name}`);
        scenarioToReload = name; // Store the name for modal buttons

        const modalElement = document.getElementById('reloadScenarioModal');
        if (!reloadScenarioModalInstance) {
            if (!modalElement) {
                 console.error("[ConfigManager] Reload scenario modal element not found!");
                 updateStatus("Error: Cannot open reload dialog.", true);
                 return;
            }
            if (typeof bootstrap === 'undefined' || !bootstrap.Modal) {
                 console.error("[ConfigManager] Bootstrap Modal component not found.");
                 updateStatus("Error: UI component missing.", true);
                 return;
            }
            reloadScenarioModalInstance = new bootstrap.Modal(modalElement);
        }
        
        // <<< Get modal sub-elements >>>
        const modalTitle = modalElement.querySelector('#reloadScenarioModalLabel');
        const scenarioNameEl = modalElement.querySelector('#reload-scenario-name');
        const warningTextEl = modalElement.querySelector('#reload-warning-text');
        const saveBtn = modalElement.querySelector('#reload-scenario-save-btn');
        const discardBtn = modalElement.querySelector('#reload-scenario-discard-btn');
        const loadBtn = modalElement.querySelector('#reload-scenario-load-btn');

        if (!modalTitle || !scenarioNameEl || !warningTextEl || !saveBtn || !discardBtn || !loadBtn) {
            console.error("[ConfigManager] One or more elements missing inside reload modal.");
            updateStatus("Error: Cannot open reload dialog correctly.", true);
            return;
        }

        // Set scenario name display
        scenarioNameEl.textContent = name; 

        // <<< Check modification status and adjust modal content >>>
        const isModified = localStorage.getItem(MODIFIED_FLAG_STORAGE_KEY) === 'true';

        if (isModified) {
            // Scenario IS modified - show warning and Save/Discard buttons
            modalTitle.textContent = 'Reload Scenario';
            warningTextEl.style.display = ''; // Show warning
            saveBtn.style.display = ''; // Show Save button
            discardBtn.style.display = ''; // Show Discard button
            loadBtn.style.display = 'none'; // Hide Load button
        } else {
            // Scenario NOT modified - hide warning, show only Load button
            modalTitle.textContent = 'Load Scenario';
            warningTextEl.style.display = 'none'; // Hide warning
            saveBtn.style.display = 'none'; // Hide Save button
            discardBtn.style.display = 'none'; // Hide Discard button
            loadBtn.style.display = ''; // Show Load button
        }
        
        reloadScenarioModalInstance.show();
    }

    // Handler for 'Discard Changes & Reload' button in modal
    function handleModalDiscardAndReload() {
        if (!scenarioToReload) {
            console.error("[ConfigManager] Discard & Reload: scenarioToReload name is missing.");
            return;
        }
        console.log(`[ConfigManager] Discarding changes and reloading scenario: ${scenarioToReload}`);
        // <<< Call applyScenarioSettingsDirectly instead of activateConfiguration >>>
        const success = applyScenarioSettingsDirectly(scenarioToReload);
        if (success) {
            // localStorage.setItem(ACTIVE_CONFIG_NAME_STORAGE_KEY, scenarioToReload); // <<< Already handled by applyScenarioSettingsDirectly >>>
            updateStatus(`Scenario \"${scenarioToReload}\" loaded.`); // <<< Updated message
            reloadScenarioModalInstance.hide(); // Hide modal
            // setTimeout(() => window.location.reload(), 300); // <<< REMOVED Page Refresh >>>
        } else {
            // Should be rare, but handle error
            updateStatus(`Error loading scenario \"${scenarioToReload}\". Check console.`, true);
            reloadScenarioModalInstance.hide(); // Also hide modal on error
        }
        scenarioToReload = null; // Clear stored name
    }

    // --- NEW: Handler for 'Load' button in modal (when not modified) ---
    // This performs the same action as discarding, as there are no changes.
    function handleModalLoadScenario() {
         if (!scenarioToReload) {
            console.error("[ConfigManager] Load Scenario: scenarioToReload name is missing.");
            return;
        }
        console.log(`[ConfigManager] Loading scenario: ${scenarioToReload}`);
        // <<< Call applyScenarioSettingsDirectly instead of activateConfiguration >>>
        const success = applyScenarioSettingsDirectly(scenarioToReload);
        if (success) {
            // localStorage.setItem(ACTIVE_CONFIG_NAME_STORAGE_KEY, scenarioToReload); // <<< Already handled by applyScenarioSettingsDirectly >>>
            updateStatus(`Scenario \"${scenarioToReload}\" loaded.`); // <<< Updated message
            reloadScenarioModalInstance.hide(); // Hide modal
            // setTimeout(() => window.location.reload(), 300); // <<< REMOVED Page Refresh >>>
        } else {
            updateStatus(`Error loading scenario \"${scenarioToReload}\". Check console.`, true);
            reloadScenarioModalInstance.hide(); // Also hide modal on error
        }
        scenarioToReload = null; // Clear stored name
    }

    // Handler for 'Save Changes & Reload' button in modal 
    function handleModalSaveAndReload() {
        if (!scenarioToReload) {
            console.error("[ConfigManager] Save & Reload: scenarioToReload name is missing.");
            return;
        }
        console.log(`[ConfigManager] Saving current changes before reloading scenario: ${scenarioToReload}`);
        
        // 1. Trigger the "Save Current As..." logic
        const savedName = prompt("Save current settings as scenario name:"); // Reuse existing save logic prompt
        if (savedName && savedName.trim()) {
            const trimmedSaveName = savedName.trim();
            const configs = _getConfigurationsObject();
            let proceedToSave = true;
            if (configs.hasOwnProperty(trimmedSaveName)) {
                if (!confirm(`Scenario named "${trimmedSaveName}" already exists. Overwrite?`)) {
                    proceedToSave = false;
                }
            }

            if (proceedToSave) {
                const saveSuccess = saveCurrentAppSettingsAs(trimmedSaveName); // Save the current state
                if (saveSuccess) {
                     updateStatus(`Current settings saved as "${trimmedSaveName}".`);
                     populateScenarioTable(); // Update table to show newly saved scenario
                     
                     // 2. Now proceed with reloading the original target scenario
                     console.log(`[ConfigManager] Save successful. Now loading original target: ${scenarioToReload}`);
                     // <<< Call applyScenarioSettingsDirectly instead of activateConfiguration >>>
                     const reloadSuccess = applyScenarioSettingsDirectly(scenarioToReload);
                     if (reloadSuccess) {
                        // localStorage.setItem(ACTIVE_CONFIG_NAME_STORAGE_KEY, scenarioToReload); // <<< Already handled by applyScenarioSettingsDirectly >>>
                        updateStatus(`Scenario \"${scenarioToReload}\" loaded after saving changes.`); // <<< Updated message
                        reloadScenarioModalInstance.hide();
                        // setTimeout(() => window.location.reload(), 300); // <<< REMOVED Page Refresh >>>
                     } else {
                         updateStatus(`Changes saved, but error reloading scenario \"${scenarioToReload}\". Check console.`, true);
                         reloadScenarioModalInstance.hide(); // Also hide modal on error
                     }
                } else {
                     updateStatus(`Error saving current settings as \"${trimmedSaveName}\". Reload cancelled.`, true);
                     // Keep modal open? Or close? Let's close it for now.
                     reloadScenarioModalInstance.hide(); 
                }
            } else {
                 updateStatus("Save cancelled (overwrite declined). Reload cancelled.");
                 // Keep modal open?
                 reloadScenarioModalInstance.hide();
            }
        } else if (savedName !== null) { 
            updateStatus("Save cancelled (empty name). Reload cancelled.", true);
             reloadScenarioModalInstance.hide();
        } else { // User cancelled prompt
             updateStatus("Save cancelled. Reload cancelled.");
             reloadScenarioModalInstance.hide();
        }
        scenarioToReload = null; // Clear stored name
    }

    // --- NEW: Handler for 'Update' button (for Last scenario) ---
    function handleUpdateScenario(name) {
        if (confirm(`Update scenario "${name}" with the current settings? This will overwrite the saved version.`)) {
            console.log(`[ConfigManager] Updating scenario "${name}" with current settings...`);
            const saveSuccess = saveCurrentAppSettingsAs(name);
            if (saveSuccess) {
                // Overwrite successful, mark as no longer modified
                localStorage.setItem(MODIFIED_FLAG_STORAGE_KEY, 'false'); 
                populateScenarioTable(); // Refresh table to show "Active" status
                updateStatus(`Scenario "${name}" updated successfully.`);
            } else {
                updateStatus(`Error updating scenario "${name}". Check console.`, true);
            }
        } else {
            updateStatus("Update cancelled.");
        }
    }

    // --- Public Methods & Initialization Hook ---
    return {
        // Public methods from core utilities:
        getAllConfigurationNames: getAllConfigurationNames,
        saveCurrentAppSettingsAs: saveCurrentAppSettingsAs,
        activateConfiguration: activateConfiguration,
        deleteConfiguration: deleteConfiguration,
        getConfiguration: getConfiguration,
        // UI Initialization function:
        initializeUI: initializeUI,
        // <<< NEW: Expose modification detection initializer >>>
        initializeModificationDetection: initializeModificationDetection
    };

})(); 

// === Prevent Default Browser Drag/Drop Behavior ===
// Prevent the browser from trying to open the file itself when dropped outside the zone
window.addEventListener("dragover", function(e){
    e.preventDefault();
},false);
window.addEventListener("drop", function(e){
    e.preventDefault();
},false);

// === Initialization ===
document.addEventListener('DOMContentLoaded', () => {
    // Check if the config manager object exists (it should if the script loaded)
    if (window.AnalyticsConfigManager) {
        console.log("[ConfigManager] DOM Loaded. Initializing UI bindings.");
        AnalyticsConfigManager.initializeUI();
    } else {
        console.error("[ConfigManager] AnalyticsConfigManager object not found after DOMContentLoaded!");
    }
}); 