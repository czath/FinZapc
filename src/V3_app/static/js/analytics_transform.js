document.addEventListener('DOMContentLoaded', function() {
    console.log("Analytics_transform.js: DOMContentLoaded event fired.");

    // --- Global Variables for Transformations --- 
    let transformationRules = []; // Array to hold transformation rule definitions
    let transformedData = [];     // Array to hold the data after transformations
    const TRANSFORM_RULES_STORAGE_KEY = 'analyticsTransformationRules';

    // --- Element References (Transform Tab) --- 
    const rulesContainer = document.getElementById('transformation-rules-container');
    const addRuleButton = document.getElementById('add-transformation-rule-btn');
    const applyTransformationsButton = document.getElementById('apply-transformations-btn');
    const saveRulesButton = document.getElementById('save-transform-rules-btn');
    const loadRulesButton = document.getElementById('load-transform-rules-btn');
    const transformStatus = document.getElementById('transform-status');
    const transformedDataOutput = document.getElementById('transformed-data-output');
    
    // --- Modal Element References ---
    const ruleModal = document.getElementById('transformationRuleModal');
    const saveRuleButton = document.getElementById('save-transformation-rule-btn');
    const transformRuleTypeSelect = document.getElementById('transform-rule-type');
    const transformOutputNameInput = document.getElementById('transform-output-name');
    const transformRuleCommentInput = document.getElementById('transform-rule-comment');
    const transformParametersContainer = document.getElementById('transform-parameters-container');
    const transformRuleIdInput = document.getElementById('transform-rule-id');
    const transformModalStatus = document.getElementById('transform-modal-status');
    const outputNameFeedback = document.getElementById('output-name-feedback');
    const bsRuleModal = ruleModal ? new bootstrap.Modal(ruleModal) : null; // Bootstrap Modal instance

    // --- Rule Type Definitions --- 
    // TODO: Expand this with more types and their parameter definitions
    const ruleTypeDefinitions = {
        'arithmetic': {
            name: 'Arithmetic operation',
            description: 'Create a new field using a formula involving existing numeric fields.',
            parameters: [
                { id: 'formula', name: 'Formula', type: 'textarea', placeholder: 'Example: ({P/E} * {EPS (ttm)}) + 10', required: true },
            ]
        },
        // Add other types like 'ratio', 'weighted_sum', etc. later
    };

    // --- Functions for Rule Persistence --- 
    function loadTransformationRulesFromStorage() {
        console.log("Transform: Loading rules from localStorage...");
        const savedRules = localStorage.getItem(TRANSFORM_RULES_STORAGE_KEY);
        if (savedRules) {
            try {
                transformationRules = JSON.parse(savedRules);
                if (!Array.isArray(transformationRules)) {
                    transformationRules = [];
                }
                console.log("Loaded rules:", transformationRules);
            } catch (e) {
                console.error("Error parsing saved transformation rules:", e);
                transformationRules = [];
                localStorage.removeItem(TRANSFORM_RULES_STORAGE_KEY);
            }
        } else {
            transformationRules = []; // Initialize empty if nothing saved
            console.log("No saved transformation rules found.");
        }
    }

    function saveTransformationRulesToStorage() {
        console.log("Transform: Saving rules to localStorage...");
        try {
            localStorage.setItem(TRANSFORM_RULES_STORAGE_KEY, JSON.stringify(transformationRules));
            if(transformStatus) transformStatus.textContent = "Rules saved locally.";
        } catch (e) {
            console.error("Error saving transformation rules to localStorage:", e);
             if(transformStatus) transformStatus.textContent = "Error saving rules locally. Check console.";
        }
    }

    // --- Functions for Transformation Logic --- 
    function applyTransformations(inputData, rules) {
        console.log("Transform: applyTransformations called.");
        if (!inputData || !Array.isArray(inputData) || inputData.length === 0) {
            console.warn("Transform: No input data provided for transformations.");
            if(transformStatus) transformStatus.textContent = "No data to transform (Load & Filter data first)."
            return []; // Return empty array if no input
        }
        
        // Filter rules to only include defined types before proceeding
        const validRules = rules.filter(rule => ruleTypeDefinitions.hasOwnProperty(rule.type));
        const invalidRuleCount = rules.length - validRules.length;
        if (invalidRuleCount > 0) {
            console.warn(`${invalidRuleCount} rules skipped due to unknown type.`);
            // Optionally update status here
        }

        if (!validRules || !Array.isArray(validRules) || validRules.length === 0) {
             console.log("No valid/enabled transformation rules defined. Returning original data.");
              if(transformStatus) transformStatus.textContent = "No valid rules to apply."
             return inputData; // Return input data if no rules
         }
        
        // Create a deep copy to avoid modifying the original preparedData
        let workingData = JSON.parse(JSON.stringify(inputData));
        let ruleErrors = []; // Collect errors during processing
        
        // Determine initial available fields from the input data structure
        let initialFields = new Set();
        if (workingData.length > 0) {
             // Add keys from the top-level object (like ticker, source, error)
            Object.keys(workingData[0]).forEach(key => {
                if (key !== 'processed_data') { // Exclude the container itself
                    initialFields.add(key);
                }
            });
             // Add keys from the first row's processed_data, if it exists
             if (workingData[0].processed_data) {
                 Object.keys(workingData[0].processed_data).forEach(key => initialFields.add(key));
             }
        }
        let availableFieldsDuringTransform = new Set(initialFields); // Start with fields from data
        console.log("Initial available fields for transform:", Array.from(availableFieldsDuringTransform));

        
        // --- Rule Execution Loop --- 
        validRules.forEach((rule, ruleIndex) => { // Use validRules here
            if (rule.enabled === false) { // Skip disabled rules
                console.log(`Skipping disabled rule: ${rule.outputFieldName || rule.id}`);
                return;
            }
        
            console.log(`Transform: Applying rule ${ruleIndex + 1} (ID: ${rule.id}, Type: ${rule.type}, Output: ${rule.outputFieldName})`);
            const outputFieldName = rule.outputFieldName;
        
            // Check for output field name collision before processing rows
            if (availableFieldsDuringTransform.has(outputFieldName)) {
                const errorMsg = `Rule ${ruleIndex + 1} ('${outputFieldName}') skipped: Output field name already exists.`;
                console.error(errorMsg);
                ruleErrors.push(errorMsg);
                return; // Skip this rule
            }

            // Process each data row for the current rule
            workingData = workingData.map((item, itemIndex) => {
                if (!item) return item; // Skip if item is somehow null/undefined
                
                let newValue = null;
                try {
                    switch (rule.type) {
                        case 'arithmetic':
                            // Pass the *full set* of fields available *at this stage* to the handler
                            newValue = executeArithmeticRule(item, rule.parameters, availableFieldsDuringTransform);
                            break;
                        // Add cases for other rule types (ratio, normalize, etc.) here
                        default:
                            // This case should technically not be hit due to filtering validRules above
                            console.warn(`Unsupported rule type '${rule.type}' for rule ${rule.outputFieldName}. Skipping calculation for this row.`);
                            break;
                    }
                } catch (error) {
                    const rowErrorMsg = `Error in rule '${outputFieldName}' (Type: ${rule.type}) at row ${itemIndex + 1} (Ticker: ${item.ticker}): ${error.message}`;
                    // Only log row-level errors once per rule to avoid flooding
                    if (!ruleErrors.some(e => e.startsWith(`Error in rule '${outputFieldName}'`))) {
                        console.error(rowErrorMsg, error);
                        ruleErrors.push(rowErrorMsg); // Add specific error
                    } 
                    newValue = null; // Ensure error results in null
                }

                // Add the new field to the processed_data (or create processed_data if missing)
                if (!item.processed_data) {
                     item.processed_data = {};
                }
                // Store the calculated value
                item.processed_data[outputFieldName] = newValue;
                return item;
            });

            // Add the newly created field to the set of available fields for subsequent rules
            // Check if the calculation actually produced non-error results before adding
            // (We add it even if some rows failed, as long as the rule didn't skip entirely)
            availableFieldsDuringTransform.add(outputFieldName);
            console.log(`Rule ${ruleIndex + 1} applied. Available fields now:`, Array.from(availableFieldsDuringTransform));

        });
        // --- End Rule Execution Loop ---
        
        // --- Final Status Update ---
        if (ruleErrors.length > 0) {
            if(transformStatus) transformStatus.textContent = `Transformations applied with ${ruleErrors.length} error(s). Check console for details.`;
        } else {
            if(transformStatus) transformStatus.textContent = `Transformations applied successfully.`;
        }
        
        console.log("Transformation process completed.");
        return workingData;
    }

    // --- Rule Execution Handlers ---
    function executeArithmeticRule(item, params, availableFields) {
        if (!params || !params.formula) {
            throw new Error("Missing formula parameter for arithmetic rule.");
        }
        let formula = params.formula;

        // Find all field placeholders like {FieldName} or {Field Name with Spaces}
        const fieldPlaceholders = formula.match(/\{([^{}]+)\}/g) || [];
        const fieldNames = fieldPlaceholders.map(ph => ph.substring(1, ph.length - 1)); // Extract names

        const args = []; // Values to pass to the function
        const argNames = []; // Variable names inside the function
        let hasInvalidArgument = false; // Flag if any input is invalid

        fieldNames.forEach((fieldName, index) => {
            const cleanArgName = `arg${index}`; // Create safe variable names (arg0, arg1, ...)
            argNames.push(cleanArgName);

            // Replace placeholder in formula with the safe variable name
            // Use replaceAll for multiple occurrences of the same field
            const placeholderRegex = new RegExp(`\\{${fieldName.replace(/[-\/\\^$*+?.()|[\]]/g, '\\$&')}\\}`, 'g'); // Escape regex special chars in fieldName
            formula = formula.replace(placeholderRegex, cleanArgName);

            // Get the value from the item
            let value = null;
            // Check availableFields first - rule should only use fields known at this stage
            if (!availableFields.has(fieldName)) {
                 console.warn(`Field '{${fieldName}}' used in formula is not available at this stage for ticker ${item.ticker}. Using null.`);
                 value = null; 
            } else if (fieldName === 'ticker' && item.ticker !== undefined) { // Check existence
                 value = item.ticker; // Cannot do arithmetic on ticker, will likely cause NaN
            } else if (fieldName === 'source' && item.source !== undefined) {
                 value = item.source; // Cannot do arithmetic on source
            } else if (item.processed_data && item.processed_data.hasOwnProperty(fieldName)) {
                 value = item.processed_data[fieldName];
            } 
            // else value remains null

            // Convert value to number, handle null/undefined/non-numeric
            const numValue = Number(value);
            if (value === null || value === undefined || String(value).trim() === '' || isNaN(numValue)) {
                // If any field is non-numeric or missing, the result should be null
                console.debug(`Non-numeric or missing value for field '{${fieldName}}' (value: ${value}) in ticker ${item.ticker}. Result will be null.`);
                args.push(null); // Push null to indicate issue
                hasInvalidArgument = true; 
            } else {
                args.push(numValue);
            }
        });

        // Check if any argument became null due to non-numeric/missing source data
        if (hasInvalidArgument) {
            return null; // Abort calculation for this row if any input is invalid
        }

        // Use the Function constructor for safe execution
        try {
            // Ensure formula is not empty after substitutions
            if (formula.trim() === '') {
                throw new Error("Formula is empty after field substitution.");
            }
            const dynamicFunction = new Function(...argNames, `"use strict"; return (${formula});`);
            const result = dynamicFunction(...args);

            // Check for NaN or Infinity which indicate issues like division by zero
            if (result === null || result === undefined || !Number.isFinite(result)) {
                console.debug(`Arithmetic result is non-finite (NaN/Infinity) for ticker ${item.ticker}. Formula: ${params.formula}. Returning null.`);
                return null;
            }
            return result;
        } catch (e) {
            // Catch errors during formula execution (e.g., syntax errors, division by zero implicitly handled by isFinite)
            throw new Error(`Formula execution failed: ${e.message}. Formula: ${params.formula}`);
        }
    }

    // --- Functions for Rendering UI --- 
    function renderTransformationRules() {
        console.log("Transform: Rendering rules list...");
        if (!rulesContainer) return;
        
        rulesContainer.innerHTML = ''; // Clear existing rules
        
        if (transformationRules.length === 0) {
            rulesContainer.innerHTML = '<p class="text-muted small">No transformations defined yet. Click \"+ Add Rule\" to begin.</p>';
            return;
        }
        
        const ruleListGroup = document.createElement('div');
        ruleListGroup.className = 'list-group list-group-flush'; // Flush removes borders

        transformationRules.forEach((rule, index) => {
            const ruleItem = document.createElement('div');
            // Add validation state indication if rule type is unknown
            const isKnownType = ruleTypeDefinitions.hasOwnProperty(rule.type);
            ruleItem.className = `list-group-item list-group-item-action px-2 py-1 ${!isKnownType ? 'list-group-item-danger' : ''}`;
            ruleItem.dataset.ruleId = rule.id; // Store rule ID for reference
            if (!isKnownType) {
                ruleItem.title = `Error: Unknown rule type '${rule.type}'. This rule will be skipped.`;
            }

            const itemContent = document.createElement('div');
            itemContent.className = 'd-flex w-100 justify-content-between align-items-center';

            // Left side: Rule Info (Type, Name, Comment)
            const ruleInfoDiv = document.createElement('div');
            ruleInfoDiv.className = 'flex-grow-1 me-3'; // Take up space, margin right
            
            const ruleTitle = document.createElement('h6');
            ruleTitle.className = 'mb-0 small fw-bold';
            ruleTitle.textContent = `${rule.outputFieldName || '(Unnamed Rule)'} [${getRuleTypeName(rule.type)}]`; 
            ruleInfoDiv.appendChild(ruleTitle);

            if (rule.comment) {
                const ruleComment = document.createElement('small');
                ruleComment.className = 'd-block text-muted';
                ruleComment.textContent = rule.comment;
                ruleInfoDiv.appendChild(ruleComment);
            }

            itemContent.appendChild(ruleInfoDiv);

            // Right side: Controls (Enable, Move, Edit, Delete)
            const controlsDiv = document.createElement('div');
            controlsDiv.className = 'd-flex align-items-center flex-shrink-0'; // Don't shrink

            // Enable Toggle
            const enableSwitch = document.createElement('div');
            enableSwitch.className = 'form-check form-switch me-3'; // Margin for spacing
            const enableInput = document.createElement('input');
            enableInput.type = 'checkbox';
            enableInput.className = 'form-check-input';
            enableInput.role = 'switch';
            enableInput.checked = rule.enabled !== false; // Default to true if undefined
            enableInput.id = `enable-switch-${rule.id}`;
            ruleItem.style.opacity = enableInput.checked ? '1' : '0.6'; // Initial visual state
            enableInput.title = enableInput.checked ? 'Disable this rule' : 'Enable this rule';
            enableInput.addEventListener('change', (e) => {
                const ruleIdToToggle = rule.id;
                const isEnabled = e.target.checked;
                const ruleIndex = transformationRules.findIndex(r => r.id === ruleIdToToggle);

                if (ruleIndex > -1) {
                    console.log(`Toggling rule ${ruleIdToToggle} (index: ${ruleIndex}) to enabled: ${isEnabled}`);
                    transformationRules[ruleIndex].enabled = isEnabled;
                    // Update visual state of the specific list item
                    const listItem = rulesContainer.querySelector(`.list-group-item[data-rule-id="${ruleIdToToggle}"]`);
                    if (listItem) {
                         listItem.style.opacity = isEnabled ? '1' : '0.6';
                         enableInput.title = isEnabled ? 'Disable this rule' : 'Enable this rule';
                    }
                    saveTransformationRulesToStorage(); // Save changes
                } else {
                    console.error(`Could not find rule with ID ${ruleIdToToggle} to toggle.`);
                }
            });
            enableSwitch.appendChild(enableInput);
            controlsDiv.appendChild(enableSwitch);

            // Move Buttons
            const moveUpBtn = document.createElement('button');
            moveUpBtn.className = 'btn btn-sm btn-outline-secondary me-1 py-0 px-1';
            moveUpBtn.innerHTML = '<i class="bi bi-arrow-up"></i>';
            moveUpBtn.title = 'Move rule up';
            moveUpBtn.disabled = index === 0; // Disable if first item
            moveUpBtn.addEventListener('click', (e) => {
                e.stopPropagation(); // Prevent item click
                // TODO: Implement moveRule(rule.id, 'up');
                 console.log(`Move rule ${rule.id} up`);
                 // alert("Move logic not yet implemented.");
            });
            controlsDiv.appendChild(moveUpBtn);

            const moveDownBtn = document.createElement('button');
            moveDownBtn.className = 'btn btn-sm btn-outline-secondary me-2 py-0 px-1';
            moveDownBtn.innerHTML = '<i class="bi bi-arrow-down"></i>';
            moveDownBtn.title = 'Move rule down';
            moveDownBtn.disabled = index === transformationRules.length - 1; // Disable if last item
            moveDownBtn.addEventListener('click', (e) => {
                e.stopPropagation(); 
                // TODO: Implement moveRule(rule.id, 'down');
                console.log(`Move rule ${rule.id} down`);
                 // alert("Move logic not yet implemented.");
            });
            controlsDiv.appendChild(moveDownBtn);

            // Edit Button
            const editBtn = document.createElement('button');
            editBtn.className = 'btn btn-sm btn-outline-primary me-1 py-0 px-1';
            editBtn.innerHTML = '<i class="bi bi-pencil"></i>';
            editBtn.title = 'Edit rule';
            editBtn.addEventListener('click', (e) => {
                e.stopPropagation(); 
                // TODO: Implement editRule(rule.id);
                 console.log(`Edit rule ${rule.id}`);
                 // alert("Edit logic not yet implemented.");
            });
            controlsDiv.appendChild(editBtn);

            // Delete Button
            const deleteBtn = document.createElement('button');
            deleteBtn.className = 'btn btn-sm btn-outline-danger py-0 px-1';
            deleteBtn.innerHTML = '<i class="bi bi-trash"></i>';
            deleteBtn.title = 'Delete rule';
            deleteBtn.addEventListener('click', (e) => {
                e.stopPropagation(); 
                // TODO: Implement deleteRule(rule.id);
                 console.log(`Delete rule ${rule.id}`);
                 // alert("Delete logic not yet implemented.");
            });
            controlsDiv.appendChild(deleteBtn);

            itemContent.appendChild(controlsDiv);
            ruleItem.appendChild(itemContent);
            ruleListGroup.appendChild(ruleItem);
        });
        
        rulesContainer.appendChild(ruleListGroup);
    }
    
    function getRuleTypeName(typeKey) {
        // Now defined inside DOMContentLoaded, can access ruleTypeDefinitions
        return ruleTypeDefinitions[typeKey]?.name || typeKey || 'Unknown';
    }

    function renderTransformedDataPreview(dataToPreview) {
        console.log("Transform: Rendering transformed data preview...");
        if (!transformedDataOutput) return;
        
        // Use the passed data, or the global transformedData if none provided
        const data = dataToPreview || transformedData; 

        if (!data || data.length === 0) {
            transformedDataOutput.textContent = 'No transformed data to display. Apply transformations first.';
            return;
        }
        
        // Display first 10 rows as JSON string
        const previewData = data.slice(0, 10);
        try {
            // Use JSON.stringify with a replacer to handle potential BigInts if they ever occur
            transformedDataOutput.textContent = JSON.stringify(previewData, (key, value) =>
                typeof value === 'bigint' ? value.toString() : value, 
            2);
        } catch (e) {
             console.error("Error stringifying transformed data for preview:", e);
             transformedDataOutput.textContent = "Error displaying preview. Check console.";
        }
    }

    // --- Modal UI Functions ---
    function populateRuleTypeSelector() {
        if (!transformRuleTypeSelect) return;
        transformRuleTypeSelect.innerHTML = '<option value="" selected disabled>-- Select Type --</option>'; // Reset
        for (const typeKey in ruleTypeDefinitions) {
            const option = document.createElement('option');
            option.value = typeKey;
            option.textContent = ruleTypeDefinitions[typeKey].name;
            option.title = ruleTypeDefinitions[typeKey].description;
            transformRuleTypeSelect.appendChild(option);
        }
    }

    function renderRuleParametersUI(ruleType) {
        if (!transformParametersContainer || !ruleTypeDefinitions[ruleType]) {
            transformParametersContainer.innerHTML = '<p class="text-muted small text-center">Select a valid Rule Type to configure parameters.</p>';
            return;
        }

        transformParametersContainer.innerHTML = ''; // Clear existing
        const definition = ruleTypeDefinitions[ruleType];

        definition.parameters.forEach(param => {
            const formGroup = document.createElement('div');
            formGroup.className = 'mb-3';

            const label = document.createElement('label');
            label.htmlFor = `param-${param.id}`;
            label.className = 'form-label form-label-sm';
            label.textContent = param.label + (param.required ? ' *' : '');
            formGroup.appendChild(label);

            let inputElement;
            if (param.type === 'textarea') {
                inputElement = document.createElement('textarea');
                inputElement.rows = 3;
            } else if (param.type === 'select') { // Placeholder for future field selector
                 inputElement = document.createElement('select');
                 // TODO: Populate options (e.g., available fields)
            } else { // Default to text input
                inputElement = document.createElement('input');
                inputElement.type = param.type || 'text';
            }

            inputElement.id = `param-${param.id}`;
            inputElement.className = 'form-control form-control-sm';
            if (param.placeholder) inputElement.placeholder = param.placeholder;
            if (param.required) inputElement.required = true;
            // TODO: Set value if editing an existing rule

            formGroup.appendChild(inputElement);
            transformParametersContainer.appendChild(formGroup);
        });
    }

    function resetRuleModal() {
        if (!ruleModal) return;
        transformRuleIdInput.value = ''; // Clear editing ID
        transformRuleTypeSelect.value = ''; // Reset type selection
        transformOutputNameInput.value = '';
        transformRuleCommentInput.value = '';
        transformParametersContainer.innerHTML = '<p class="text-muted small text-center">Select a Rule Type to configure parameters.</p>'; // Clear params
        transformModalStatus.textContent = ''; // Clear errors
        // Clear validation states if needed
        transformOutputNameInput.classList.remove('is-invalid');
        outputNameFeedback.textContent = '';
        console.log("Rule modal reset for adding new rule.");
    }

    // --- Event Listeners --- 
    if (addRuleButton) {
        addRuleButton.addEventListener('click', () => {
            console.log("Add Rule button clicked - resetting and opening modal.");
            resetRuleModal();
            // Modal is opened via data-bs-toggle attribute in HTML
        });
    }

    if (applyTransformationsButton) {
        applyTransformationsButton.addEventListener('click', () => {
            console.log("Apply Transformations button clicked.");
            // Trigger the main analytics module to run the transformation process
            if (window.AnalyticsMainModule && typeof window.AnalyticsMainModule.runTransformations === 'function') {
                 window.AnalyticsMainModule.runTransformations();
            } else {
                console.error("AnalyticsMainModule or runTransformations function not found.");
                alert("Error: Cannot trigger transformation process. Main module not available.");
                 if(transformStatus) transformStatus.textContent = "Error: Main analytics module not found.";
            }
        });
    }

    if (saveRulesButton) {
        saveRulesButton.addEventListener('click', () => {
            console.log("Save Rules button clicked.");
            saveTransformationRulesToStorage();
        });
    }

    if (loadRulesButton) {
        loadRulesButton.addEventListener('click', () => {
            console.log("Load Rules button clicked.");
            loadTransformationRulesFromStorage();
            renderTransformationRules(); // Re-render the list after loading
             if(transformStatus) transformStatus.textContent = "Rules loaded from local storage.";
        });
    }
    
    if (transformRuleTypeSelect) {
         transformRuleTypeSelect.addEventListener('change', (e) => {
             console.log(`Rule type changed to: ${e.target.value}`);
             renderRuleParametersUI(e.target.value);
         });
    }

    if (saveRuleButton) { // Listener for the button INSIDE the modal
        saveRuleButton.addEventListener('click', () => {
            console.log("Save Rule button (inside modal) clicked.");
            
            // --- Basic Save Logic (Add New Only, minimal validation) ---
            const ruleType = transformRuleTypeSelect.value;
            const outputName = transformOutputNameInput.value.trim();
            const comment = transformRuleCommentInput.value.trim();
            const ruleId = transformRuleIdInput.value; // Check if editing

            // Simple Validation
            let isValid = true;
            transformOutputNameInput.classList.remove('is-invalid');
            outputNameFeedback.textContent = '';
            transformModalStatus.textContent = '';
            if (!ruleType) {
                // Should not happen if dropdown is used correctly
                transformModalStatus.textContent = 'Please select a rule type.';
                isValid = false;
            }
            if (!outputName) {
                transformOutputNameInput.classList.add('is-invalid');
                outputNameFeedback.textContent = 'Output field name is required.';
                isValid = false;
            }
            // TODO: Add validation for output name uniqueness
            // TODO: Add validation for parameters (e.g., formula required)
            
            if (!isValid) {
                console.warn("Save aborted due to validation errors.");
                return;
            }
            
            // Read parameters (Example for arithmetic)
            const parameters = {};
            if (ruleType === 'arithmetic') {
                const formulaInput = document.getElementById('param-formula');
                if (formulaInput) {
                    parameters.formula = formulaInput.value.trim();
                    // TODO: Add validation for non-empty formula if required
                } else {
                    console.error("Could not find formula input for arithmetic rule.");
                    transformModalStatus.textContent = 'Internal error: Formula input missing.';
                    return; // Stop saving
                }
            }
            // TODO: Add parameter reading for other rule types
            
            if (ruleId) {
                // --- Update Existing Rule (TODO) ---
                console.log(`TODO: Update rule with ID: ${ruleId}`);
                alert("Updating existing rules not yet implemented.");
            } else {
                // --- Add New Rule ---
                const newRule = {
                    id: `rule-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`, // Simple unique ID
                    type: ruleType,
                    outputFieldName: outputName,
                    comment: comment,
                    enabled: true, // Default to enabled
                    parameters: parameters
                };
                console.log("Adding new rule:", newRule);
                transformationRules.push(newRule);
            }

            // Close modal
            if (bsRuleModal) bsRuleModal.hide();

            // Re-render the rule list
            renderTransformationRules();

            // Optionally save immediately to storage?
             saveTransformationRulesToStorage(); // Let's save after each add/edit for now
        });
    }

    // --- Initialization --- 
    console.log("Initializing Transformation module...");
    loadTransformationRulesFromStorage();
    populateRuleTypeSelector(); // Populate modal dropdown
    renderTransformationRules();
    renderTransformedDataPreview(); // Render empty preview initially

    function getRuleTypeName(typeKey) {
        // Now defined inside DOMContentLoaded, can access ruleTypeDefinitions
        return ruleTypeDefinitions[typeKey]?.name || typeKey || 'Unknown';
    }
    
    // --- Expose Module API --- 
    window.AnalyticsTransformModule = {
        applyTransformations: applyTransformations,
        getTransformationRules: () => transformationRules, // Return current rules
        renderTransformedDataPreview: renderTransformedDataPreview // Allow external update of preview
    };
    console.log("AnalyticsTransformModule initialized and exposed.");

}); 