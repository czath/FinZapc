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
        'text_manipulation': {
            name: 'Text Manipulation',
            description: 'Create a new field using JavaScript string operations and functions on existing fields.',
            parameters: [
                {
                    id: 'expression',
                    name: 'Expression',
                    type: 'textarea',
                    placeholder: 'Example: {Sector} + \'-\' + {Industry}\nExample: parseFloat({field_as_string})\nExample: {Name}.substring(0, 5).toLowerCase()',
                    required: true
                }
            ]
        },
        // Add other types like 'normalization', etc. later
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

        // --- Discover ALL potential numeric fields from the entire dataset --- FIX
        console.log("Scanning all data to discover potential fields for aggregation...");
        const allPotentialFields = new Set();
        workingData.forEach(item => {
            if (!item) return;
            // Check top-level keys (excluding processed_data itself)
            Object.keys(item).forEach(key => {
                if (key !== 'processed_data') {
                    allPotentialFields.add(key);
                }
            });
            // Check keys within processed_data if it exists
            if (item.processed_data) {
                 Object.keys(item.processed_data).forEach(key => {
                     allPotentialFields.add(key);
                 });
            }
         });
         console.log("Discovered potential fields:", Array.from(allPotentialFields));

         // --- Determine which of these fields are actually numeric (for aggregation) --- FIX
         // (This requires iterating data again, but ensures we only aggregate numeric fields)
         const numericFieldsForAggregation = new Set();
         const tempFieldMetadata = {}; // To store type info during scan

         allPotentialFields.forEach(field => {
             let numericCount = 0;
             let valueFound = false;
             for (const item of workingData) { // Use for...of for potential early exit
                 if (!item) continue;
                 let value = null;
                 // Check both top-level and processed_data for the value
                 if (item.hasOwnProperty(field) && field !== 'processed_data') {
                     value = item[field];
                 } else if (item.processed_data && item.processed_data.hasOwnProperty(field)) {
                     value = item.processed_data[field];
                 }
                 
                 if (value !== null && value !== undefined && String(value).trim() !== '') {
                     valueFound = true;
                     const numValue = Number(value);
                     if (!isNaN(numValue)) {
                         numericCount++;
                     } else {
                        // If we find a non-numeric value, we can potentially stop checking this field early
                        // depending on the heuristic. For now, let's check all.
                     }
                 } 
             }
             // Heuristic: Consider field numeric if at least one value was found 
             // AND the majority (e.g., >50% or >80%) of found values were numeric?
             // Let's use a simple heuristic for now: if any numeric value is found, consider it.
             // More robust: check numericCount / foundCount > threshold.
             if (numericCount > 0) { 
                 numericFieldsForAggregation.add(field);
             }
             // Store type for potential later use (optional)
             tempFieldMetadata[field] = (numericCount > 0) ? 'numeric' : (valueFound ? 'text' : 'empty');
         });
         console.log("Identified numeric fields for aggregation:", Array.from(numericFieldsForAggregation));
         
         // --- Optimization Pre-scan: Identify Needed Aggregates from Rules --- ADDED
         console.log("Pre-scanning rules to identify needed aggregates...");
         const neededAggregates = {}; // { fieldName: Set<string>('min', 'max', 'avg', 'sum', 'median', 'values') }
         // Reuse the aggregate regex from executeArithmeticRule
         const aggregateScanRegex = /(?:(MIN|MAX|AVG|SUM|MEDIAN)\s*\(\s*'?\{([^{}]+)\}?'?\s*\))|(?:(TRIM_AVG)\s*\(\s*'?\{([^{}]+)\}?'?\s*,\s*(\d+(?:\.\d+)?%?)\s*\))|(?:(AVG_AFTER_TRIM_MIN|AVG_AFTER_TRIM_MAX)\s*\(\s*'?\{([^{}]+)\}?'?\s*,\s*(\d+)\s*\))/gi;

         rules.forEach(rule => {
            if (!rule.enabled || rule.type !== 'arithmetic' || !rule.parameters?.formula) {
                return; // Skip disabled or non-arithmetic rules
            }
            const formula = rule.parameters.formula;
            let match;
            // Reset regex lastIndex before each test/exec loop
            aggregateScanRegex.lastIndex = 0; 
            while ((match = aggregateScanRegex.exec(formula)) !== null) {
                const functionName = (match[1] || match[3] || match[6])?.toLowerCase();
                const fieldName = match[2] || match[4] || match[7];

                if (fieldName && functionName) {
                     if (!neededAggregates[fieldName]) {
                         neededAggregates[fieldName] = new Set();
                     }
                     
                     // Determine the required aggregate type
                     let requiredType;
                     switch(functionName) { // Use functionKey from outer scope? No, needs to be from functionName
                          case 'min': requiredType = 'min'; break;
                          case 'max': requiredType = 'max'; break;
                          case 'avg': requiredType = 'avg'; break;
                          case 'sum': requiredType = 'sum'; break;
                          case 'median': requiredType = 'median'; break;
                          // For any trimmed function, we need the raw values array
                          case 'trim_avg':
                          case 'avg_after_trim_min':
                          case 'avg_after_trim_max':
                              requiredType = 'values'; 
                              break;
                          default:
                              console.warn(`[Pre-scan] Unknown aggregate function found: ${functionName}`);
                              continue; // Skip unknown function
                     }
                     
                     // If a dynamic trim needs values, ensure standard ones are also calculated if needed elsewhere
                     // For now, we are calculating all anyway, so just add the type.
                     neededAggregates[fieldName].add(requiredType);
                     
                     // If avg or sum is needed, implicitly need count/sum for avg
                     // But we calculate all for now.
                }
            }
         });
         console.log("Needed aggregates identified by pre-scan:", neededAggregates);
         // --- End Optimization Pre-scan ---

         // --- Calculate Aggregates --- OPTIMIZED
         console.log("Calculating *ONLY* needed aggregates for identified numeric fields..."); 
        const aggregateResults = {};
         
         // Iterate only over fields identified as numeric
         numericFieldsForAggregation.forEach(field => { 
             const neededTypes = neededAggregates[field]; // Get the Set of needed types for this field
             
             // Skip field if pre-scan found no rules using its aggregates (shouldn't happen often with current setup, but good practice)
             // OR if it wasn't identified as numeric (already filtered by loop, but belt-and-suspenders)
             if (!neededTypes || neededTypes.size === 0) { 
                  // console.log(`Skipping aggregate calculation for field ${field} - not needed by any rule.`);
                  return; 
             }

              // Initialize variables potentially needed
              let min = Infinity, max = -Infinity, sum = 0, count = 0, numericFound = false;
              // Only collect raw values if needed for median or dynamic trimmed calculations
              const collectValues = neededTypes.has('median') || neededTypes.has('values');
              const fieldValues = collectValues ? [] : null; // Store as null if not needed
              const calculateMinMax = neededTypes.has('min') || neededTypes.has('max');
              const calculateSumCount = neededTypes.has('avg') || neededTypes.has('sum');

             workingData.forEach(item => {
                  if (!item) return; 
                  let value = null;
                  // Check both top-level and processed_data for the value
                  if (item.hasOwnProperty(field) && field !== 'processed_data') {
                      value = item[field];
                  } else if (item.processed_data && item.processed_data.hasOwnProperty(field)) {
                      value = item.processed_data[field];
                  }
                  
                  // Process if a value was found and is numeric
                  if (value !== null && value !== undefined) { 
                     const numValue = Number(value);
                      if (String(value).trim() !== '' && !isNaN(numValue)) {
                         numericFound = true;
                          // --- Perform calculations ONLY if needed --- 
                          if (calculateMinMax) {
                         if (numValue < min) min = numValue;
                         if (numValue > max) max = numValue;
                     }
                          if (calculateSumCount) {
                              sum += numValue;
                              count++;
                          }
                          if (collectValues) {
                              fieldValues.push(numValue); 
                          }
                          // --- End conditional calculations --- 
                      }
                   } // Skip logging for missing/invalid values unless specifically debugging
              }); // End data loop for a field

              // --- Store ONLY calculated/needed aggregates --- 
              const fieldAggregates = {}; // Object to store results for this field
             if (numericFound) {
                  if (neededTypes.has('min')) fieldAggregates.min = min;
                  if (neededTypes.has('max')) fieldAggregates.max = max;
                  if (neededTypes.has('sum')) fieldAggregates.sum = sum;
                  if (neededTypes.has('avg')) fieldAggregates.avg = (count > 0) ? sum / count : null;
                  if (neededTypes.has('median')) fieldAggregates.median = calculateMedian(fieldValues); 
                  if (neededTypes.has('values')) fieldAggregates.values = fieldValues; 
             } else {
                  // Store default nulls/empty array only for the types that were needed
                  if (neededTypes.has('min')) fieldAggregates.min = null;
                  if (neededTypes.has('max')) fieldAggregates.max = null;
                  if (neededTypes.has('sum')) fieldAggregates.sum = 0;
                  if (neededTypes.has('avg')) fieldAggregates.avg = null;
                  if (neededTypes.has('median')) fieldAggregates.median = null; 
                  if (neededTypes.has('values')) fieldAggregates.values = [];
              }
              
              // Add the calculated aggregates for this field to the main results object
              aggregateResults[field] = fieldAggregates;
              // --- End storing aggregates ---

         }); // End field loop

         console.log("Final Calculated Aggregates Object (Optimized):", aggregateResults); // MODIFIED Log
        // --- END Aggregate Calculation ---
        
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
                            newValue = executeArithmeticRule(item, rule.parameters, availableFieldsDuringTransform, aggregateResults);
                            break;
                        case 'text_manipulation':
                            newValue = executeTextManipulationRule(item, rule.parameters, availableFieldsDuringTransform);
                            break;
                        // case 'ratio': ... removed ...
                        // case 'weighted_sum': ... removed ...
                        // Add cases for other rule types (normalize, etc.) here
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

    // --- NEW: Function to Delete a Rule ---
    function deleteRule(ruleId) {
        console.log(`Attempting to delete rule with ID: ${ruleId}`);
        const initialLength = transformationRules.length;
        transformationRules = transformationRules.filter(rule => rule.id !== ruleId);

        if (transformationRules.length < initialLength) {
            console.log(`Rule ${ruleId} deleted successfully.`);
            saveTransformationRulesToStorage();
            renderTransformationRules(); // Re-render the list to reflect the deletion
            if(transformStatus) transformStatus.textContent = "Rule deleted.";
        } else {
            console.warn(`Rule with ID ${ruleId} not found for deletion.`);
             if(transformStatus) transformStatus.textContent = "Could not find rule to delete.";
        }
    }
    // --- END NEW Function --- 

    // --- NEW: Function to Trigger Editing a Rule ---
    function editRule(ruleId) {
        console.log(`Attempting to edit rule with ID: ${ruleId}`);
        const ruleToEdit = transformationRules.find(rule => rule.id === ruleId);
        if (ruleToEdit) {
            openTransformationRuleModal(ruleToEdit); // Call the existing modal function with the rule data
        } else {
            console.error(`Rule with ID ${ruleId} not found for editing.`);
             if(transformStatus) transformStatus.textContent = "Could not find rule to edit.";
        }
    }
    // --- END NEW Function ---

    // --- NEW: Function to Move a Rule Up or Down ---
    function moveRule(ruleId, direction) {
        const index = transformationRules.findIndex(rule => rule.id === ruleId);
        if (index === -1) {
            console.error(`Rule with ID ${ruleId} not found for moving.`);
            if(transformStatus) transformStatus.textContent = "Could not find rule to move.";
            return;
        }

        let newIndex = direction === 'up' ? index - 1 : index + 1;

        // Check bounds
        if (newIndex < 0 || newIndex >= transformationRules.length) {
            console.log(`Rule ${ruleId} is already at the ${direction === 'up' ? 'top' : 'bottom'}.`);
            return; // Cannot move further
        }

        console.log(`Moving rule ${ruleId} from index ${index} to ${newIndex}.`);

        // Perform the move
        const [ruleToMove] = transformationRules.splice(index, 1); // Remove the rule
        transformationRules.splice(newIndex, 0, ruleToMove); // Insert it at the new position

        // Save and re-render
        saveTransformationRulesToStorage();
        renderTransformationRules();
        if(transformStatus) transformStatus.textContent = "Rule order changed.";
    }
    // --- END NEW Function ---

    // --- NEW: Helper function to calculate Median ---
    function calculateMedian(numericValues) {
        if (!numericValues || numericValues.length === 0) {
            return null;
        }
        // Sort numbers numerically
        const sortedValues = [...numericValues].sort((a, b) => a - b);
        const mid = Math.floor(sortedValues.length / 2);

        if (sortedValues.length % 2 === 0) {
            // Even number of values: average the two middle ones
            return (sortedValues[mid - 1] + sortedValues[mid]) / 2;
        } else {
            // Odd number of values: return the middle one
            return sortedValues[mid];
        }
    }
    // --- END NEW Helper ---

    // --- NEW: Helper function to calculate Trimmed Average ---
    function calculateTrimmedAverage(numericValues, trimPercent) {
        if (!numericValues || numericValues.length === 0 || trimPercent === undefined || trimPercent === null || trimPercent < 0 || trimPercent >= 0.5) {
            console.warn(`[calculateTrimmedAverage] Invalid input: numericValues empty or trimPercent invalid/missing (${trimPercent}).`);
            return null;
        }
        // Sort numbers numerically
        const sortedValues = [...numericValues].sort((a, b) => a - b);
        const count = sortedValues.length;
        const removeCount = Math.floor(count * trimPercent); // Items to remove from EACH end

        if (removeCount * 2 >= count) {
            // Trimmed everything or more, return null
            return null;
        }

        // Slice the array to remove elements from both ends
        const trimmedValues = sortedValues.slice(removeCount, count - removeCount);

        // Calculate sum of remaining values
        const sum = trimmedValues.reduce((acc, val) => acc + val, 0);
        
        // Calculate average
        return sum / trimmedValues.length;
    }
    // --- END NEW Helper ---

    // --- NEW: Helper function to calculate Average after trimming MIN N values ---
    function calculateAvgAfterTrimMin(numericValues, removeCount) {
        const count = numericValues?.length || 0;
        // Validate input: need values, removeCount must be non-negative integer, cannot remove all values
        if (count === 0 || !Number.isInteger(removeCount) || removeCount < 0 || removeCount >= count) {
            console.warn(`[calculateAvgAfterTrimMin] Invalid input: count=${count}, removeCount=${removeCount}.`);
            return null;
        }
        if (removeCount === 0) { // Optimization: no trimming, just calculate standard average
            const sum = numericValues.reduce((acc, val) => acc + val, 0);
            return sum / count;
        }
        // Sort numerically
        const sortedValues = [...numericValues].sort((a, b) => a - b);
        // Slice to keep values *after* removing the bottom 'removeCount'
        const keptValues = sortedValues.slice(removeCount);
        // Calculate sum of remaining values
        const sum = keptValues.reduce((acc, val) => acc + val, 0);
        // Return average of kept values
        return sum / keptValues.length;
    }
    // --- END NEW Helper ---

    // --- NEW: Helper function to calculate Average after trimming MAX N values ---
    function calculateAvgAfterTrimMax(numericValues, removeCount) {
        const count = numericValues?.length || 0;
        // Validate input: need values, removeCount must be non-negative integer, cannot remove all values
        if (count === 0 || !Number.isInteger(removeCount) || removeCount < 0 || removeCount >= count) {
            console.warn(`[calculateAvgAfterTrimMax] Invalid input: count=${count}, removeCount=${removeCount}.`);
            return null;
        }
        if (removeCount === 0) { // Optimization: no trimming, just calculate standard average
            const sum = numericValues.reduce((acc, val) => acc + val, 0);
            return sum / count;
        }
        // Sort numerically
        const sortedValues = [...numericValues].sort((a, b) => a - b);
        // Slice to keep values *before* removing the top 'removeCount'
        const keptValues = sortedValues.slice(0, count - removeCount);
        // Calculate sum of remaining values
        const sum = keptValues.reduce((acc, val) => acc + val, 0);
        // Return average of kept values
        return sum / keptValues.length;
    }
    // --- END NEW Helper ---

    // --- NEW: Helper function to extract Nth substring between delimiters ---
    function extractDelimitedSubstring(text, delimiterPair, index) {
        console.log(`[Helper] extractDelimitedSubstring called with: text='${text}', delimiter='${delimiterPair}', index=${index}`); // Log Inputs
        if (typeof text !== 'string' || typeof delimiterPair !== 'string' || delimiterPair.length !== 2 || !Number.isInteger(index) || index <= 0) {
            console.warn(`[extractDelimitedSubstring] Invalid input: text='${text}', delimiter='${delimiterPair}', index=${index}`);
            return null;
        }

        // --- ADDED: Remove surrounding quotes if present --- 
        let processedText = text.trim(); // Trim whitespace first
        if (processedText.startsWith('"') && processedText.endsWith('"')) {
             processedText = processedText.substring(1, processedText.length - 1);
             console.log(`[extractDelimitedSubstring] Removed surrounding quotes. Text is now: '${processedText}'`); 
        }
        console.log(`[Helper] Text after quote processing: '${processedText}'`); // Log Processed Text
        // --- END ADDED ---

        const startDelim = delimiterPair[0];
        const endDelim = delimiterPair[1];
        
        // Escape delimiters for regex
        const escapedStart = startDelim.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&');
        const escapedEnd = endDelim.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&');

        // Regex to find content between delimiters, non-greedy
        const regex = new RegExp(`${escapedStart}(.*?)${escapedEnd}`, 'g');
        console.log(`[Helper] Constructed Regex: ${regex}`); // Log Regex
        
        let match;
        let count = 0;
        let result = null;

        // Use the processedText for matching
        while ((match = regex.exec(processedText)) !== null) { 
            console.log(`[Helper] Regex match found: Full='${match[0]}', Group1='${match[1]}'`); // Log Match
            count++;
            if (count === index) {
                // Found the Nth match
                result = match[1]; // Group 1 captures the content between delimiters
                console.log(`[Helper] Found ${index}th match: '${result}'`); // Log Found Result
                break; // Stop searching
            }
        }

        if (result !== null) {
             // Trim whitespace and common separators (like comma) from the result
             const finalResult = result.trim().replace(/^,|,$/g, '').trim();
             console.log(`[Helper] Returning final result: '${finalResult}'`); // Log Return Value
             return finalResult; 
        } else {
             // Nth match not found
             console.log(`[Helper] ${index}th match not found. Returning null.`); // Log Not Found
             return null;
        }
    }
    // --- END NEW Helper ---

    // --- Make helper globally accessible for dynamic function execution --- ADDED
    window.extractDelimitedSubstring = extractDelimitedSubstring;

    // --- Rule Execution Handlers ---
    function executeArithmeticRule(item, params, availableFields, aggregateResults = {}) {
        if (!params || !params.formula) {
            throw new Error("Missing formula parameter for arithmetic rule.");
        }
        let formula = params.formula;

        // --- Aggregate Substitution (MIN, MAX, AVG, SUM, MEDIAN, TRIM_AVG, AVG_AFTER_TRIM_MIN, AVG_AFTER_TRIM_MAX) ---
        // Regex to find Aggregate({Field}) or Aggregate({Field}, Param) case-insensitively
        const aggregateRegex = /(?:(MIN|MAX|AVG|SUM|MEDIAN)\s*\(\s*'?\{([^{}]+)\}?'?\s*\))|(?:(TRIM_AVG)\s*\(\s*'?\{([^{}]+)\}?'?\s*,\s*(\d+(?:\.\d+)?%?)\s*\))|(?:(AVG_AFTER_TRIM_MIN|AVG_AFTER_TRIM_MAX)\s*\(\s*'?\{([^{}]+)\}?'?\s*,\s*(\d+)\s*\))/gi;
        
        // Check if there are any aggregate calls to substitute
        // Use test() first for efficiency before doing a full replace
        if (aggregateRegex.test(formula)) { 
            console.log(`[Arithmetic Rule] Substituting aggregates in formula "${params.formula}"...`);
            
            // Use replace with a replacer function for robustness
            // Adjusted capture groups: 
            // 1=stdFunc, 2=stdField | 3=trimAvgFunc, 4=trimAvgField, 5=trimAvgParam | 6=avgTrimCountFunc, 7=avgTrimCountField, 8=avgTrimCountParam
            formula = formula.replace(aggregateRegex, (fullMatch, stdFunc, stdField, trimAvgFunc, trimAvgField, trimAvgParam, avgTrimCountFunc, avgTrimCountField, avgTrimCountParam) => {
                const functionName = stdFunc || trimAvgFunc || avgTrimCountFunc;
                const fieldName = stdField || trimAvgField || avgTrimCountField;
                const param = trimAvgParam || avgTrimCountParam; // General parameter (string)

                let replacementValue = null;
                let functionKey = functionName.toLowerCase();

                try { // Wrap calculation in try/catch for safety within replacer
                    if (functionKey === 'trim_avg') {
                        let customTrimPercent = null;
                        // Parameter (trimParam) is required by regex for this branch
                        try {
                            if (param && param.endsWith('%')) {
                                customTrimPercent = parseFloat(param.slice(0, -1)) / 100.0;
                            } else {
                                customTrimPercent = parseFloat(param);
                            }
                            if (isNaN(customTrimPercent) || customTrimPercent < 0 || customTrimPercent >= 0.5) {
                                console.warn(`[Transform Rule] Invalid trim percentage '${param}' provided for ${fieldName} in ${fullMatch}. Substitution will be null.`);
                                customTrimPercent = null;
                            }
                        } catch (e) {
                            console.warn(`[Transform Rule] Error parsing trim percentage '${param}' for ${fieldName} in ${fullMatch}. Substitution will be null. Error: ${e}`);
                            customTrimPercent = null;
                        }

                        if (customTrimPercent !== null) {
                            const rawValues = aggregateResults[fieldName]?.values;
                            if (rawValues) {
                                replacementValue = calculateTrimmedAverage(rawValues, customTrimPercent);
                                console.log(`  - Calculated TRIM_AVG for ${fullMatch}: ${replacementValue}`); 
                            } else {
                                console.warn(`  - Cannot calculate Trimmed Average for ${fieldName}: Raw values not found.`);
                                replacementValue = null;
                            }
                        } else {
                            replacementValue = null; // Invalid trim percent means null result
                        }
                    } else if (functionKey === 'avg_after_trim_min' || functionKey === 'avg_after_trim_max') {
                        // Parameter is required by the regex (param = avgTrimCountParam)
                        let removeCount = null;
                        try {
                            removeCount = parseInt(param, 10); // Parse as integer
                            if (!Number.isInteger(removeCount) || removeCount < 0) {
                                console.warn(`[Transform Rule] Invalid remove count '${param}' provided for ${fieldName} in ${fullMatch}. Must be non-negative integer. Substitution will be null.`);
                                removeCount = null; // Mark as invalid
                            }
                        } catch (e) {
                            console.warn(`[Transform Rule] Error parsing remove count '${param}' for ${fieldName} in ${fullMatch}. Substitution will be null. Error: ${e}`);
                            removeCount = null; // Mark as invalid on error
                        }

                        if (removeCount !== null) {
                            const rawValues = aggregateResults[fieldName]?.values;
                            if (rawValues) {
                                if (functionKey === 'avg_after_trim_min') {
                                    console.log(`  - Calculating AVG_AFTER_TRIM_MIN for ${fieldName} with removeCount=${removeCount}`);
                                    replacementValue = calculateAvgAfterTrimMin(rawValues, removeCount);
                                } else { // functionKey === 'avg_after_trim_max'
                                    console.log(`  - Calculating AVG_AFTER_TRIM_MAX for ${fieldName} with removeCount=${removeCount}`);
                                    replacementValue = calculateAvgAfterTrimMax(rawValues, removeCount);
                                }
                                console.log(`  - Result from ${functionKey.toUpperCase()}: ${replacementValue}`);
                            } else {
                                console.warn(`  - Cannot calculate ${functionKey.toUpperCase()} for ${fieldName}: Raw values not found.`);
                                replacementValue = null;
                            }
                        } else {
                            replacementValue = null; // Invalid remove count means null result
                        }
                    } else {
                        // Handle other standard aggregate functions (MIN, MAX, AVG, SUM, MEDIAN)
                        if (functionKey === 'median') { functionKey = 'median'; }
                        // min, max, avg, sum are already correct
                        if (aggregateResults && aggregateResults[fieldName] && aggregateResults[fieldName].hasOwnProperty(functionKey) && aggregateResults[fieldName][functionKey] !== undefined) {
                    replacementValue = aggregateResults[fieldName][functionKey];
                             console.log(`  - Found value for ${fullMatch}: ${replacementValue}`);
                } else {
                            replacementValue = null;
                            console.warn(`  - Standard aggregate value for ${functionName}('{${fieldName}}') not found or invalid in aggregateResults (key: ${functionKey}).`);
                        }
                    }
                } catch(calcError) {
                     console.error(`  - Error during replacement calculation for ${fullMatch}: ${calcError}`);
                     replacementValue = null; // Ensure null on error
                }
                
                // Return the string for replacement (or 'null')
                const replacementString = replacementValue === null ? 'null' : String(replacementValue);
                 console.log(`  - Replacing ${fullMatch} with ${replacementString}`);
                return replacementString; 
            });

            console.log(`[Arithmetic Rule] Formula after aggregate substitution: "${formula}"`);
        } else {
             console.log(`[Arithmetic Rule] No aggregate functions found in formula "${params.formula}".`);
        }
        // --- End Aggregate Substitution ---

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

    // --- NEW: Execution Handler for Text Manipulation Rule ---
    function executeTextManipulationRule(item, params, availableFields) {
        if (!params || !params.expression) {
            throw new Error("Missing expression parameter for text manipulation rule.");
        }
        let expression = params.expression;

        // Find all field placeholders like {FieldName}
        const fieldPlaceholders = expression.match(/\{([^{}]+)\}/g) || [];
        const fieldNames = fieldPlaceholders.map(ph => ph.substring(1, ph.length - 1));

        const args = []; // Values to pass to the function
        const argNames = []; // Variable names inside the function

        fieldNames.forEach((fieldName, index) => {
            const cleanArgName = `arg${index}`; 
            argNames.push(cleanArgName);

            // Replace placeholder in expression with the safe variable name
            const placeholderRegex = new RegExp(`\\{${fieldName.replace(/[-\/\\^$*+?.()|[\]]/g, '\\$&')}\\}`, 'g');
            expression = expression.replace(placeholderRegex, cleanArgName);

            // Get the value from the item (check top-level and processed_data)
            let value = null;
            if (availableFields.has(fieldName)) { // Check if field should be available
                if (item.hasOwnProperty(fieldName) && fieldName !== 'processed_data') {
                    value = item[fieldName];
                } else if (item.processed_data && item.processed_data.hasOwnProperty(fieldName)) {
                    value = item.processed_data[fieldName];
                }
            } else {
                 console.warn(`Field '{${fieldName}}' used in expression is not available at this stage for ticker ${item.ticker}. Using null.`);
                 // Value remains null
            }
            
            // Convert null/undefined to empty string for basic safety in string ops,
            // but pass raw value otherwise to allow type checking/conversion in expression
            const argValue = (value === null || value === undefined) ? '' : value; 
            args.push(argValue);
        });

        // --- Translate custom function calls to JS method calls --- ADDED
        let translatedExpression = expression;
        try {
             // UPPERCASE(argN) -> (argN).toUpperCase()
            translatedExpression = translatedExpression.replace(/UPPERCASE\s*\(([^)]+)\)/gi, '($1).toUpperCase()');
             // LOWERCASE(argN) -> (argN).toLowerCase()
            translatedExpression = translatedExpression.replace(/LOWERCASE\s*\(([^)]+)\)/gi, '($1).toLowerCase()');
            // TRIM(argN) -> (argN).trim()
            translatedExpression = translatedExpression.replace(/TRIM\s*\(([^)]+)\)/gi, '($1).trim()');
            // SUBSTRING(argN, start, end) -> (argN).substring(start, end)
            // translatedExpression = translatedExpression.replace(/SUBSTRING\s*\(([^,]+)\s*,([^,]+)\s*,([^)]+)\)/gi, '($1).substring($2, $3)'); // REMOVED OLD SUBSTRING
            
            // EXTRACT_BY_DELIMITER(argN, delimPairStr, indexStr) -> extractDelimitedSubstring(argN, delimPairStr, indexInt)
            // This requires careful parsing of arguments within the translation step
            translatedExpression = translatedExpression.replace(
                /EXTRACT_BY_DELIMITER\s*\(([^,]+)\s*,\s*("[^"\\]*(?:\\.[^"\\]*)*"|\'[^\'\\]*(?:\\.[^\'\\]*)*\')\s*,\s*(\d+)\s*\)/gi,
                (match, argName, delimStr, indexStr) => {
                    try {
                        console.log(`[Translation] Matched EXTRACT_BY_DELIMITER. Arg: ${argName}, Delim: ${delimStr}, Index: ${indexStr}`); // Log Inputs
                        // Basic validation/parsing
                        const index = parseInt(indexStr, 10);
                        // Delimiter string includes quotes, remove them for the function call
                        const delimiterPair = delimStr.slice(1, -1);
                        if (!isNaN(index) && index > 0 && delimiterPair.length === 2) {
                            // Construct the function call string
                            const callString = `extractDelimitedSubstring(${argName}, "${delimiterPair}", ${index})`;
                            console.log(`[Translation] Constructing call: ${callString}`); // Log Constructed Call
                            return callString;
                        } else {
                            console.warn(`[Text Rule Translation] Invalid parameters for EXTRACT_BY_DELIMITER: Arg=${argName}, Delim=${delimStr}, Index=${indexStr}. Replacing with null.`);
                            return 'null'; // Replace with null string if params invalid
                        }
                    } catch (e) {
                         console.error(`[Text Rule Translation] Error processing EXTRACT_BY_DELIMITER(${argName}, ${delimStr}, ${indexStr}): ${e}. Replacing with null.`);
                         return 'null';
                    }
                }
            );

            // NUMERIC(argN) -> convertToNumeric(argN)
            translatedExpression = translatedExpression.replace(/NUMERIC\s*\(([^)]+)\)/gi, 'convertToNumeric($1)');

            // Add more translations here if needed
            
            console.log(`[Text Rule] Original Expression: ${expression}`);
            console.log(`[Text Rule] Translated Expression: ${translatedExpression}`);

        } catch (transError) {
             throw new Error(`Text Manipulation function translation failed: ${transError.message}. Original Expression: ${expression}`);
        }
        // --- End Translation ---

        // Use the Function constructor for safe execution of the *translated* expression
        try {
            if (translatedExpression.trim() === '') {
                throw new Error("Expression is empty after translation.");
            }
            // Execute the translated expression
            const dynamicFunction = new Function(...argNames, `"use strict"; return (${translatedExpression});`); 
            const result = dynamicFunction(...args);

            // Return the result directly
            return result;
        } catch (e) {
            throw new Error(`Text Manipulation execution failed: ${e.message}. Translated Expression: ${translatedExpression}`);
        }
    }
     // --- END NEW Execution Handler ---

    // --- Functions for Rendering UI --- 
    function renderTransformationRules() {
        console.log("Transform: Rendering rules list...");
        if (!rulesContainer) return;
        
        rulesContainer.innerHTML = ''; // Clear existing rules
        
        if (transformationRules.length === 0) {
            rulesContainer.innerHTML = '<p class="text-muted small">No transformations defined yet. Click "+ Add Rule" to begin.</p>';
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
                moveRule(rule.id, 'up');
            });
            controlsDiv.appendChild(moveUpBtn);

            const moveDownBtn = document.createElement('button');
            moveDownBtn.className = 'btn btn-sm btn-outline-secondary me-2 py-0 px-1';
            moveDownBtn.innerHTML = '<i class="bi bi-arrow-down"></i>';
            moveDownBtn.title = 'Move rule down';
            moveDownBtn.disabled = index === transformationRules.length - 1; // Disable if last item
            moveDownBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                moveRule(rule.id, 'down');
            });
            controlsDiv.appendChild(moveDownBtn);

            // Edit Button
            const editBtn = document.createElement('button');
            editBtn.className = 'btn btn-sm btn-outline-primary me-1 py-0 px-1';
            editBtn.innerHTML = '<i class="bi bi-pencil"></i>';
            editBtn.title = 'Edit rule';
            editBtn.addEventListener('click', (e) => {
                e.stopPropagation(); 
                editRule(rule.id);
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
                // Confirm before deleting
                if (confirm(`Are you sure you want to delete the rule "${rule.outputFieldName || rule.id}"?`)) {
                    deleteRule(rule.id);
                }
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
    function openTransformationRuleModal(rule = null) {
        console.log(`Opening rule modal. ${rule ? 'Editing rule ID: ' + rule.id : 'Adding new rule.'}`);
        const isEditing = rule !== null;

        // Set Modal Title
        const modalTitle = ruleModal.querySelector('.modal-title');
        if (modalTitle) {
            modalTitle.textContent = isEditing ? 'Edit Transformation Rule' : 'Add Transformation Rule';
        }

        // Reset common fields and statuses
        transformOutputNameInput.value = '';
        transformRuleCommentInput.value = '';
        transformRuleIdInput.value = ''; // Important: Clear ID unless editing
        transformModalStatus.textContent = '';
        transformOutputNameInput.classList.remove('is-invalid');
        outputNameFeedback.textContent = '';

        // Populate Type Selector (always done)
        populateRuleTypeSelector();

        // Clear parameters container initially
        transformParametersContainer.innerHTML = '<p class="text-muted small text-center">Select a Rule Type to configure parameters.</p>';

        if (isEditing) {
            // Populate fields for editing
            transformRuleIdInput.value = rule.id;
            transformOutputNameInput.value = rule.outputFieldName || '';
            transformRuleCommentInput.value = rule.comment || '';
            transformRuleTypeSelect.value = rule.type;

            // Render parameters for the existing rule's type
            renderRuleParametersUI(rule.type);

            // Populate parameter values
            const definition = ruleTypeDefinitions[rule.type];
            if (definition && definition.parameters) {
                definition.parameters.forEach(param => {
                    const inputElement = document.getElementById(`param-${param.id}`);
                    if (inputElement && rule.parameters && rule.parameters.hasOwnProperty(param.id)) {
                        inputElement.value = rule.parameters[param.id];
                    }
                });
            }
        } else {
            // Ensure type selector is reset if adding new
            transformRuleTypeSelect.value = '';
        }

        // Show the modal
        if (bsRuleModal) {
            bsRuleModal.show();
        } else {
            console.error("Bootstrap modal instance not available.");
        }
    }

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
            label.textContent = param.name + (param.required ? ' *' : '');
            formGroup.appendChild(label);

            let inputElement;
            if (param.type === 'textarea') {
                inputElement = document.createElement('textarea');
                inputElement.rows = 3;
            } else if (param.type === 'select') { // Placeholder for future field selector
                 inputElement = document.createElement('select');
                 // TODO: Populate options (e.g., available fields)
                 inputElement.innerHTML = '<option value="">-- Select Field --</option>'; // Add default
            } else { // Default to text input
                inputElement = document.createElement('input');
                inputElement.type = param.type || 'text';
            }

            inputElement.id = `param-${param.id}`;
            inputElement.name = `param-${param.id}`;
            inputElement.className = 'form-control form-control-sm';
            if (param.placeholder) inputElement.placeholder = param.placeholder;
            if (param.required) inputElement.required = true;
            // Value population is handled in openTransformationRuleModal

            formGroup.appendChild(inputElement);

            // --- Add helper buttons specifically for Arithmetic formula textarea ---
            if (ruleType === 'arithmetic' && param.id === 'formula') {
                const helpersContainer = document.createElement('div');
                helpersContainer.className = 'mt-2 d-flex flex-wrap gap-1'; // Spacing and wrapping

                // Field Picker Button (Dropdown Trigger)
                const fieldPickerGroup = document.createElement('div');
                fieldPickerGroup.className = 'btn-group';
                const fieldPickerBtn = document.createElement('button');
                fieldPickerBtn.type = 'button';
                fieldPickerBtn.className = 'btn btn-sm btn-outline-secondary dropdown-toggle';
                fieldPickerBtn.dataset.bsToggle = 'dropdown';
                fieldPickerBtn.innerHTML = '<i class="bi bi-list-ul"></i> Fields';
                const fieldPickerMenu = document.createElement('ul');
                fieldPickerMenu.className = 'dropdown-menu dropdown-menu-sm'; // Add -sm for smaller font
                fieldPickerMenu.id = 'arithmetic-field-picker-dropdown'; // ID used for populating
                fieldPickerMenu.innerHTML = '<li><span class="dropdown-item-text text-muted small">Loading...</span></li>'; // Placeholder

                // Get available fields (needs access to main module data)
                let availableFields = [];
                let numericFields = [];
                try {
                    // Attempt to get fields and metadata from the main module
                    const mainModule = window.AnalyticsMainModule;
                    if (mainModule && typeof mainModule.getAvailableFields === 'function' && typeof mainModule.getFieldMetadata === 'function') {
                        availableFields = mainModule.getAvailableFields(); // Get ALL fields first
                        const metadata = mainModule.getFieldMetadata();
                        // Filter for numeric fields
                        numericFields = availableFields.filter(f => metadata[f]?.type === 'numeric');
                        console.log("[Field Picker] Populating with numeric fields:", numericFields);
                    } else {
                         console.error("[Field Picker] AnalyticsMainModule or required functions (getAvailableFields/getFieldMetadata) not found.");
                         numericFields = []; // Fallback to empty
                    }
                } catch (e) {
                    console.error("[Field Picker] Error getting fields from main module:", e);
                    numericFields = []; // Fallback to empty
                }

                // Clear placeholder before populating
                fieldPickerMenu.innerHTML = '';

                if (numericFields.length > 0) {
                    numericFields.sort().forEach(fieldName => {
                        const li = document.createElement('li');
                        const button = document.createElement('button');
                        button.type = 'button';
                        button.className = 'dropdown-item btn btn-link btn-sm py-0 text-start'; // Use btn styling
                        button.textContent = fieldName;
                        button.addEventListener('click', (e) => {
                            e.preventDefault();
                            const formulaTextarea = document.getElementById('param-formula');
                            insertTextAtCursor(formulaTextarea, `{${fieldName}}`);
                        });
                        li.appendChild(button);
                        fieldPickerMenu.appendChild(li);
                    });
                } else {
                    // Show message if no numeric fields found
                    fieldPickerMenu.innerHTML = '<li><span class="dropdown-item-text text-muted small">(No numeric fields found)</span></li>';
                }

                fieldPickerGroup.appendChild(fieldPickerBtn);
                fieldPickerGroup.appendChild(fieldPickerMenu);
                helpersContainer.appendChild(fieldPickerGroup);

                // MIN Button
                const minBtn = document.createElement('button');
                minBtn.type = 'button';
                minBtn.className = 'btn btn-sm btn-outline-secondary';
                minBtn.textContent = 'MIN()';
                minBtn.title = 'Insert MIN({FieldName})';
                minBtn.addEventListener('click', () => {
                    const textToInsert = 'MIN({FieldName})';
                    const placeholderLength = '{FieldName}'.length;
                    insertTextAtCursor(inputElement, textToInsert);
                    // Select the placeholder
                    const endPos = inputElement.selectionEnd;
                    const startPosPlaceholder = endPos - placeholderLength - 1; // 1 for the closing brace
                    inputElement.setSelectionRange(startPosPlaceholder, endPos -1);
                });
                helpersContainer.appendChild(minBtn);

                // MAX Button
                const maxBtn = document.createElement('button');
                maxBtn.type = 'button';
                maxBtn.className = 'btn btn-sm btn-outline-secondary';
                maxBtn.textContent = 'MAX()';
                maxBtn.title = 'Insert MAX({FieldName})';
                maxBtn.addEventListener('click', () => {
                    const textToInsert = 'MAX({FieldName})';
                    const placeholderLength = '{FieldName}'.length;
                    insertTextAtCursor(inputElement, textToInsert);
                    // Select the placeholder
                    const endPos = inputElement.selectionEnd;
                    const startPosPlaceholder = endPos - placeholderLength - 1;
                    inputElement.setSelectionRange(startPosPlaceholder, endPos -1);
                });
                helpersContainer.appendChild(maxBtn);

                // AVG Button
                const avgBtn = document.createElement('button');
                avgBtn.type = 'button';
                avgBtn.className = 'btn btn-sm btn-outline-secondary';
                avgBtn.textContent = 'AVG()';
                avgBtn.title = 'Insert AVG({FieldName})';
                avgBtn.addEventListener('click', () => {
                    const textToInsert = 'AVG({FieldName})';
                    const placeholderLength = '{FieldName}'.length;
                    insertTextAtCursor(inputElement, textToInsert);
                    // Select the placeholder
                    const endPos = inputElement.selectionEnd;
                    const startPosPlaceholder = endPos - placeholderLength - 1;
                    inputElement.setSelectionRange(startPosPlaceholder, endPos -1);
                });
                helpersContainer.appendChild(avgBtn);

                // SUM Button
                const sumBtn = document.createElement('button');
                sumBtn.type = 'button';
                sumBtn.className = 'btn btn-sm btn-outline-secondary';
                sumBtn.textContent = 'SUM()';
                sumBtn.title = 'Insert SUM({FieldName})';
                sumBtn.addEventListener('click', () => {
                    const textToInsert = 'SUM({FieldName})';
                    const placeholderLength = '{FieldName}'.length;
                    insertTextAtCursor(inputElement, textToInsert);
                    // Select the placeholder
                    const endPos = inputElement.selectionEnd;
                    const startPosPlaceholder = endPos - placeholderLength - 1;
                    inputElement.setSelectionRange(startPosPlaceholder, endPos -1);
                });
                helpersContainer.appendChild(sumBtn);

                // MEDIAN Button
                const medianBtn = document.createElement('button');
                medianBtn.type = 'button';
                medianBtn.className = 'btn btn-sm btn-outline-secondary';
                medianBtn.textContent = 'MEDIAN()';
                medianBtn.title = 'Insert MEDIAN({FieldName})';
                medianBtn.addEventListener('click', () => {
                    const textToInsert = 'MEDIAN({FieldName})';
                    const placeholderLength = '{FieldName}'.length;
                    insertTextAtCursor(inputElement, textToInsert);
                    const endPos = inputElement.selectionEnd;
                    const startPosPlaceholder = endPos - placeholderLength - 1;
                    inputElement.setSelectionRange(startPosPlaceholder, endPos -1);
                });
                helpersContainer.appendChild(medianBtn);

                // TRIM_AVG Button
                const trimAvgBtn = document.createElement('button');
                trimAvgBtn.type = 'button';
                trimAvgBtn.className = 'btn btn-sm btn-outline-secondary';
                trimAvgBtn.textContent = 'TRIM_AVG()';
                trimAvgBtn.title = 'Insert TRIM_AVG({FieldName}, 0.1)';
                trimAvgBtn.addEventListener('click', () => {
                    const textToInsert = 'TRIM_AVG({FieldName}, 0.1)';
                    const placeholderLength = '{FieldName}'.length;
                    const initialCursorPos = inputElement.selectionStart; // Get cursor pos BEFORE insertion
                    
                    insertTextAtCursor(inputElement, textToInsert);
                    
                    // Calculate selection start/end based on initial cursor pos
                    const startPosPlaceholder = initialCursorPos + 'TRIM_AVG('.length;
                    const endPosPlaceholder = startPosPlaceholder + placeholderLength;
                    inputElement.setSelectionRange(startPosPlaceholder, endPosPlaceholder);
                });
                helpersContainer.appendChild(trimAvgBtn);

                // AVG_AFTER_TRIM_MIN Button
                const avgTrimMinBtn = document.createElement('button');
                avgTrimMinBtn.type = 'button';
                avgTrimMinBtn.className = 'btn btn-sm btn-outline-secondary';
                avgTrimMinBtn.textContent = 'AVG_AFTER_TRIM_MIN()';
                avgTrimMinBtn.title = 'Insert AVG_AFTER_TRIM_MIN({FieldName}, 10)'; // Default count 10
                avgTrimMinBtn.addEventListener('click', () => {
                    const textToInsert = 'AVG_AFTER_TRIM_MIN({FieldName}, 10)'; 
                    const placeholderLength = '{FieldName}'.length;
                    const initialCursorPos = inputElement.selectionStart;
                    insertTextAtCursor(inputElement, textToInsert);
                    const startPosPlaceholder = initialCursorPos + 'AVG_AFTER_TRIM_MIN('.length;
                    const endPosPlaceholder = startPosPlaceholder + placeholderLength;
                    inputElement.setSelectionRange(startPosPlaceholder, endPosPlaceholder);
                });
                helpersContainer.appendChild(avgTrimMinBtn);

                // AVG_AFTER_TRIM_MAX Button
                const avgTrimMaxBtn = document.createElement('button');
                avgTrimMaxBtn.type = 'button';
                avgTrimMaxBtn.className = 'btn btn-sm btn-outline-secondary';
                avgTrimMaxBtn.textContent = 'AVG_AFTER_TRIM_MAX()';
                avgTrimMaxBtn.title = 'Insert AVG_AFTER_TRIM_MAX({FieldName}, 10)'; // Default count 10
                avgTrimMaxBtn.addEventListener('click', () => {
                    const textToInsert = 'AVG_AFTER_TRIM_MAX({FieldName}, 10)'; 
                    const placeholderLength = '{FieldName}'.length;
                    const initialCursorPos = inputElement.selectionStart;
                    insertTextAtCursor(inputElement, textToInsert);
                    const startPosPlaceholder = initialCursorPos + 'AVG_AFTER_TRIM_MAX('.length;
                    const endPosPlaceholder = startPosPlaceholder + placeholderLength;
                    inputElement.setSelectionRange(startPosPlaceholder, endPosPlaceholder);
                });
                helpersContainer.appendChild(avgTrimMaxBtn);

                // Operator Buttons
                ['+', '-', '*', '/', '(', ')'].forEach(op => {
                    const opBtn = document.createElement('button');
                    opBtn.type = 'button';
                    opBtn.className = 'btn btn-sm btn-outline-secondary';
                    opBtn.textContent = op;
                    opBtn.title = `Insert '${op}'`;
                    opBtn.addEventListener('click', () => insertTextAtCursor(inputElement, op));
                    helpersContainer.appendChild(opBtn);
                });

                formGroup.appendChild(helpersContainer); // Append helpers below textarea
            }
            // --- END Add helper buttons ---

            // --- Add helper buttons specifically for Text Manipulation expression textarea --- ADDED
            else if (ruleType === 'text_manipulation' && param.id === 'expression') {
                const helpersContainer = document.createElement('div');
                helpersContainer.className = 'mt-2 d-flex flex-wrap gap-1'; // Spacing and wrapping

                // --- Field Picker --- 
                const fieldPickerGroup = document.createElement('div');
                fieldPickerGroup.className = 'btn-group';
                const fieldPickerBtn = document.createElement('button');
                fieldPickerBtn.type = 'button';
                fieldPickerBtn.className = 'btn btn-sm btn-outline-secondary dropdown-toggle';
                fieldPickerBtn.dataset.bsToggle = 'dropdown';
                fieldPickerBtn.innerHTML = '<i class="bi bi-list-ul"></i> Fields';
                const fieldPickerMenu = document.createElement('ul');
                fieldPickerMenu.className = 'dropdown-menu dropdown-menu-sm'; // Add -sm for smaller font
                fieldPickerMenu.innerHTML = '<li><span class="dropdown-item-text text-muted small">Loading...</span></li>'; // Placeholder

                // Get ALL available fields (including non-numeric)
                let allAvailableFields = [];
                try {
                    const mainModule = window.AnalyticsMainModule;
                    if (mainModule && typeof mainModule.getAvailableFields === 'function') {
                        allAvailableFields = mainModule.getAvailableFields(); 
                        console.log("[Text Field Picker] Populating with fields:", allAvailableFields);
                    } else {
                         console.error("[Text Field Picker] AnalyticsMainModule or getAvailableFields not found.");
                         allAvailableFields = [];
                    }
                } catch (e) {
                    console.error("[Text Field Picker] Error getting fields from main module:", e);
                    allAvailableFields = [];
                }

                // Clear placeholder and populate
                fieldPickerMenu.innerHTML = ''; 
                if (allAvailableFields.length > 0) {
                    allAvailableFields.sort().forEach(fieldName => {
                        const li = document.createElement('li');
                        const button = document.createElement('button');
                        button.type = 'button';
                        button.className = 'dropdown-item btn btn-link btn-sm py-0 text-start';
                        button.textContent = fieldName;
                        button.addEventListener('click', (e) => {
                            e.preventDefault();
                            // Use the correct textarea ID (should be inputElement)
                            insertTextAtCursor(inputElement, `{${fieldName}}`); 
                        });
                        li.appendChild(button);
                        fieldPickerMenu.appendChild(li);
                    });
                } else {
                    fieldPickerMenu.innerHTML = '<li><span class="dropdown-item-text text-muted small">(No fields found)</span></li>';
                }

                fieldPickerGroup.appendChild(fieldPickerBtn);
                fieldPickerGroup.appendChild(fieldPickerMenu);
                helpersContainer.appendChild(fieldPickerGroup);
                // --- End Field Picker ---
                
                // --- Operation Buttons --- 
                const ops = [
                    // Keep standard operators/functions as simple inserts
                    { text: '+', insert: ' + ', title: 'Concatenate (Add Strings)' }, // No selection needed
                    { text: `''`, insert: `''`, title: 'Insert Empty String' },
                    // { text: 'parseFloat()', insert: 'parseFloat()', title: 'Convert to Number (Decimal)', select: '' }, // REMOVED
                    // { text: 'parseInt()', insert: 'parseInt()', title: 'Convert to Number (Integer)', select: '' }, // REMOVED
                    // Use function style for methods, selecting {FieldName}
                    { text: 'UPPERCASE()', insert: 'UPPERCASE({FieldName})', title: 'Convert field to Uppercase', select: '{FieldName}' },
                    { text: 'LOWERCASE()', insert: 'LOWERCASE({FieldName})', title: 'Convert field to Lowercase', select: '{FieldName}' },
                    { text: 'TRIM()', insert: 'TRIM({FieldName})', title: 'Remove Whitespace from field', select: '{FieldName}' },
                    // Renamed SUBSTRING to EXTRACT_BY_DELIMITER
                    { text: 'EXTRACT_BY_DELIMITER()', insert: 'EXTRACT_BY_DELIMITER({FieldName}, "()", 1)', title: 'Extract Nth text between delimiters (e.g., "()", "[]")', select: '{FieldName}' }, 
                    // Generic NUMERIC conversion - Corrected to use placeholder
                    { text: 'NUMERIC()', insert: 'NUMERIC({FieldName})', title: 'Convert field value to Number (handles %, etc.)', select: '{FieldName}' }
                ];

                ops.forEach(op => {
                    const opBtn = document.createElement('button');
                    opBtn.type = 'button';
                    opBtn.className = 'btn btn-sm btn-outline-secondary';
                    opBtn.textContent = op.text;
                    opBtn.title = op.title;
                    opBtn.addEventListener('click', () => {
                         const startPos = inputElement.selectionStart; 
                         const textToInsert = op.insert;
                         insertTextAtCursor(inputElement, textToInsert);
                         
                         // Handle selection logic
                         if (op.select !== undefined) { 
                             let selectStartOffset, selectEndOffset;
                             if (op.select === '{FieldName}') {
                                // Find the start of {FieldName} within the inserted text
                                selectStartOffset = textToInsert.indexOf('{FieldName}');
                                selectEndOffset = selectStartOffset + '{FieldName}'.length;
                             } else if (op.select === '') { 
                                 // Select inside parentheses if select is empty string
                                 selectStartOffset = textToInsert.indexOf('(') + 1;
                                 selectEndOffset = textToInsert.indexOf(')');
                                 if (selectEndOffset <= selectStartOffset) { // Handle cases like `` or '' where ) might not exist or be first
                                     selectEndOffset = selectStartOffset; 
                                 }
                             } else { 
                                 // Select a specific substring like 'start, end'
                                 selectStartOffset = textToInsert.indexOf(op.select);
                                 selectEndOffset = selectStartOffset + op.select.length;
                             }

                             // Calculate final selection range based on original cursor position
                             if (selectStartOffset >= 0) { // Only select if found
                                 const finalSelectStart = startPos + selectStartOffset;
                                 const finalSelectEnd = startPos + selectEndOffset;
                                 inputElement.setSelectionRange(finalSelectStart, finalSelectEnd);
                             }
                         }
                         // Else: no selection needed (like for '+')
                     });
                    helpersContainer.appendChild(opBtn);
                });
                // --- End Operation Buttons ---

                formGroup.appendChild(helpersContainer); // Append all helpers
            }
            // --- END Add Text helper buttons ---

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

    // --- NEW HELPER: Insert text into textarea at cursor position ---
    function insertTextAtCursor(textarea, textToInsert) {
        if (!textarea) return;
        const startPos = textarea.selectionStart;
        const endPos = textarea.selectionEnd;
        const currentText = textarea.value;

        textarea.value = currentText.substring(0, startPos) + textToInsert + currentText.substring(endPos);

        // Move cursor to after the inserted text
        textarea.selectionStart = textarea.selectionEnd = startPos + textToInsert.length;
        textarea.focus(); // Keep focus on the textarea
    }
    // --- END HELPER ---

    // --- Event Listeners --- 
    if (addRuleButton) {
        addRuleButton.addEventListener('click', () => {
            console.log("Add Rule button clicked - resetting and opening modal.");
            openTransformationRuleModal(); // Call the new function to open for adding
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
            
            // Read parameters dynamically based on the selected rule type
            const parameters = {};
            const definition = ruleTypeDefinitions[ruleType];
            if (definition && definition.parameters) {
                let paramsValid = true;
                definition.parameters.forEach(param => {
                    const inputElement = document.getElementById(`param-${param.id}`);
                    if (inputElement) {
                        const value = inputElement.value.trim();
                        if (param.required && !value) {
                            // Add visual feedback for required parameter fields
                            inputElement.classList.add('is-invalid');
                            // Optionally add a feedback message sibling
                            paramsValid = false;
                            transformModalStatus.textContent = `Parameter '${param.name}' is required.`;
                        } else {
                            inputElement.classList.remove('is-invalid');
                        }
                        parameters[param.id] = value;
                    } else {
                        console.error(`Could not find input element for parameter ${param.id}`);
                        // Consider if this should be a fatal error for saving
                        transformModalStatus.textContent = `Internal error: Parameter input ${param.id} missing.`;
                        paramsValid = false;
                    }
                });

                if (!paramsValid) {
                    console.warn("Save aborted due to missing required parameters.");
                    return; // Stop saving if required parameters are missing
                }
            } else if (ruleType) {
                 console.log(`Rule type ${ruleType} has no defined parameters to read.`);
            } else { // This case should ideally not be hit due to earlier validation
                console.error("Save button clicked but no rule type selected.");
                transformModalStatus.textContent = 'Error: No rule type selected.';
                return;
            } // else case (no rule type) already handled by basic validation
            
            // Construct the rule data object
            const ruleData = {
                // ID will be set based on add/edit
                type: ruleType,
                outputFieldName: outputName,
                comment: comment,
                // enabled status should be preserved or set default
                parameters: parameters
            };

            if (ruleId) {
                // --- Update Existing Rule --- 
                const index = transformationRules.findIndex(r => r.id === ruleId);
                if (index !== -1) {
                    console.log(`Updating rule with ID: ${ruleId}`);
                    // Merge existing data (like enabled status) with new data
                    transformationRules[index] = {
                        ...transformationRules[index], // Keep existing id, enabled status
                        ...ruleData // Overwrite with new form data (type, name, comment, params)
                    };
                     if(transformStatus) transformStatus.textContent = `Rule "${ruleData.outputFieldName}" updated.`;
                } else {
                    console.error(`Rule ID ${ruleId} not found for update. Cannot save changes.`);
                     transformModalStatus.textContent = `Error: Rule ID ${ruleId} not found. Cannot save.`;
                    // Don't close modal or save if ID is invalid
                    return; 
                }
                // console.log(`TODO: Update rule with ID: ${ruleId}`);
                // alert("Updating existing rules not yet implemented.");
            } else {
                // --- Add New Rule --- 
                ruleData.id = `rule-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`; // Generate new ID
                ruleData.enabled = true; // Default new rules to enabled

                console.log("Adding new rule:", ruleData);
                transformationRules.push(ruleData);
                 if(transformStatus) transformStatus.textContent = `Rule "${ruleData.outputFieldName}" added.`;
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

    // --- END NEW Helper ---

    // --- NEW: Helper function to attempt conversion to a numeric value ---
    function convertToNumeric(value) {
        if (value === null || value === undefined) {
            return null;
        }
        // If it's already a number, return it
        if (typeof value === 'number' && !isNaN(value)) {
            return value;
        }
        // If it's not a string, we can't parse further (unless specific types added later)
        if (typeof value !== 'string') {
             console.warn(`[convertToNumeric] Input is not a string or number: type=${typeof value}, value=${value}. Returning null.`);
            return null;
        }

        const cleanedValue = value.trim();
        if (cleanedValue === '') {
            return null;
        }

        // Handle Percentage
        if (cleanedValue.endsWith('%')) {
            const numPart = cleanedValue.slice(0, -1).trim();
            const num = parseFloat(numPart);
            // Return decimal representation (e.g., "55%" -> 0.55)
            return !isNaN(num) ? num / 100.0 : null;
        }

        // Handle potential K/M/B/T suffixes? (OPTIONAL - Keep simple for now)
        // Example (can be expanded later):
        // let multiplier = 1;
        // let numPart = cleanedValue;
        // if (cleanedValue.endsWith('K')) { multiplier = 1000; numPart = cleanedValue.slice(0,-1); } 
        // else if ... 
        // const num = parseFloat(numPart);
        // return !isNaN(num) ? num * multiplier : null;
        
        // Handle plain number (allow commas? No, parseFloat handles basic format)
        const num = parseFloat(cleanedValue);
        return !isNaN(num) ? num : null; 
    }
    window.convertToNumeric = convertToNumeric; // Make globally accessible
    // --- END NEW Helper ---

    // --- Make helper globally accessible for dynamic function execution --- 

}); 