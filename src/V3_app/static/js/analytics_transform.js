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
        'set_field_conditionally': {
            name: 'Set Field Conditionally (IF/THEN/ELSE)', // Updated Name
            description: 'Set the value of this new output field based on a logical condition.', // Updated Description
            parameters: [
                {
                    id: 'condition',
                    name: 'Condition (evaluates to true/false)',
                    type: 'textarea',
                    placeholder: 'Example: {P/E} > 30 && {Market Cap} > 0',
                    required: true
                },
                {
                    // Renamed parameter id and name
                    id: 'output_value_if_true',
                    name: 'Output Value / Expression if True',
                    type: 'textarea',
                    placeholder: 'Example: "High" or {Price} * 1.1', // Clarified placeholder
                    required: true
                },
                 {
                    // Renamed parameter id and name
                    id: 'output_value_if_false',
                    name: 'Output Value / Expression if False',
                    type: 'textarea',
                    placeholder: 'Example: "Low" or {Price}', // Clarified placeholder
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
         // <<< Regex to find BOTH global and grouped aggregates >>>
         // Groups: 1=aggFunc, 2=aggField, 3=groupKeyword (in), 4=groupField | 5=trimFunc, 6=trimField, 7=trimParam | 8=trimCountFunc, 9=trimCountField, 10=trimCountParam
         const aggregateScanRegex = /(?:(MIN|MAX|AVG|SUM|MEDIAN)\\s*\\(\\s*\\{([^{}]+)\\}\s*(?:\\b(in)\\b\\s*\\{([^{}]+)\\})?\\s*\\))|(?:(TRIM_AVG)\\s*\\(\\s*\'\\{([^{}]+)\\}\'?\\s*,\\s*(\\d+(?:\\.\\d+)?%?)\\s*\\))|(?:(AVG_AFTER_TRIM_MIN|AVG_AFTER_TRIM_MAX)\\s*\\(\\s*\'\\{([^{}]+)\\}\'?\\s*,\\s*(\\d+)\\s*\\))/gi; // CORRECTED REGEX

         // <<< NEW: Structure for needed GROUPED aggregates >>>
         const neededGroupedAggregates = {}; // { groupField: { aggField: Set<aggFunc> } }

         // <<< Store ALL numeric fields encountered for potential grouping >>>
         const allNumericFields = new Set(); // Keep track for efficiency later
 
         rules.forEach(rule => {
            if (!rule.enabled || rule.type !== 'arithmetic' || !rule.parameters?.formula) {
                return; // Skip disabled or non-arithmetic rules
            }
            let formula = rule.parameters.formula; 
            // console.log(`[Pre-scan] Processing Formula: "${formula}"`); // DEBUG REMOVED

            let match;
            const tempGroupedPlaceholders = {}; 
            let placeholderIndex = 0;

            // --- Manual Pre-Parse for FUNC({...} in {...}) Syntax ---
            // console.log("[Pre-scan Manual] Checking for FUNC({...} in {...}) patterns..."); // DEBUG REMOVED
            const manualGroupedPattern = /\(\s*\{([^{}]+)\}\s*\b(in)\b\s*\{([^{}]+)\}\s*\)/i; 
            const validAggFuncs = ["MIN", "MAX", "AVG", "SUM", "MEDIAN"];
            let currentSearchIndex = 0;
            let potentialMatchPos = formula.indexOf('(', currentSearchIndex);

            while (potentialMatchPos !== -1) {
                let functionName = null;
                let functionStartPos = -1;
                for(let i = potentialMatchPos - 1; i >= 0; i--) {
                    const char = formula[i];
                    if (char === ' ' || char === '\t' || char === '\n' || char === '\r') continue; 
                    if (char.match(/[a-zA-Z]/)) { 
                         let start = i;
                         while (start >= 0 && formula[start].match(/[a-zA-Z]/)) {
                             start--;
                         }
                         functionStartPos = start + 1;
                         functionName = formula.substring(functionStartPos, i + 1).toUpperCase();
                         break;
                    } else {
                         break; 
                    }
                }

                if (functionName && validAggFuncs.includes(functionName)) {
                    manualGroupedPattern.lastIndex = potentialMatchPos; 
                    const subMatch = manualGroupedPattern.exec(formula);
                    
                    if (subMatch && subMatch.index === potentialMatchPos) {
                        // const fullMatchedString = formula.substring(functionStartPos, potentialMatchPos + subMatch[0].length); // DEBUG REMOVED
                        // console.log(`[Pre-scan Manual] Found potential match: Func='${functionName}', Pattern='${subMatch[0]}', Full='${fullMatchedString}'`); // DEBUG REMOVED
                        
                        const aggField = subMatch[1];
                        const groupField = subMatch[3];
                        const aggFuncLower = functionName.toLowerCase();
                        let requiredType; 
                        switch(aggFuncLower) {
                            case 'min': requiredType = 'min'; break;
                            case 'max': requiredType = 'max'; break;
                            case 'avg': requiredType = 'avg'; break;
                            case 'sum': requiredType = 'sum'; break;
                            case 'median': requiredType = 'median'; break;
                            default: requiredType = null; 
                        }

                        if (requiredType) {
                            // console.log(`  - Recording GROUPED requirement: ${functionName}({${aggField}} in {${groupField}}) -> Type: ${requiredType}`); // DEBUG REMOVED
                            if (!neededGroupedAggregates[groupField]) {
                                neededGroupedAggregates[groupField] = {};
                            }
                            if (!neededGroupedAggregates[groupField][aggField]) {
                                neededGroupedAggregates[groupField][aggField] = new Set();
                            }
                            neededGroupedAggregates[groupField][aggField].add(requiredType);
                            
                            const placeholder = `__TEMP_GROUPED_AGG_${placeholderIndex++}__`;
                            tempGroupedPlaceholders[placeholder] = { aggFunc: aggFuncLower, aggField, groupField };
                            formula = formula.substring(0, functionStartPos) + placeholder + formula.substring(potentialMatchPos + subMatch[0].length);
                            // console.log(`  - Formula modified for regex pass: "${formula}"`); // DEBUG REMOVED
                            currentSearchIndex = functionStartPos + placeholder.length;
                        } else {
                            currentSearchIndex = potentialMatchPos + 1; 
                        }
                    } else {
                         currentSearchIndex = potentialMatchPos + 1; 
                    }
                } else {
                     currentSearchIndex = potentialMatchPos + 1; 
                }
                potentialMatchPos = formula.indexOf('(', currentSearchIndex);
            }
            // console.log("[Pre-scan Manual] Finished manual check."); // DEBUG REMOVED
            // --- End Manual Pre-Parse ---
            
             rule.parameters.tempGroupedPlaceholders = tempGroupedPlaceholders; 
             rule.parameters.formulaWithPlaceholders = formula; 

            // --- Standard Regex Parse for remaining aggregates (on potentially modified formula) ---
            // console.log(`[Pre-scan Regex] Checking remaining formula "${formula}" for other aggregates...`); // DEBUG REMOVED
            const patternString = "(?:(MIN|MAX|AVG|SUM|MEDIAN)\\s*\\(\\s*\\{([^{}]+)\\}\s*\\))|(?:(TRIM_AVG)\\s*\\(\\s*\'\\{([^{}]+)\\}\'?\\s*,\\s*(\\d+(?:\\.\\d+)?%?)\\s*\\))|(?:(AVG_AFTER_TRIM_MIN|AVG_AFTER_TRIM_MAX)\\s*\\(\\s*\'\\{([^{}]+)\\}\'?\\s*,\\s*(\\d+)\\s*\\))"; 
            const aggregateScanRegex = new RegExp(patternString, "gi");            
            
            aggregateScanRegex.lastIndex = 0; 
            while ((match = aggregateScanRegex.exec(formula)) !== null) {
                 // console.log(`[Pre-scan Regex] Found Other Aggregate Match:`, match); // DEBUG REMOVED
                 const aggFunc = (match[1] || match[3] || match[6])?.toLowerCase();
                 const aggField = match[2] || match[4] || match[7];

                 if (aggField && aggFunc) {
                     let requiredType;
                     switch(aggFunc) {
                        case 'min': requiredType = 'min'; break;
                        case 'max': requiredType = 'max'; break;
                        case 'avg': requiredType = 'avg'; break;
                        case 'sum': requiredType = 'sum'; break;
                        case 'median': requiredType = 'median'; break;
                        case 'trim_avg':
                        case 'avg_after_trim_min':
                        case 'avg_after_trim_max':
                            requiredType = 'values'; 
                            break;
                        default:
                            console.warn(`[Pre-scan Regex] Unknown aggregate function found: ${aggFunc}`); // Keep this warning
                            continue;
                     }
                    // console.log(`  - Recording GLOBAL/OTHER requirement: ${aggFunc.toUpperCase()}({${aggField}}) -> Type: ${requiredType}`); // DEBUG REMOVED
                    if (!neededAggregates[aggField]) {
                        neededAggregates[aggField] = new Set();
                    }
                    neededAggregates[aggField].add(requiredType);
                 }
            }
         });
         console.log("Needed GLOBAL/OTHER aggregates identified by pre-scan:", neededAggregates); 
         console.log("Needed GROUPED aggregates identified by pre-scan:", neededGroupedAggregates);
         // --- End Optimization Pre-scan ---

         // --- Calculate Aggregates --- OPTIMIZED
         console.log("Calculating *ONLY* needed GLOBAL aggregates..."); 
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

         console.log("Final Calculated GLOBAL Aggregates Object:", aggregateResults); // MODIFIED Log
        // --- END Aggregate Calculation ---
        
        // --- NEW: Calculate GROUPED Aggregates --- 
        console.log("Calculating needed GROUPED aggregates...");
        const groupedAggregateResults = {}; // { groupField: { groupValue: { aggField: { results... } } } }

        // Iterate through each GROUPING field identified in the pre-scan
        for (const groupField in neededGroupedAggregates) {
            if (!neededGroupedAggregates.hasOwnProperty(groupField)) continue;
            console.log(` Processing groupField: ${groupField}`);
            groupedAggregateResults[groupField] = {}; // Initialize object for this grouping field
            const neededAggFields = neededGroupedAggregates[groupField]; // Fields to aggregate for this groupField

            // Temporary storage for intermediate results PER GROUP VALUE
            const intermediateGroupData = {}; // { groupValue: { aggField: { sum, count, min, max, values } } }

            // --- Pass 1: Iterate data to collect intermediate values for this grouping field --- 
            workingData.forEach(item => {
                if (!item) return;
                let groupValue = null;
                // Get the value for the current grouping field (e.g., item's Sector)
                if (item.processed_data && item.processed_data.hasOwnProperty(groupField)) {
                    groupValue = item.processed_data[groupField];
                } else if (item.hasOwnProperty(groupField) && groupField !== 'processed_data') {
                    groupValue = item[groupField];
                }
                // Convert null/undefined group values to a consistent key like '__NONE__' or skip?
                // Let's use a key for now to handle potential aggregations on null groups.
                const groupKey = (groupValue === null || groupValue === undefined || String(groupValue).trim() === '') ? '__NONE__' : String(groupValue);

                // Initialize storage for this specific group value if not seen before
                if (!intermediateGroupData[groupKey]) {
                    intermediateGroupData[groupKey] = {};
                }

                // Now, for this item, get the values of the AGGREGATION fields needed for this groupField
                for (const aggField in neededAggFields) {
                    if (!neededAggFields.hasOwnProperty(aggField)) continue;
                    
                    // Initialize storage for this aggField within this groupKey if not seen before
                    if (!intermediateGroupData[groupKey][aggField]) {
                        intermediateGroupData[groupKey][aggField] = {
                            sum: 0,
                            count: 0,
                            min: Infinity,
                            max: -Infinity,
                            values: [] // Always collect for potential median/avg later
                        };
                    }

                    // Get the value to be aggregated (e.g., item's Price)
                    let valueToAggregate = null;
                    if (item.processed_data && item.processed_data.hasOwnProperty(aggField)) {
                        valueToAggregate = item.processed_data[aggField];
                    } else if (item.hasOwnProperty(aggField) && aggField !== 'processed_data') {
                        valueToAggregate = item[aggField];
                    }
                    
                    // Convert to number and update intermediate stats
                    const numValue = Number(valueToAggregate);
                    if (valueToAggregate !== null && valueToAggregate !== undefined && String(valueToAggregate).trim() !== '' && !isNaN(numValue)) {
                        const stats = intermediateGroupData[groupKey][aggField];
                        stats.sum += numValue;
                        stats.count++;
                        if (numValue < stats.min) stats.min = numValue;
                        if (numValue > stats.max) stats.max = numValue;
                        stats.values.push(numValue);
                    }
                } // End loop through needed aggFields
            }); // End data iteration (Pass 1) for groupField

            // --- Pass 2: Finalize calculations for each group value within this groupField --- 
            console.log(` Finalizing aggregates for groupField: ${groupField}`);
            for (const groupKey in intermediateGroupData) {
                if (!intermediateGroupData.hasOwnProperty(groupKey)) continue;
                groupedAggregateResults[groupField][groupKey] = {}; // Initialize final results object for this groupKey
                
                for (const aggField in intermediateGroupData[groupKey]) {
                    if (!intermediateGroupData[groupKey].hasOwnProperty(aggField)) continue;
                    
                    const stats = intermediateGroupData[groupKey][aggField];
                    const neededFunctions = neededGroupedAggregates[groupField][aggField]; // Get the Set of needed funcs
                    const finalResults = {};

                    // <<< Remove intermediate stats log >>>
                    // console.log(`  [Group Calc DEBUG] Finalizing: GroupKey='${groupKey}', AggField='${aggField}', Stats:`, JSON.parse(JSON.stringify(stats))); 

                    if (stats.count > 0) {
                        if (neededFunctions.has('sum')) finalResults.sum = stats.sum;
                        if (neededFunctions.has('avg')) {
                            finalResults.avg = stats.sum / stats.count;
                            // <<< Remove AVG calculation log >>>
                            // console.log(`    [Group Calc DEBUG] Calculated AVG: ${finalResults.avg} (Sum: ${stats.sum}, Count: ${stats.count})`); 
                        }
                        if (neededFunctions.has('min')) finalResults.min = stats.min;
                        if (neededFunctions.has('max')) finalResults.max = stats.max;
                        if (neededFunctions.has('median')) finalResults.median = calculateMedian(stats.values);
                        finalResults.count = stats.count; 
                    } else { 
                        // Handle case where group had no valid numeric data for this aggField
                         if (neededFunctions.has('sum')) finalResults.sum = 0;
                         if (neededFunctions.has('avg')) finalResults.avg = null;
                         if (neededFunctions.has('min')) finalResults.min = null;
                         if (neededFunctions.has('max')) finalResults.max = null;
                         if (neededFunctions.has('median')) finalResults.median = null;
                         finalResults.count = 0;
                    }

                    groupedAggregateResults[groupField][groupKey][aggField] = finalResults;
                }
            }
        } // End loop through groupFields

        console.log("Final Calculated GROUPED Aggregates Object:", groupedAggregateResults);
        // --- END NEW: Calculate GROUPED Aggregates --- 
        
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
                            newValue = executeArithmeticRule(item, rule.parameters, availableFieldsDuringTransform, aggregateResults, groupedAggregateResults);
                            break;
                        case 'text_manipulation':
                            newValue = executeTextManipulationRule(item, rule.parameters, availableFieldsDuringTransform);
                            break;
                        case 'set_field_conditionally':
                            newValue = executeSetFieldConditionallyRule(item, rule.parameters, availableFieldsDuringTransform, aggregateResults);
                            break;
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
    function executeArithmeticRule(item, params, availableFields, aggregateResults = {}, groupedAggregateResults = {}) {
        if (!params || !params.formula) {
            throw new Error("Missing formula parameter for arithmetic rule.");
        }
        let processedFormula = params.formulaWithPlaceholders || params.formula; 
        // console.log(`[Arithmetic Rule] Starting execution with formula: "${processedFormula}"`); // DEBUG REMOVED
        
        const tempGroupedPlaceholders = params.tempGroupedPlaceholders || {}; 
        
        // --- Manual Substitution for Grouped Aggregates using Placeholders ---
        if (Object.keys(tempGroupedPlaceholders).length > 0) {
             // console.log(`[Arithmetic Rule Manual] Substituting grouped aggregate placeholders in "${processedFormula}"...`); // DEBUG REMOVED
             for (const placeholder in tempGroupedPlaceholders) {
                 if (tempGroupedPlaceholders.hasOwnProperty(placeholder)) {
                     const { aggFunc, aggField, groupField } = tempGroupedPlaceholders[placeholder];
                     let replacementValue = null;
                     let functionKey = aggFunc; 
                     try {
                         let currentItemGroupValue = null;
                         if (item.processed_data && item.processed_data.hasOwnProperty(groupField)) {
                             currentItemGroupValue = item.processed_data[groupField];
                         } else if (item.hasOwnProperty(groupField) && groupField !== 'processed_data') {
                             currentItemGroupValue = item[groupField];
                         }
                         const groupKey = (currentItemGroupValue === null || currentItemGroupValue === undefined || String(currentItemGroupValue).trim() === '') ? '__NONE__' : String(currentItemGroupValue);
                         
                         // console.log(`  [Group Lookup Placeholder] GroupField: ${groupField}, GroupKey: ${groupKey}, AggField: ${aggField}, FuncKey: ${functionKey}`); // DEBUG REMOVED
                         replacementValue = groupedAggregateResults?.[groupField]?.[groupKey]?.[aggField]?.[functionKey];
                         // console.log(`  [Group Lookup Placeholder DEBUG] Raw value looked up:`, replacementValue); // DEBUG REMOVED
                         if (replacementValue === undefined) replacementValue = null;
                         // console.log(`  - Placeholder ${placeholder} -> ${replacementValue}`); // DEBUG REMOVED
                     } catch (calcError) {
                         console.error(`  - Error during manual grouped substitution for ${placeholder}: ${calcError}`); // Keep this error
                         replacementValue = null;
                     }
                     const replacementString = replacementValue === null ? 'null' : String(replacementValue);
                     processedFormula = processedFormula.replaceAll(placeholder, replacementString);
                 }
             }
             // console.log(`[Arithmetic Rule Manual] Formula after placeholder substitution: "${processedFormula}"`); // DEBUG REMOVED
        }
        // --- End Manual Substitution ---

        // --- Standard Regex Substitution for remaining aggregates ---
        const patternString = "(?:(MIN|MAX|AVG|SUM|MEDIAN)\\s*\\(\\s*\\{([^{}]+)\\}\s*\\))|(?:(TRIM_AVG)\\s*\\(\\s*\'\\{([^{}]+)\\}\'?\\s*,\\s*(\\d+(?:\\.\\d+)?%?)\\s*\\))|(?:(AVG_AFTER_TRIM_MIN|AVG_AFTER_TRIM_MAX)\\s*\\(\\s*\'\\{([^{}]+)\\}\'?\\s*,\\s*(\\d+)\\s*\\))"; 
        const aggregateRegex = new RegExp(patternString, "gi"); 
        
        aggregateRegex.lastIndex = 0; 
        if (aggregateRegex.test(processedFormula)) { 
            // console.log(`[Arithmetic Rule Regex] Substituting other (Global/TRIM) aggregates in formula "${processedFormula}" for item ${item.ticker}...`); // DEBUG REMOVED
            aggregateRegex.lastIndex = 0; 
            processedFormula = processedFormula.replace(aggregateRegex, (fullMatch, stdAggFunc, stdAggField, trimFunc, trimField, trimParam, trimCountFunc, trimCountField, trimCountParam) => {
                const functionName = stdAggFunc || trimFunc || trimCountFunc;
                const fieldName = stdAggField || trimField || trimCountField;
                const param = trimParam || trimCountParam;
                let replacementValue = null;
                let functionKey = functionName.toLowerCase();

                try { 
                    // --- Handle GLOBAL Trimmed Average (TRIM_AVG) --- 
                    if (functionKey === 'trim_avg') {
                        let customTrimPercent = null;
                         try {
                             if (param && param.endsWith('%')) {
                                 customTrimPercent = parseFloat(param.slice(0, -1)) / 100.0;
                             } else {
                                 customTrimPercent = parseFloat(param);
                             }
                             if (isNaN(customTrimPercent) || customTrimPercent < 0 || customTrimPercent >= 0.5) {
                                 console.warn(`[Transform Rule Regex] Invalid trim percentage '${param}' for ${fieldName}. Sub null.`);
                                 customTrimPercent = null;
                             }
                         } catch (e) {
                              console.warn(`[Transform Rule Regex] Error parsing trim percentage '${param}' for ${fieldName}. Sub null. Error: ${e}`);
                             customTrimPercent = null;
                         }
                         if (customTrimPercent !== null) {
                             const rawValues = aggregateResults[fieldName]?.values;
                             if (rawValues) {
                                 replacementValue = calculateTrimmedAverage(rawValues, customTrimPercent);
                                 // console.log(`  - Regex Calculated TRIM_AVG for ${fullMatch}: ${replacementValue}`); // DEBUG REMOVED
                             } else {
                                 console.warn(`  - Regex Cannot calculate Trimmed Average for ${fieldName}: Raw values not found.`); // Keep this warning
                                 replacementValue = null;
                             }
                         } else {
                             replacementValue = null;
                         }
                    } 
                    // --- Handle GLOBAL Trim Min/Max (AVG_AFTER_TRIM_MIN/MAX) --- 
                    else if (functionKey === 'avg_after_trim_min' || functionKey === 'avg_after_trim_max') {
                        let removeCount = null;
                         try {
                             removeCount = parseInt(param, 10);
                             if (!Number.isInteger(removeCount) || removeCount < 0) {
                                 console.warn(`[Transform Rule Regex] Invalid remove count '${param}' for ${fieldName}. Sub null.`);
                                 removeCount = null;
                             }
                         } catch (e) {
                             console.warn(`[Transform Rule Regex] Error parsing remove count '${param}' for ${fieldName}. Sub null. Error: ${e}`);
                             removeCount = null;
                         }
                         if (removeCount !== null) {
                             const rawValues = aggregateResults[fieldName]?.values;
                             if (rawValues) {
                                 if (functionKey === 'avg_after_trim_min') {
                                     replacementValue = calculateAvgAfterTrimMin(rawValues, removeCount);
                                 } else { 
                                     replacementValue = calculateAvgAfterTrimMax(rawValues, removeCount);
                                 }
                                 // console.log(`  - Regex Result from ${functionKey.toUpperCase()}: ${replacementValue}`); // DEBUG REMOVED
                             } else {
                                 console.warn(`  - Regex Cannot calculate ${functionKey.toUpperCase()} for ${fieldName}: Raw values not found.`); // Keep this warning
                                 replacementValue = null;
                             }
                         } else {
                             replacementValue = null;
                         }
                    } 
                    // --- Handle standard GLOBAL aggregates (MIN, MAX, AVG, SUM, MEDIAN) --- 
                    else {
                         if (aggregateResults && aggregateResults[fieldName] && aggregateResults[fieldName].hasOwnProperty(functionKey) && aggregateResults[fieldName][functionKey] !== undefined) {
                             replacementValue = aggregateResults[fieldName][functionKey];
                             // console.log(`  - Regex Global Lookup: ${functionName}({${fieldName}}) -> ${replacementValue}`); // DEBUG REMOVED
                         } else {
                             replacementValue = null;
                             console.warn(`  - Regex Standard GLOBAL aggregate value for ${functionName}('{${fieldName}}') not found or invalid (key: ${functionKey}).`); // Keep this warning
                         }
                     }
                 } catch(calcError) {
                     console.error(`  - Regex Error during replacement calculation for ${fullMatch}: ${calcError}`); // Keep this error
                     replacementValue = null;
                }
                const replacementString = replacementValue === null ? 'null' : String(replacementValue);
                 // console.log(`  - Regex Replacing ${fullMatch} with ${replacementString}`); // DEBUG REMOVED
                return replacementString; 
            });
            // console.log(`[Arithmetic Rule Regex] Formula after other substitution: "${processedFormula}"`); // DEBUG REMOVED
        }
        // --- End Standard Regex Substitution ---

        // --- Final Formula Evaluation ---
        const fieldPlaceholders = processedFormula.match(/\{([^{}]+)\}/g) || [];
        const fieldNames = fieldPlaceholders.map(ph => ph.substring(1, ph.length - 1));

        const args = []; 
        const argNames = []; 

        fieldNames.forEach((fieldName, index) => {
            const cleanArgName = `arg${index}`; // Create safe variable names (arg0, arg1, ...)
            argNames.push(cleanArgName);

            // Replace placeholder in formula with the safe variable name
            const placeholderRegex = new RegExp(`\\{${fieldName.replace(/[-\/\\^$*+?.()|[\]]/g, '\\$&')}\\}`, 'g');
            processedFormula = processedFormula.replace(placeholderRegex, cleanArgName);

            // Get the value from the item
            let value = null;
            if (!availableFields.has(fieldName)) {
                 console.warn(`Field '{${fieldName}}' used in formula is not available at this stage for ticker ${item.ticker}. Using null.`);
                 value = null;
            } else if (fieldName === 'ticker' && item.ticker !== undefined) {
                 value = item.ticker;
            } else if (fieldName === 'source' && item.source !== undefined) {
                 value = item.source;
            } else if (item.processed_data && item.processed_data.hasOwnProperty(fieldName)) {
                 value = item.processed_data[fieldName];
            }
            // else value remains null

            // REMOVED Strict numeric conversion and invalid argument check
            // const numValue = Number(value);
            // if (value === null || value === undefined || String(value).trim() === '' || isNaN(numValue)) {
            //     console.debug(`Non-numeric or missing value for field '{${fieldName}}' (value: ${value}) in ticker ${item.ticker}. Result will be null.`);
            //     args.push(null);
            //     hasInvalidArgument = true;
            // } else {
            //     args.push(numValue);
            // }
            args.push(value); // Push the raw value (string, number, null, etc.)
        });

        // console.log(`[Arithmetic Rule EXECUTION] Final Processed Formula: "${processedFormula}"`); // DEBUG REMOVED
        // console.log(`[Arithmetic Rule EXECUTION] Arguments for function:`, argNames, args); // DEBUG REMOVED
        try {
            if (processedFormula.trim() === '') {
                throw new Error("Formula is empty after field substitution.");
            }
            const dynamicFunction = new Function(...argNames, `"use strict"; return (${processedFormula});`);
            const result = dynamicFunction(...args);

            if (result === null || result === undefined || !Number.isFinite(result)) {
                // console.debug(`Arithmetic result is non-finite (NaN/Infinity) for ticker ${item.ticker}. Formula: ${params.formula}. Returning null.`); // DEBUG REMOVED
                return null;
            }
            return result;
        } catch (e) {
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

    // --- Execution Handler for Set Field Conditionally Rule --- RENAMED
    function executeSetFieldConditionallyRule(item, params, availableFields, aggregateResults) { // RENAMED function
        // Check for renamed parameters
        if (!params || !params.condition || params.output_value_if_true === undefined || params.output_value_if_false === undefined) {
            throw new Error("Missing condition, output_value_if_true, or output_value_if_false parameter for Set Field Conditionally rule.");
        }
        
        let conditionExpr = params.condition;
        let trueExpr = params.output_value_if_true; // Use renamed param
        let falseExpr = params.output_value_if_false; // Use renamed param
        
        // Combine all expressions to find all unique placeholders
        const combinedExpr = conditionExpr + ' ' + trueExpr + ' ' + falseExpr;
        const fieldPlaceholders = combinedExpr.match(/\{([^{}]+)\}/g) || [];
        // Use a Set to get unique field names, then convert to array
        const fieldNames = [...new Set(fieldPlaceholders.map(ph => ph.substring(1, ph.length - 1)))];

        const args = []; // Values to pass to the function
        const argNames = []; // Variable names inside the function

        // Substitute placeholders in all three expressions
        fieldNames.forEach((fieldName, index) => {
            const cleanArgName = `arg${index}`; 
            argNames.push(cleanArgName);
            
            const placeholderRegex = new RegExp(`\\{${fieldName.replace(/[-\/\\^$*+?.()|[\]]/g, '\\$&')}\\}`, 'g');
            conditionExpr = conditionExpr.replace(placeholderRegex, cleanArgName);
            trueExpr = trueExpr.replace(placeholderRegex, cleanArgName);
            falseExpr = falseExpr.replace(placeholderRegex, cleanArgName);

            // Get the value from the item (check top-level and processed_data)
            let value = null;
            if (availableFields.has(fieldName)) {
                if (item.hasOwnProperty(fieldName) && fieldName !== 'processed_data') {
                    value = item[fieldName];
                } else if (item.processed_data && item.processed_data.hasOwnProperty(fieldName)) {
                    value = item.processed_data[fieldName];
                }
            } else {
                 console.warn(`[Conditional Rule] Field '{${fieldName}}' used in expression is not available for ticker ${item.ticker}. Using null.`);
            }
            // For conditional logic, it's often better to pass the raw value (including null)
            // rather than converting null to empty string like in text manipulation.
            args.push(value); 
        });
        
        // TODO: Add aggregate substitution for trueExpr and falseExpr if needed later.
        // Example using the replacer logic from arithmetic (would need careful integration):
        // trueExpr = trueExpr.replace(aggregateRegex, replacerFunction); 
        // falseExpr = falseExpr.replace(aggregateRegex, replacerFunction); 

        // Use the Function constructor for safe execution
        try {
             // Construct the body carefully to evaluate condition then return appropriate expr
             const functionBody = `
                "use strict";
                try {
                    const conditionResult = (${conditionExpr});
                    if (conditionResult) {
                        return (${trueExpr});
                    } else {
                        return (${falseExpr});
                    }
                } catch (evalError) {
                    console.error('Error during conditional rule evaluation: Condition="${conditionExpr}", TrueExpr="${trueExpr}", FalseExpr="${falseExpr}". Error:', evalError);
                    return null; // Return null if evaluation inside the dynamic function fails
                }
            `;
            const dynamicFunction = new Function(...argNames, functionBody); 
            const result = dynamicFunction(...args);
            return result; 

        } catch (constructError) {
            // Error creating the dynamic function itself (likely syntax error in generated body)
            throw new Error(`Set Field Conditionally function construction failed: ${constructError.message}. Condition: ${params.condition}, True: ${params.output_value_if_true}, False: ${params.output_value_if_false}`); // Updated error message
        }
    }
    // --- END Execution Handler ---

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
        
        const data = dataToPreview || []; // Use passed data or empty array

        if (!data || data.length === 0) {
            transformedDataOutput.textContent = 'No transformed data to display.'; // Updated message
            return;
        }
        
        // Get enabled status from main module to filter preview
        let enabledStatus = {};
        try {
             const mainModule = window.AnalyticsMainModule;
             if (mainModule && typeof mainModule.getFieldEnabledStatus === 'function') {
                 enabledStatus = mainModule.getFieldEnabledStatus() || {};
             } else {
                 console.warn("[Preview] Cannot get field enabled status from main module.");
             }
        } catch (e) {
             console.error("[Preview] Error getting field enabled status:", e);
        }
        
        // Display first 10 rows as JSON string, filtering out disabled fields
        const previewData = data.slice(0, 10).map(item => {
            if (!item) return null; // Handle null items just in case
            const filteredItem = {};
            // Include top-level fields by default (ticker, source, error)
            if (item.ticker !== undefined) filteredItem.ticker = item.ticker;
            if (item.source !== undefined) filteredItem.source = item.source;
            if (item.error !== undefined) filteredItem.error = item.error;
            
            // Include processed_data fields only if enabled
            if (item.processed_data) {
                filteredItem.processed_data = {};
                for (const field in item.processed_data) {
                    // Default to true if status is missing (for new fields created by transform)
                    const isEnabled = enabledStatus[field] === true; 
                    if (isEnabled) {
                        filteredItem.processed_data[field] = item.processed_data[field];
                    }
                }
            }
            return filteredItem;
        }).filter(item => item !== null); // Remove any null items from map result

        try {
            transformedDataOutput.textContent = JSON.stringify(previewData, (key, value) =>
                typeof value === 'bigint' ? value.toString() : value, 
            2);
        } catch (e) {
             console.error("Error stringifying filtered transformed data for preview:", e);
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
                // <<< ADD scrolling styles >>>
                fieldPickerMenu.style.maxHeight = '250px'; // Limit height
                fieldPickerMenu.style.overflowY = 'auto'; // Enable vertical scroll
                fieldPickerMenu.innerHTML = '<li><span class="dropdown-item-text text-muted small">Loading...</span></li>'; // Placeholder

                // --- Get Available Fields from Main Module (with error handling) ---
                let availableFieldsFromMain = [];
                let fieldEnabledStatus = {}; // To store enabled status
                try {
                    if (window.AnalyticsMainModule) {
                        if (typeof window.AnalyticsMainModule.getAvailableFields === 'function') {
                            availableFieldsFromMain = window.AnalyticsMainModule.getAvailableFields() || [];
                        } else {
                            console.warn("[Transform Modal] getAvailableFields function not found on AnalyticsMainModule.");
                        }
                        // Get enabled status
                        if (typeof window.AnalyticsMainModule.getFieldEnabledStatus === 'function') {
                            fieldEnabledStatus = window.AnalyticsMainModule.getFieldEnabledStatus() || {};
                        } else {
                             console.warn("[Transform Modal] getFieldEnabledStatus function not found on AnalyticsMainModule.");
                        }
                         // <<< ADD LOGGING HERE >>>
                         console.log("[Transform Modal DEBUG] Data received before filtering:");
                         console.log("[Transform Modal DEBUG] availableFieldsFromMain:", JSON.stringify(availableFieldsFromMain));
                         console.log("[Transform Modal DEBUG] fieldEnabledStatus:", JSON.stringify(fieldEnabledStatus));
                         // <<< END LOGGING >>>
                    } else {
                         console.warn("[Transform Modal] AnalyticsMainModule not found. Cannot get fields or status.");
                    }
                } catch (e) {
                    console.error("[Transform Modal] Error getting fields/status from AnalyticsMainModule:", e);
                }

                // Filter fields based on enabled status (strict true check)
                const enabledFieldsForDropdown = availableFieldsFromMain.filter(field => {
                    const isEnabled = fieldEnabledStatus[field] === true;
                    return isEnabled;
                });
                console.log("[Transform Modal] Fields for dropdown after filtering by enabled status (strict true check):", enabledFieldsForDropdown);

                // --- Populate Field Selectors ---
                fieldPickerMenu.innerHTML = '';
                if (enabledFieldsForDropdown.length > 0) {
                    enabledFieldsForDropdown.forEach(fieldName => {
                        const li = document.createElement('li');
                        const button = document.createElement('button');
                        button.type = 'button';
                        button.className = 'dropdown-item btn btn-link btn-sm py-0 text-start';
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
                    fieldPickerMenu.innerHTML = '<li><span class="dropdown-item-text text-muted small">(No fields)</span></li>';
                }

                fieldPickerGroup.appendChild(fieldPickerBtn);
                fieldPickerGroup.appendChild(fieldPickerMenu);
                helpersContainer.appendChild(fieldPickerGroup);

                // <<< NEW: Group By Field Picker Button >>>
                const groupFieldPickerGroup = document.createElement('div');
                groupFieldPickerGroup.className = 'btn-group';
                const groupFieldPickerBtn = document.createElement('button');
                groupFieldPickerBtn.type = 'button';
                groupFieldPickerBtn.className = 'btn btn-sm btn-outline-secondary dropdown-toggle';
                groupFieldPickerBtn.dataset.bsToggle = 'dropdown';
                groupFieldPickerBtn.innerHTML = '<i class="bi bi-folder2"></i> Group By'; // Icon indicating grouping
                groupFieldPickerBtn.title = 'Select Grouping Field';
                const groupFieldPickerMenu = document.createElement('ul');
                groupFieldPickerMenu.className = 'dropdown-menu dropdown-menu-sm';
                groupFieldPickerMenu.id = 'arithmetic-group-field-picker-dropdown'; // Unique ID
                // Add scrolling styles
                groupFieldPickerMenu.style.maxHeight = '250px'; 
                groupFieldPickerMenu.style.overflowY = 'auto'; 
                groupFieldPickerMenu.innerHTML = '<li><span class="dropdown-item-text text-muted small">Loading...</span></li>'; // Placeholder
                
                groupFieldPickerGroup.appendChild(groupFieldPickerBtn);
                groupFieldPickerGroup.appendChild(groupFieldPickerMenu);
                helpersContainer.appendChild(groupFieldPickerGroup); // Add the new group
                // <<< END NEW >>>
                // <<< Populate Group By Picker >>>
                try {
                    const mainModule = window.AnalyticsMainModule;
                    let allFieldsForGrouping = [];
                    let metadataForGrouping = {};
                    if (mainModule && typeof mainModule.getAvailableFields === 'function' && typeof mainModule.getFieldMetadata === 'function') {
                        // Use PRE-transform fields/metadata for defining grouping options
                        allFieldsForGrouping = mainModule.getAvailableFields(); 
                        metadataForGrouping = mainModule.getFieldMetadata();
                        // <<< Get Enabled Status >>>
                        enabledStatusForGrouping = mainModule.getFieldEnabledStatus ? mainModule.getFieldEnabledStatus() : {}; 
                     } else {
                         console.error("[Group Field Picker] AnalyticsMainModule or required functions not found.");
                     }
                    // Filter for non-numeric AND enabled fields
                    const nonNumericEnabledFields = allFieldsForGrouping.filter(f => 
                        metadataForGrouping[f]?.type !== 'numeric' && // Check type
                        (enabledStatusForGrouping[f] === true || enabledStatusForGrouping[f] === undefined) // Check enabled (default true if missing)
                    );
                    console.log("[Group Field Picker] Populating with non-numeric, enabled fields:", nonNumericEnabledFields);
 
                     groupFieldPickerMenu.innerHTML = ''; // Clear loading placeholder
                    if (nonNumericEnabledFields.length > 0) {
                        nonNumericEnabledFields.sort().forEach(fieldName => {
                            const li = document.createElement('li');
                            const button = document.createElement('button');
                            button.type = 'button';
                            button.className = 'dropdown-item btn btn-link btn-sm py-0 text-start';
                            button.textContent = fieldName;
                            button.addEventListener('click', (e) => {
                                e.preventDefault();
                                const formulaTextarea = document.getElementById('param-formula');
                                // Insert {FieldName} at cursor - user manually places it in 'in {...}'
                                insertTextAtCursor(formulaTextarea, `{${fieldName}}`); 
                            });
                            li.appendChild(button);
                            groupFieldPickerMenu.appendChild(li);
                        });
                    } else {
                        groupFieldPickerMenu.innerHTML = '<li><span class="dropdown-item-text text-muted small">(No text/grouping fields found)</span></li>';
                    }
                } catch (e) {
                     console.error("[Group Field Picker] Error populating picker:", e);
                     groupFieldPickerMenu.innerHTML = '<li><span class="dropdown-item-text text-danger small">Error loading fields</span></li>';
                }
                // <<< END Populate >>>
                // MIN Button
                const minBtn = document.createElement('button');
                minBtn.type = 'button';
                minBtn.className = 'btn btn-sm btn-outline-secondary';
                minBtn.textContent = 'MIN()';
                minBtn.title = 'Insert MIN({FieldName})';
                minBtn.addEventListener('click', () => {
                    const textToInsert = 'MIN({FieldName})';
                    const placeholderLength = '{FieldName}'.length;
                    const initialCursorPos = inputElement.selectionStart; // Get cursor pos BEFORE insertion
                    insertTextAtCursor(inputElement, textToInsert);
                    // Select the placeholder inside the parentheses
                    const startPosPlaceholder = initialCursorPos + 'MIN('.length;
                    const endPosPlaceholder = startPosPlaceholder + placeholderLength;
                    inputElement.setSelectionRange(startPosPlaceholder, endPosPlaceholder);
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
                    const initialCursorPos = inputElement.selectionStart;
                    insertTextAtCursor(inputElement, textToInsert);
                    // Select the placeholder inside the parentheses
                    const startPosPlaceholder = initialCursorPos + 'MAX('.length;
                    const endPosPlaceholder = startPosPlaceholder + placeholderLength;
                    inputElement.setSelectionRange(startPosPlaceholder, endPosPlaceholder);
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
                    const initialCursorPos = inputElement.selectionStart;
                    insertTextAtCursor(inputElement, textToInsert);
                    // Select the placeholder inside the parentheses
                    const startPosPlaceholder = initialCursorPos + 'AVG('.length;
                    const endPosPlaceholder = startPosPlaceholder + placeholderLength;
                    inputElement.setSelectionRange(startPosPlaceholder, endPosPlaceholder);
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
                    const initialCursorPos = inputElement.selectionStart;
                    insertTextAtCursor(inputElement, textToInsert);
                    // Select the placeholder inside the parentheses
                    const startPosPlaceholder = initialCursorPos + 'SUM('.length;
                    const endPosPlaceholder = startPosPlaceholder + placeholderLength;
                    inputElement.setSelectionRange(startPosPlaceholder, endPosPlaceholder);
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
                    const initialCursorPos = inputElement.selectionStart;
                    insertTextAtCursor(inputElement, textToInsert);
                    // Select the placeholder inside the parentheses
                    const startPosPlaceholder = initialCursorPos + 'MEDIAN('.length;
                    const endPosPlaceholder = startPosPlaceholder + placeholderLength;
                    inputElement.setSelectionRange(startPosPlaceholder, endPosPlaceholder);
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

                // <<< NEW: Grouped Aggregate Button >>>
                const groupedAggBtn = document.createElement('button');
                groupedAggBtn.type = 'button';
                groupedAggBtn.className = 'btn btn-sm btn-outline-info'; // Use info color for distinction
                groupedAggBtn.textContent = 'AGG(... in Group)'; // <<< Generic Text
                groupedAggBtn.title = 'Insert Grouped Aggregate Template: FUNC({AggField} in {GroupField})'; // <<< Updated Generic Title to new syntax
                groupedAggBtn.addEventListener('click', () => {
                    const textToInsert = 'AVG({AggField} in {GroupField})'; // <<< Insert with 'in' inside parens
                    const functionLength = 'AVG'.length;
                    const initialCursorPos = inputElement.selectionStart;
                    insertTextAtCursor(inputElement, textToInsert);
                    // <<< Select the FUNCTION part (AVG) >>>
                    const startPosPlaceholder = initialCursorPos;
                    const endPosPlaceholder = startPosPlaceholder + functionLength;
                    inputElement.setSelectionRange(startPosPlaceholder, endPosPlaceholder);
                });
                helpersContainer.appendChild(groupedAggBtn);
                // <<< END NEW >>>

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
                fieldPickerMenu.id = 'arithmetic-field-picker-dropdown'; // ID used for populating
                // <<< ADD scrolling styles >>>
                fieldPickerMenu.style.maxHeight = '250px'; // Limit height
                fieldPickerMenu.style.overflowY = 'auto'; // Enable vertical scroll
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

            // --- Add helper buttons specifically for Conditional Assignment textareas --- ADDED
            else if (ruleType === 'set_field_conditionally') { // Apply to all params: condition, value_if_true, value_if_false
                 const helpersContainer = document.createElement('div');
                 helpersContainer.className = 'mt-1 d-flex flex-wrap gap-1'; // Smaller margin, spacing 

                 // Determine target textarea based on the parameter ID
                 const targetTextarea = inputElement; // Assumes inputElement is the current textarea
                 
                 // --- Field Picker (All Fields) --- 
                 const fieldPickerGroup = document.createElement('div');
                 fieldPickerGroup.className = 'btn-group';
                 const fieldPickerBtn = document.createElement('button');
                 fieldPickerBtn.type = 'button';
                 fieldPickerBtn.className = 'btn btn-sm btn-outline-secondary dropdown-toggle';
                 fieldPickerBtn.dataset.bsToggle = 'dropdown';
                 fieldPickerBtn.innerHTML = '<i class="bi bi-list-ul"></i> Fields';
                 const fieldPickerMenu = document.createElement('ul');
                 fieldPickerMenu.className = 'dropdown-menu dropdown-menu-sm';
                 // <<< ADD scrolling styles >>>
                 fieldPickerMenu.style.maxHeight = '250px'; // Limit height
                 fieldPickerMenu.style.overflowY = 'auto'; // Enable vertical scroll
                 fieldPickerMenu.innerHTML = '<li><span class="dropdown-item-text text-muted small">Loading...</span></li>';

                 // Get ALL available fields
                 let allAvailableFields = [];
                 try {
                     const mainModule = window.AnalyticsMainModule;
                     if (mainModule && typeof mainModule.getAvailableFields === 'function') {
                         allAvailableFields = mainModule.getAvailableFields(); 
                     } else { allAvailableFields = []; }
                 } catch (e) { allAvailableFields = []; }

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
                             insertTextAtCursor(targetTextarea, `{${fieldName}}`); 
                         });
                         li.appendChild(button);
                         fieldPickerMenu.appendChild(li);
                     });
                 } else {
                     fieldPickerMenu.innerHTML = '<li><span class="dropdown-item-text text-muted small">(No fields)</span></li>';
                 }
                 fieldPickerGroup.appendChild(fieldPickerBtn);
                 fieldPickerGroup.appendChild(fieldPickerMenu);
                 helpersContainer.appendChild(fieldPickerGroup);
                 // --- End Field Picker ---

                 // --- Operator / Value Buttons --- 
                 let ops = [];
                 if (param.id === 'condition') {
                     // Operators for Condition
                     ops = [
                         { text: '===', insert: ' === ', title: 'Strict Equals' },
                         { text: '!==', insert: ' !== ', title: 'Strict Not Equals' },
                         { text: '>', insert: ' > ', title: 'Greater Than' },
                         { text: '<', insert: ' < ', title: 'Less Than' },
                         { text: '>=', insert: ' >= ', title: 'Greater Than or Equal' },
                         { text: '<=', insert: ' <= ', title: 'Less Than or Equal' },
                         { text: '&&', insert: ' && ', title: 'Logical AND' },
                         { text: '||', insert: ' || ', title: 'Logical OR' },
                         { text: '!', insert: '!', title: 'Logical NOT' },
                         { text: '()', insert: '()' , title: 'Parentheses', select: '' },
                         { text: 'null', insert: 'null', title: 'Insert null value' },
                         { text: 'true', insert: 'true', title: 'Insert true value' },
                         { text: 'false', insert: 'false', title: 'Insert false value' },
                         { text: `''`, insert: `''`, title: 'Insert Empty String' },
                         { text: `0`, insert: `0`, title: 'Insert Zero' }
                     ];
                 } else { // For value_if_true and value_if_false
                     // Allow basic operators, functions, values
                     ops = [
                         // Basic Arithmetic (more complex handled by dedicated type)
                         { text: '+', insert: ' + ', title: 'Add / Concatenate' },
                         { text: '-', insert: ' - ', title: 'Subtract' },
                         { text: '*', insert: ' * ', title: 'Multiply' },
                         { text: '/', insert: ' / ', title: 'Divide' },
                         { text: '()', insert: '()', title: 'Parentheses', select: '' },
                          // Basic values
                         { text: 'null', insert: 'null', title: 'Insert null value' },
                         { text: `''`, insert: `''`, title: 'Insert Empty String' },
                         { text: `0`, insert: `0`, title: 'Insert Zero' },
                         // TODO: Add Aggregate function buttons here later if needed (AVG, SUM etc)
                     ];
                 }

                 ops.forEach(op => {
                     const opBtn = document.createElement('button');
                     opBtn.type = 'button';
                     opBtn.className = 'btn btn-sm btn-outline-secondary';
                     opBtn.textContent = op.text;
                     opBtn.title = op.title;
                     opBtn.addEventListener('click', () => {
                          const startPos = targetTextarea.selectionStart; 
                          insertTextAtCursor(targetTextarea, op.insert);
                          if (op.select !== undefined && op.select === '') { // Select inside parens
                               const finalSelectStart = startPos + op.insert.indexOf('(') + 1;
                               targetTextarea.setSelectionRange(finalSelectStart, finalSelectStart);
                          }
                      });
                     helpersContainer.appendChild(opBtn);
                 });
                 // --- End Operator Buttons ---

                 formGroup.appendChild(helpersContainer); 
            }
            // --- END Add Conditional helper buttons ---

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
            
            // <<< Define related buttons to disable >>>
            const buttonsToDisable = [
                applyTransformationsButton,
                addRuleButton,
                saveRulesButton,
                loadRulesButton
                // Add export/import buttons here if they exist later
            ];

            // <<< Show spinner and disable buttons >>>
            if (typeof window.showSpinner === 'function') {
                window.showSpinner(applyTransformationsButton, null); // Pass null as we handle disabling others manually
                buttonsToDisable.forEach(btn => { if(btn) btn.disabled = true; });
            } else {
                console.error("Global showSpinner function not found!");
                // Fallback: just disable the main button
                applyTransformationsButton.disabled = true;
            }
            if(transformStatus) transformStatus.textContent = "Applying transformations..."; // Update status

            try {
            // Trigger the main analytics module to run the transformation process
                // <<< Use setTimeout to allow UI repaint before starting work >>>
                setTimeout(() => {
                    try {
            if (window.AnalyticsMainModule && typeof window.AnalyticsMainModule.runTransformations === 'function') {
                 window.AnalyticsMainModule.runTransformations();
                             // Status message will be updated by the main module's runTransformations
            } else {
                console.error("AnalyticsMainModule or runTransformations function not found.");
                alert("Error: Cannot trigger transformation process. Main module not available.");
                 if(transformStatus) transformStatus.textContent = "Error: Main analytics module not found.";
            }
                    } catch (innerError) {
                        // Catch errors *during* the transformation run
                        console.error("Error during transformation execution:", innerError);
                        if(transformStatus) transformStatus.textContent = `Error: ${innerError.message}`;
                    } finally {
                        // <<< Hide spinner and re-enable buttons AFTER the timeout finishes >>>
                        if (typeof window.hideSpinner === 'function') {
                            window.hideSpinner(applyTransformationsButton, null); // Pass null as we handle enabling others manually
                            buttonsToDisable.forEach(btn => { if(btn) btn.disabled = false; });
                        } else {
                            console.error("Global hideSpinner function not found!");
                            // Fallback: just re-enable the main button
                            applyTransformationsButton.disabled = false;
                        }
                    }
                }, 0); // Timeout of 0 ms yields control briefly

            } catch (error) {
                 // Catch errors specifically from triggering the process (e.g., showSpinner errors - unlikely now)
                 console.error("Error setting up transformation process:", error);
                 if(transformStatus) transformStatus.textContent = `Error: ${error.message}`;
                 // <<< Need to hide spinner/enable buttons here too if setup fails >>>
                 if (typeof window.hideSpinner === 'function') {
                     window.hideSpinner(applyTransformationsButton, null);
                     buttonsToDisable.forEach(btn => { if(btn) btn.disabled = false; });
                 } else {
                     applyTransformationsButton.disabled = false;
                 }
            } 
            // <<< REMOVE finally block from outer try...catch >>>
            // finally {
            //      // <<< Hide spinner and re-enable buttons >>>
            //      ...
            // }
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
        renderTransformedDataPreview: renderTransformedDataPreview, // Allow external update of preview
        loadTransformationRules: loadTransformationRules // <<< ADDED Setter
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

    // --- NEW: Function to Load Rules Directly ---
    function loadTransformationRules(rulesArray) {
        console.log("[Transform] Loading rules directly:", rulesArray);
        if (!Array.isArray(rulesArray)) {
            console.error("[Transform] Invalid rules data provided for direct load.", rulesArray);
            transformationRules = []; // Reset to empty array
        } else {
            // Basic validation/mapping could be added here if needed (e.g., ensure ID, type, etc.)
            transformationRules = rulesArray;
        }
        // No need to save to storage here
        renderTransformationRules(); // Re-render the rules list UI
        console.log("[Transform] Rules loaded and UI updated.");
    }
    // --- END NEW Function ---

}); 