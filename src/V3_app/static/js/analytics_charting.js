// Analytics Charting Module
(function() {
    console.log("AnalyticsChartingModule loading...");

    // --- Module-Scoped Variables ---
    let reportChartInstance = null;
    let currentChartPlotData = []; // Holds plot data ({x, y, ticker,...}) for current chart rendering
    let highlightedPointIndex = -1;
    let originalPointStyle = null;
    let chartSearchTimeout;
    const HIGHLIGHT_COLOR = 'rgba(255, 255, 0, 1)'; // Bright yellow
    const HIGHLIGHT_RADIUS_INCREASE = 6; // Use the latest value

    // --- DOM Element References (initialized in initChart) ---
    let reportFieldSelector = null;
    let reportXAxisSelector = null;
    let reportChartCanvas = null;
    let chartStatus = null;
    let reportColorSelector = null;
    let reportChartTypeSelector = null;
    let reportSizeSelector = null;
    let resetChartBtn = null;
    let swapAxesBtn = null;
    let chartTickerSearchInput = null;
    let chartSearchStatus = null; // Added for search status messages

    // --- Helper Functions ---

    // Helper function to get value from item, prioritizing processed_data
    // Moved from renderChart to module scope
    const getValue = (item, field) => {
        if (!item) return null;

        // <<< ADD Ticker Debugging >>>
        let valueFound = null;
        let foundLocation = null;

        // 1. Check inside processed_data (where original AND synthetic fields live)
        if (item.processed_data && item.processed_data.hasOwnProperty(field)) {
            // return item.processed_data[field]; // OLD RETURN
            valueFound = item.processed_data[field];
            foundLocation = 'processed_data';
        }
        // 2. If not found in processed_data, check top-level fields
        // <<< Use simple property check instead of hasOwnProperty >>>
        else if (item[field] !== undefined && field !== 'processed_data') {
                // console.debug(`[getValue] Field '${field}' found at top level.`); // Keep debug for non-ticker?
                // return item[field]; // OLD RETURN
                valueFound = item[field];
                foundLocation = 'top-level';
        }

        // <<< ADD Ticker Debugging LOG >>>
        // Keep the detailed logging for Ticker if still needed for debugging getValue issues
        if (field === 'Ticker') {
            // <<< Update log to reflect new check >>>
            // console.log(`[getValue DEBUG for Ticker] Checking field '${field}' on item:`, item); // Log the item object
            try {
                // console.log(`[getValue DEBUG for Ticker] Object.keys(item):`, Object.keys(item)); // Log keys
            } catch (e) {
                // console.log(`[getValue DEBUG for Ticker] Error getting Object.keys(item):`, e);
            }
            // console.log(`[getValue DEBUG for Ticker] Checked processed_data: ${item.processed_data?.hasOwnProperty('Ticker') ? 'Found' : 'Not Found'}. Checked top-level (item["Ticker"] !== undefined): ${item['Ticker'] !== undefined ? 'Found' : 'Not Found'}. Found Location: ${foundLocation || 'None'}. Value Returning:`, valueFound);
        }

        // 3. Return the found value or null if not found anywhere
        return valueFound;
        // <<< END Ticker Debugging >>>
    };


    // --- Chart Rendering Logic ---
    function renderChart() {
        // Check if initialization has happened
        if (!reportChartCanvas || !chartStatus || !reportFieldSelector || !reportXAxisSelector || !reportColorSelector || !reportSizeSelector || !reportChartTypeSelector) {
            console.error('[Charting] Chart elements not initialized. Call initChart first.');
            return;
        }

        const ctx = reportChartCanvas.getContext('2d');
        if (!ctx) {
            console.error('[Charting] Failed to get 2D context from canvas.');
            return;
        }

        // Access data and formatting functions from the main module
        const mainModule = window.AnalyticsMainModule;
        if (!mainModule) {
            console.error("[Charting] AnalyticsMainModule not found!");
            if (chartStatus) chartStatus.textContent = 'Error: Main analytics module not loaded.';
            return;
        }

        const finalDataForAnalysis = mainModule.getFinalDataForAnalysis ? mainModule.getFinalDataForAnalysis() : [];
        const finalFieldMetadata = mainModule.getFinalFieldMetadata ? mainModule.getFinalFieldMetadata() : {};
        // Get formatting functions/data needed from main module
        const formatNumericValue = mainModule.formatNumericValue || function(val) { return String(val); }; // Provide fallback
        // Assuming these getters exist/will be added in Step 4
        const postTransformNumericFormats = mainModule.getPostTransformNumericFormats ? mainModule.getPostTransformNumericFormats() : {};
        const preTransformNumericFormats = mainModule.getNumericFieldFormats ? mainModule.getNumericFieldFormats() : {}; // Still need pre-transform as fallback? Yes, tooltip uses it.

        // --- Check if data is ready (adapted from original) ---
        if (!finalDataForAnalysis || !Array.isArray(finalDataForAnalysis) || finalDataForAnalysis.length === 0 || !finalFieldMetadata || Object.keys(finalFieldMetadata).length === 0) {
            console.log('[Charting] Post-transformation data not ready. Clearing chart.');
            chartStatus.textContent = 'Load data and run transformations to generate the chart.';
            if (reportChartInstance) {
                reportChartInstance.destroy();
                reportChartInstance = null;
                console.log('[Charting] Previous chart instance destroyed.');
            }
            return;
        }

        const xAxisField = reportXAxisSelector.value || 'index';
        const yAxisField = reportFieldSelector.value;

        console.log("[Charting] Rendering chart...");

        let plotData = [];
        const labels = [];
        const numericData = [];
        const pointBackgroundColors = [];
        const pointBorderColors = [];
        let nonNumericXCount = 0;
        let nonNumericYCount = 0;
        let nonNumericSizeCount = 0;
        let missingItemCount = 0;

        const selectedYField = yAxisField; // Already have this
        const selectedXField = xAxisField; // Already have this
        const colorField = reportColorSelector.value;
        const sizeField = reportSizeSelector.value;
        const selectedChartType = reportChartTypeSelector.value;
        const useIndexAsX = (!selectedXField || selectedXField === 'index');
        const currentMetadata = finalFieldMetadata; // Use post-transform metadata directly

        console.log(`[Charting] Selected X:${useIndexAsX ? 'Index' : selectedXField}, Y:${selectedYField}, Color:${colorField || '-'}, Size:${sizeField || '-'}, Type:${selectedChartType}`);

        chartStatus.textContent = ''; // Clear previous status

        // --- Disable/Enable Selectors based on Type ---
        if (selectedChartType === 'bar') {
            reportXAxisSelector.disabled = true;
        } else {
            reportXAxisSelector.disabled = false;
        }
        reportSizeSelector.disabled = (selectedChartType !== 'bubble');

        // --- Validation ---
        if (!selectedYField) {
            chartStatus.textContent = 'Please select a field for the Y axis.';
            if (reportChartInstance) reportChartInstance.destroy(); reportChartInstance = null;
            return;
        }
        if (selectedChartType === 'bubble' && !sizeField) {
            chartStatus.textContent = 'Please select a field for Bubble Size.';
            if (reportChartInstance) reportChartInstance.destroy(); reportChartInstance = null;
            return;
        }
        if (!finalDataForAnalysis || finalDataForAnalysis.length === 0) {
            chartStatus.textContent = 'No data available to plot.';
             if (reportChartInstance) reportChartInstance.destroy(); reportChartInstance = null;
            return;
        }

        // --- Pre-calculate min/max for size scaling (Bubble only) ---
        let minSize = Infinity;
        let maxSize = -Infinity;
        if (selectedChartType === 'bubble' && sizeField) {
            finalDataForAnalysis.forEach(item => {
                if (item) {
                    const sizeValue = getValue(item, sizeField);
                    const numericSize = Number(sizeValue);
                    if (!isNaN(numericSize) && numericSize > 0) {
                        if (numericSize < minSize) minSize = numericSize;
                        if (numericSize > maxSize) maxSize = numericSize;
                    }
                }
            });
            if (minSize === Infinity || maxSize === -Infinity) {
                console.warn(`[Charting] No valid positive numeric data found for size field '${sizeField}'. Bubbles will have default size.`);
                minSize = 1; maxSize = 1;
            } else if (minSize === maxSize) {
                 minSize = maxSize / 2;
            }
            console.log(`[Charting - Bubble Scaling] Min Size: ${minSize}, Max Size: ${maxSize} for field '${sizeField}'`);
        }

        // --- Color mapping logic ---
        const colorMap = {};
        const predefinedColors = [
            'rgba(255, 99, 132, 0.7)', 'rgba(54, 162, 235, 0.7)', 'rgba(255, 206, 86, 0.7)',
            'rgba(75, 192, 192, 0.7)', 'rgba(153, 102, 255, 0.7)', 'rgba(255, 159, 64, 0.7)',
            'rgba(199, 199, 199, 0.7)', 'rgba(83, 102, 255, 0.7)', 'rgba(40, 159, 64, 0.7)',
            'rgba(210, 99, 132, 0.7)'
        ];
        let colorIndex = 0;
        const defaultColor = 'rgba(128, 128, 128, 0.6)';
        const getColorForValue = (value) => {
            if (value === null || value === undefined || value === '') return defaultColor;
            const valueStr = String(value);
            if (!colorMap[valueStr]) {
                colorMap[valueStr] = predefinedColors[colorIndex % predefinedColors.length];
                colorIndex++;
            }
            return colorMap[valueStr];
        };

        // --- Process data points ---
        nonNumericXCount = 0; nonNumericYCount = 0; nonNumericSizeCount = 0; missingItemCount = 0;

        finalDataForAnalysis.forEach((item, index) => {
             let pointColor = defaultColor;
             let colorValue = null;

             if (item) {
                 const rawYValue = getValue(item, selectedYField);
                 const numericYValue = Number(rawYValue);
                 const isYNumeric = !isNaN(numericYValue) && rawYValue !== null && String(rawYValue).trim() !== '';

                 let rawXValue = null;
                 let numericXValue = null;
                 let isXNumeric = false;
                 if (selectedChartType === 'scatter' || selectedChartType === 'line' || selectedChartType === 'bubble') {
                     if (useIndexAsX) {
                         numericXValue = index; rawXValue = index; isXNumeric = true;
                     } else {
                         rawXValue = getValue(item, selectedXField);
                         numericXValue = Number(rawXValue);
                         isXNumeric = !isNaN(numericXValue) && rawXValue !== null && String(rawXValue).trim() !== '';
                     }
                 }

                 let isValidPoint = false;
                 if (selectedChartType === 'scatter' || selectedChartType === 'line') {
                     isValidPoint = isXNumeric && isYNumeric;
                     if (!isXNumeric && !useIndexAsX) nonNumericXCount++; // Only count if specific X field fails
                     if (!isYNumeric) nonNumericYCount++;
                 } else if (selectedChartType === 'bar') {
                     isValidPoint = isYNumeric;
                     if (!isYNumeric) nonNumericYCount++;
                 } else if (selectedChartType === 'bubble') {
                     const rawSizeValue = getValue(item, sizeField);
                     const numericSizeValue = Number(rawSizeValue);
                     const isSizeNumericPositive = !isNaN(numericSizeValue) && numericSizeValue > 0;
                     isValidPoint = isXNumeric && isYNumeric && isSizeNumericPositive;
                     if (!isXNumeric && !useIndexAsX) nonNumericXCount++;
                     if (!isYNumeric) nonNumericYCount++;
                     if (!isSizeNumericPositive) nonNumericSizeCount++;
                 }

                 if (isValidPoint) {
                     if (colorField) {
                         colorValue = getValue(item, colorField);
                         pointColor = getColorForValue(colorValue);
                     } else {
                         pointColor = predefinedColors[0];
                     }

                     // Common data parts
                     const commonData = {
                         ticker: item.ticker || 'N/A', // Use ticker if available
                         colorValue: colorValue,
                         originalY: rawYValue
                     };

                     if (selectedChartType === 'scatter' || selectedChartType === 'line') {
                         plotData.push({
                             ...commonData,
                             x: numericXValue,
                             y: numericYValue,
                             originalX: rawXValue
                         });
                     } else if (selectedChartType === 'bar') {
                         labels.push(commonData.ticker || `Index ${index}`); // Use ticker or index for label
                         numericData.push(numericYValue);
                         // Store color value temporarily for bar chart color assignment later
                         plotData.push({ ...commonData }); // Push minimal data needed for color lookup
                     } else if (selectedChartType === 'bubble') {
                         const rawSizeValue = getValue(item, sizeField);
                         const numericSizeValue = Number(rawSizeValue);
                         const minRadius = 5; const maxRadius = 30;
                         let radius = minRadius;
                         if (maxSize > minSize) {
                             radius = minRadius + ((numericSizeValue - minSize) / (maxSize - minSize)) * (maxRadius - minRadius);
                         }
                         radius = Math.max(minRadius, radius);
                         plotData.push({
                             ...commonData,
                             x: numericXValue,
                             y: numericYValue,
                             r: radius,
                             originalX: rawXValue,
                             originalSize: rawSizeValue
                         });
                     }

                     // Push colors (only needed for scatter/line/bubble here)
                      if (selectedChartType !== 'bar') {
                          pointBackgroundColors.push(pointColor);
                          pointBorderColors.push(pointColor.replace(/0\.\d+\)/, '1)')); // Make border opaque
                      }
                 }
             } else {
                  missingItemCount++;
             }
        });

        // --- Handle Bar Chart Colors ---
          if (selectedChartType === 'bar') {
              // Assign colors based on the temporary plotData (which holds colorValue)
              plotData.forEach((pd, index) => {
                  let barColor = defaultColor;
                  if (colorField) {
                      barColor = getColorForValue(pd.colorValue);
                  } else {
                      barColor = predefinedColors[0]; // Use default if no color field
                  }
                  pointBackgroundColors.push(barColor);
                  pointBorderColors.push(barColor.replace(/0\.\d+\)/, '1)'));
              });
              // Clear the temporary plotData for bar charts as it's not used directly in the chart config
              plotData = [];
          }
         // --- End Bar Chart Colors ---

        // Check if any valid data points were found
        const hasData = (selectedChartType === 'bar' ? numericData.length > 0 : plotData.length > 0);
        if (!hasData) {
            chartStatus.textContent = `No valid numeric data found for the selected axes.`;
             if (reportChartInstance) { reportChartInstance.destroy(); reportChartInstance = null; }
            return;
        }

        // --- Construct status message about excluded points ---
        let excludedMessages = [];
        if (missingItemCount > 0) excludedMessages.push(`${missingItemCount} missing records`);
        if (!useIndexAsX && (selectedChartType === 'scatter' || selectedChartType === 'line' || selectedChartType === 'bubble') && nonNumericXCount > 0) {
            excludedMessages.push(`${nonNumericXCount} invalid X ('${selectedXField}')`);
        }
        if (nonNumericYCount > 0) {
            excludedMessages.push(`${nonNumericYCount} invalid Y ('${selectedYField}')`);
        }
        if (selectedChartType === 'bubble' && nonNumericSizeCount > 0) {
            excludedMessages.push(`${nonNumericSizeCount} invalid Size ('${sizeField}')`);
        }
        const plottedCount = hasData ? (selectedChartType === 'bar' ? numericData.length : plotData.length) : 0;
        if (excludedMessages.length > 0) {
            chartStatus.textContent = `Plotting ${plottedCount} points. Excluded: ${excludedMessages.join(', ')}.`;
        } else {
            chartStatus.textContent = `Plotting ${plottedCount} points.`;
        }

        // Destroy previous chart instance
        if (reportChartInstance) {
            reportChartInstance.destroy();
            reportChartInstance = null; // Ensure it's nullified
        }

        // --- Determine data structure and axis config based on chart type ---
        let chartDataConfig;
        let xAxisConfig;

        if (selectedChartType === 'scatter' || selectedChartType === 'line' || selectedChartType === 'bubble') {
             chartDataConfig = {
                 datasets: [{
                     label: `${selectedYField} vs ${useIndexAsX ? 'Index' : selectedXField}`,
                     data: plotData, // Use {x, y, ...} data
                     backgroundColor: pointBackgroundColors,
                     borderColor: pointBorderColors,
                     pointRadius: selectedChartType === 'scatter' ? 5 : (selectedChartType === 'bubble' ? undefined : 3), // Bubble radius set in data
                     pointHoverRadius: selectedChartType === 'scatter' ? 7 : (selectedChartType === 'bubble' ? undefined : 5),
                     borderWidth: selectedChartType === 'line' ? 2 : 1,
                     fill: selectedChartType === 'line' ? false : undefined,
                     tension: selectedChartType === 'line' ? 0.1 : undefined
                 }]
             };
             xAxisConfig = {
                  title: { display: true, text: useIndexAsX ? 'Record Index' : selectedXField },
                  type: 'linear', position: 'bottom'
             };
         } else if (selectedChartType === 'bar') {
             chartDataConfig = {
                 labels: labels,
                 datasets: [{
                     label: selectedYField,
                     data: numericData,
                     backgroundColor: pointBackgroundColors,
                     borderColor: pointBorderColors,
                     borderWidth: 1
                 }]
             };
             xAxisConfig = {
                  title: { display: true, text: 'Ticker / Record' },
                  type: 'category', position: 'bottom'
             };
         } else {
              console.error(`[Charting] Unsupported chart type: ${selectedChartType}`);
              chartStatus.textContent = `Unsupported chart type selected: ${selectedChartType}`;
              return;
         }

        // --- Create the chart ---
         reportChartInstance = new Chart(ctx, {
             type: selectedChartType,
             data: chartDataConfig,
             options: {
                 responsive: true,
                 maintainAspectRatio: false,
                 scales: {
                     x: xAxisConfig,
                     y: {
                         ticks: {
                             callback: function(value, index, ticks) {
                                 const yField = selectedYField; // Use captured variable
                                 // Prioritize post-transform format for Y axis ticks
                                 const format = (yField && postTransformNumericFormats.hasOwnProperty(yField))
                                                  ? postTransformNumericFormats[yField]
                                                  : ((yField && preTransformNumericFormats[yField]) ? preTransformNumericFormats[yField] : 'default');
                                 return formatNumericValue(value, format);
                             }
                         },
                         title: { display: true, text: selectedYField }
                     }
                 },
                 plugins: {
                     legend: { display: true, position: 'top' },
                     tooltip: {
                         callbacks: {
                             label: function(context) {
                                 let labelLines = [];
                                 const chartType = context.chart.config.type;
                                 const pointData = context.raw;
                                 const yField = selectedYField;
                                 const xField = selectedXField;
                                 const useIndexAsX = (!xField || xField === 'index');
                                 const colorField = reportColorSelector.value;
                                 const sizeField = reportSizeSelector.value;

                                 if (chartType === 'bar') {
                                     labelLines.push(`Ticker: ${context.label || 'N/A'}`);
                                     const yFormat = (yField && postTransformNumericFormats.hasOwnProperty(yField))
                                                       ? postTransformNumericFormats[yField]
                                                       : ((yField && preTransformNumericFormats[yField]) ? preTransformNumericFormats[yField] : 'default');
                                     const formattedY = formatNumericValue(context.parsed.y, yFormat);
                                     labelLines.push(`${yField || 'Value'}: ${formattedY} (Raw: ${context.parsed.y})`);
                                     // Find original color value for bar tooltips
                                     if (colorField) {
                                         const dataIndex = context.dataIndex;
                                         // This lookup might be fragile if labels aren't unique/stable
                                         const originalItem = finalDataForAnalysis.find((item, idx) =>
                                              (item && item.ticker === context.label) || (!item?.ticker && context.label === `Index ${idx}`)
                                         );
                                         if (originalItem) {
                                             const colorValue = getValue(originalItem, colorField);
                                             if (colorValue !== null && colorValue !== undefined) {
                                                  labelLines.push(`${colorField}: ${colorValue}`);
                                             }
                                         }
                                     }
                                 } else if (pointData) { // Scatter/Line/Bubble
                                     labelLines.push(`Ticker: ${pointData.ticker || 'N/A'}`);
                                     const xLabel = useIndexAsX ? 'Index' : xField;
                                     const yLabel = yField;
                                     const xFormat = (xField && postTransformNumericFormats.hasOwnProperty(xField))
                                                       ? postTransformNumericFormats[xField]
                                                       : ((xField && preTransformNumericFormats[xField]) ? preTransformNumericFormats[xField] : 'default');
                                     const formattedX = useIndexAsX ? pointData.x : formatNumericValue(pointData.x, xFormat);
                                     const yFormatTooltip = (yLabel && postTransformNumericFormats.hasOwnProperty(yLabel))
                                                       ? postTransformNumericFormats[yLabel]
                                                       : ((yLabel && preTransformNumericFormats[yLabel]) ? preTransformNumericFormats[yLabel] : 'default');
                                     const formattedYTooltip = formatNumericValue(pointData.y, yFormatTooltip);
                                     labelLines.push(`${xLabel}: ${formattedX} ${!useIndexAsX && pointData.originalX != pointData.x ? '(Raw: '+pointData.originalX+')' : ''}`);
                                     labelLines.push(`${yLabel}: ${formattedYTooltip} ${pointData.originalY != pointData.y ? '(Raw: '+pointData.originalY+')' : ''}`);
                                     if (colorField && pointData.colorValue !== null && pointData.colorValue !== undefined) {
                                         labelLines.push(`${colorField}: ${pointData.colorValue}`);
                                     }
                                     if (chartType === 'bubble' && sizeField && pointData.originalSize !== null && pointData.originalSize !== undefined) {
                                         const sizeFormat = (sizeField && postTransformNumericFormats.hasOwnProperty(sizeField))
                                                           ? postTransformNumericFormats[sizeField]
                                                           : ((sizeField && preTransformNumericFormats[sizeField]) ? preTransformNumericFormats[sizeField] : 'default');
                                         const formattedSize = formatNumericValue(pointData.originalSize, sizeFormat);
                                         labelLines.push(`${sizeField} (Size): ${formattedSize}`);
                                     }
                                 } else {
                                      labelLines.push(`${context.dataset.label || 'Data'}: (${context.parsed.x}, ${context.parsed.y})`);
                                 }
                                 return labelLines;
                             }
                         }
                     }, // End tooltip
                     zoom: { // Keep zoom config
                         pan: { enabled: true, mode: 'xy', threshold: 5, overscroll: true },
                         zoom: {
                             wheel: { enabled: true },
                             pinch: { enabled: true },
                             drag: { enabled: true, modifierKey: 'shift' },
                             mode: 'xy',
                         }
                     }
                 } // End plugins
             } // End options
         }); // End new Chart

         // Force an update (removed in previous step, let's see if implicit update is enough)
         // if (reportChartInstance) { reportChartInstance.update(); }

         console.log(`[Charting] Chart rendered successfully as ${selectedChartType}.`);

         // <<< Assign final plot data for search functionality >>>
         // Important: Assign the correct data structure based on chart type
         if (selectedChartType === 'bar') {
              // For bar charts, search needs labels and original items (if possible)
              // Let's store an array of {label: string, originalItemIndex: number} for lookup
              currentChartPlotData = labels.map((lbl, idx) => ({
                  ticker: lbl, // Use 'ticker' property consistent with other types
                  // We might not have the original item easily here if labels aren't tickers
                  // For simplicity, search will only work on ticker labels for bar charts for now.
                  originalIndex: idx // Store original index if needed later
              }));
         } else {
              // For scatter, line, bubble, plotData has the {x, y, ticker,...} objects
              currentChartPlotData = plotData;
         }
         console.log(`[Charting] Updated currentChartPlotData for search with ${currentChartPlotData.length} points.`);

    } // --- END renderChart ---


    // --- Chart Highlighting Logic ---
    function resetChartHighlight() {
        // Check if chart instance or previous highlight state exists
        if (!reportChartInstance || highlightedPointIndex < 0 || !originalPointStyle) {
            return;
        }
        try {
            const dataset = reportChartInstance.data.datasets[0];
            if (!dataset) {
                console.warn("[Charting - resetHighlight] Dataset not found.");
                return;
            }

            // Restore all stored original styles
            const propsToRestore = Object.keys(originalPointStyle);
            propsToRestore.forEach(prop => {
                if (dataset.hasOwnProperty(prop)) { // Check if dataset still has the property
                    if (Array.isArray(dataset[prop])) {
                        // Ensure index is valid before restoring
                        if (highlightedPointIndex < dataset[prop].length) {
                            dataset[prop][highlightedPointIndex] = originalPointStyle[prop];
                        } else {
                             console.warn(`[Charting - resetHighlight] Index ${highlightedPointIndex} out of bounds for array ${prop}`);
                        }
                    } else {
                        // If the dataset property is NOT an array, restore the single value
                        dataset[prop] = originalPointStyle[prop];
                    }
                }
            });

            reportChartInstance.update('none'); // Update without animation
            console.log(`[Charting - resetHighlight] Reset highlight for index ${highlightedPointIndex}`);
        } catch (error) {
            console.error("[Charting - resetHighlight] Error resetting highlight:", error);
        } finally {
            // Clear state
            highlightedPointIndex = -1;
            originalPointStyle = null;
            if (chartSearchStatus) chartSearchStatus.textContent = ''; // Clear status message
        }
    }

    function highlightTickerOnChart(tickerToFind) {
        console.log("[Charting - highlight] Highlight function entered.");
        resetChartHighlight(); // Reset previous highlight first

        // Ensure chart and search status elements are available
        if (!reportChartInstance || !chartSearchStatus) {
            console.warn("[Charting - highlight] Chart instance or search status element missing.");
            return;
        }

        // Check data used for search is available
        if (!currentChartPlotData || currentChartPlotData.length === 0) {
            console.log("[Charting - highlight] Current chart plot data for search is empty.");
            chartSearchStatus.textContent = 'Chart data not ready for search.';
            return;
        }

        const chartType = reportChartInstance.config.type;
        // Allow highlighting on all supported types (bar, line, scatter, bubble)
        const supportedTypes = ['bar', 'scatter', 'line', 'bubble'];
        if (!supportedTypes.includes(chartType)) {
             chartSearchStatus.textContent = 'Highlighting not supported for this chart type.';
             console.warn(`[Charting - highlight] Highlighting not supported for type: ${chartType}`);
             return;
        }

        const upperTicker = tickerToFind.toUpperCase().trim();
        if (!upperTicker) {
            chartSearchStatus.textContent = ''; // Clear status if search is empty
            return; // Exit if search is empty
        }

        // --- Find Index (uses currentChartPlotData populated by renderChart) ---
        const foundIndex = currentChartPlotData.findIndex(p => p && p.ticker && p.ticker.toUpperCase() === upperTicker);

        if (foundIndex > -1) {
            try {
                const dataset = reportChartInstance.data.datasets[0];
                if (!dataset) throw new Error("Dataset not found");

                const dataLength = dataset.data?.length ?? 0;
                if (dataLength === 0) throw new Error("Dataset has no data points");
                if (foundIndex >= dataLength) throw new Error(`Found index ${foundIndex} is out of bounds for data length ${dataLength}`);


                // --- Store Original Styles ---
                originalPointStyle = {};
                const propsToStoreAndModify = ['backgroundColor', 'borderColor', 'borderWidth', 'radius', 'pointStyle'];

                const getOriginalValue = (prop) => {
                    if (!dataset.hasOwnProperty(prop)) return undefined;
                    const propValue = dataset[prop];
                    return Array.isArray(propValue) ? (propValue[foundIndex] ?? undefined) : propValue;
                };

                propsToStoreAndModify.forEach(prop => { originalPointStyle[prop] = getOriginalValue(prop); });

                // Define fallbacks (adjust for bar charts which don't use radius/pointStyle)
                const fallbacks = {
                    backgroundColor: 'rgba(128,128,128,0.1)',
                    borderColor: 'rgba(128,128,128,1)',
                    borderWidth: 1,
                    radius: (chartType === 'bar') ? undefined : 3,
                    pointStyle: (chartType === 'bar') ? undefined : 'circle'
                };
                for (const prop in fallbacks) {
                    if (originalPointStyle[prop] === undefined) originalPointStyle[prop] = fallbacks[prop];
                }
                Object.keys(originalPointStyle).forEach(key => { if (originalPointStyle[key] === undefined) delete originalPointStyle[key]; });

                console.log("[Charting - highlight] Stored original styles:", JSON.parse(JSON.stringify(originalPointStyle)));

                // --- Apply Highlight Styles ---
                const HIGHLIGHT_BORDER_WIDTH = 4;
                const HIGHLIGHT_RADIUS_INCREASE = 6;
                const HIGHLIGHT_POINT_STYLE = 'star';

                let newRadius = originalPointStyle.radius;
                if (typeof originalPointStyle.radius === 'number') {
                     newRadius = originalPointStyle.radius + HIGHLIGHT_RADIUS_INCREASE;
                }

                // Define the styles to be applied
                const highlightStyles = {
                    // backgroundColor: HIGHLIGHT_COLOR, // Example if background change needed
                    borderColor: HIGHLIGHT_COLOR,
                    borderWidth: HIGHLIGHT_BORDER_WIDTH,
                };
                // Only add radius/pointStyle if applicable for the chart type AND originals existed
                if (chartType !== 'bar' && originalPointStyle.radius !== undefined) {
                    highlightStyles.radius = newRadius;
                }
                 if (chartType !== 'bar' && chartType !== 'bubble' && originalPointStyle.pointStyle !== undefined) {
                    highlightStyles.pointStyle = HIGHLIGHT_POINT_STYLE;
                }


                // Helper to ensure property is an array and set the value
                const ensureArrayAndSet = (prop, valueToSet) => {
                    // Skip if highlight style doesn't include this property
                    if (!highlightStyles.hasOwnProperty(prop)) return;

                    // Check if property exists in dataset or original styles
                    if (!dataset.hasOwnProperty(prop) && !originalPointStyle.hasOwnProperty(prop)) {
                         console.warn(`[Charting - highlight] Skipping property '${prop}' as it doesn't exist.`);
                         return;
                    }

                    let currentArray = dataset[prop];
                    const original = originalPointStyle[prop]; // Use stored original

                    // Ensure it's an array of the correct length
                    if (!Array.isArray(currentArray)) {
                        console.log(`[Charting - highlight] Converting '${prop}' to array.`);
                        currentArray = new Array(dataLength).fill(original);
                        dataset[prop] = currentArray;
                    } else if (currentArray.length < dataLength) {
                         console.warn(`[Charting - highlight] Array for '${prop}' shorter than data. Extending...`);
                         const fillValue = currentArray[0] !== undefined ? currentArray[0] : original;
                         while(currentArray.length < dataLength) currentArray.push(fillValue);
                    }

                    // Set the value
                    if (foundIndex < currentArray.length) {
                        currentArray[foundIndex] = valueToSet;
                        console.log(`[Charting - highlight] Set ${prop}[${foundIndex}] = ${valueToSet}`);
                    } else {
                         console.error(`[Charting - highlight] Index ${foundIndex} out of bounds for '${prop}' array (len ${currentArray.length})`);
                    }
                };

                // Apply the styles using the helper for all defined highlightStyles
                for (const prop in highlightStyles) {
                   ensureArrayAndSet(prop, highlightStyles[prop]);
                }

                highlightedPointIndex = foundIndex; // Store the highlighted index
                reportChartInstance.update(); // Update with animation (more forceful redraw)

                 // Log dataset state AFTER update call for debugging
                 console.log("[Charting - highlight] Dataset state AFTER update():", JSON.parse(JSON.stringify(reportChartInstance.data.datasets[0])));


                chartSearchStatus.textContent = `Ticker ${tickerToFind.toUpperCase()} highlighted.`;
                console.log(`[Charting - highlight] Highlight applied to index ${foundIndex} for ticker ${tickerToFind}`);

            } catch (error) {
                console.error(`[Charting - highlight] Error applying highlight for ${tickerToFind}:`, error);
                chartSearchStatus.textContent = `Error highlighting ${tickerToFind}.`;
                // Reset state on error
                highlightedPointIndex = -1;
                originalPointStyle = null;
            }
        } else {
            chartSearchStatus.textContent = `Ticker ${tickerToFind.toUpperCase()} not found in chart data.`;
            console.log(`[Charting - highlight] Ticker ${tickerToFind} not found in currentChartPlotData.`);
        }
    } // --- END highlightTickerOnChart ---


    // --- Initialization Function ---
    function initChart() {
        console.log("[Charting] Initializing chart module...");

        // --- Get DOM Element References ---
        reportFieldSelector = document.getElementById('report-field-selector');
        reportXAxisSelector = document.getElementById('report-x-axis-selector');
        reportChartCanvas = document.getElementById('report-chart-canvas');
        chartStatus = document.getElementById('chart-status');
        reportColorSelector = document.getElementById('report-color-selector');
        reportChartTypeSelector = document.getElementById('report-chart-type-selector');
        reportSizeSelector = document.getElementById('report-size-selector');
        resetChartBtn = document.getElementById('reset-chart-btn');
        swapAxesBtn = document.getElementById('swap-axes-btn');
        chartTickerSearchInput = document.getElementById('chart-ticker-search');
        chartSearchStatus = document.getElementById('chart-search-status'); // Get search status element

        // --- Check if elements exist ---
        if (!reportFieldSelector || !reportXAxisSelector || !reportChartCanvas || !chartStatus || !reportColorSelector || !reportChartTypeSelector || !reportSizeSelector || !resetChartBtn || !swapAxesBtn || !chartTickerSearchInput || !chartSearchStatus) {
            console.error("[Charting] One or more required chart DOM elements not found. Initialization failed.");
            return;
        }
        console.log("[Charting] All DOM elements found.");

        // --- Register Chart.js Plugins ---
        if (window.ChartZoom) {
            try {
                // Check if already registered? Chart.js might handle this.
                 Chart.register(window.ChartZoom);
                 console.log("[Charting] Chartjs zoom plugin registered.");
            } catch (e) {
                console.error("[Charting] Error registering ChartZoom plugin:", e);
            }
        } else {
            console.warn("[Charting] Chartjs zoom plugin (ChartZoom) not found. Zoom/pan disabled.");
        }

        // --- Attach Event Listeners ---
        console.log("[Charting] Attaching event listeners...");
        reportFieldSelector.addEventListener('change', renderChart);
        reportXAxisSelector.addEventListener('change', renderChart);
        reportColorSelector.addEventListener('change', renderChart);
        reportChartTypeSelector.addEventListener('change', renderChart);
        reportSizeSelector.addEventListener('change', renderChart);

        resetChartBtn.addEventListener('click', () => {
            console.log("[Charting] Reset View button clicked");
            if (reportChartInstance) {
                try {
                    reportChartInstance.resetZoom();
                    console.log("[Charting] Chart zoom/pan reset.");
                } catch (e) {
                     console.error("[Charting] Error resetting zoom:", e);
                }
            } else {
                console.log("[Charting] No chart instance found to reset zoom.");
            }
        });

        swapAxesBtn.addEventListener('click', () => {
            console.log("[Charting] Swap Axes button clicked");
            const currentX = reportXAxisSelector.value;
            const currentY = reportFieldSelector.value;
            if (!currentY) { console.log("[Charting - Swap] Y-axis not selected."); return; }
            const yOptionInX = Array.from(reportXAxisSelector.options).some(opt => opt.value === currentY);
            if (!yOptionInX) { console.log(`[Charting - Swap] Y value '${currentY}' not in X options.`); return; }
            let newX = currentY; let newY = currentX;
            if (currentX === 'index') {
                newY = currentY;
                 console.log("[Charting - Swap] X is index. Swapping Y->X only.");
            } else {
                const xOptionInY = Array.from(reportFieldSelector.options).some(opt => opt.value === currentX);
                if (!xOptionInY) { console.log(`[Charting - Swap] X value '${currentX}' not in Y options.`); return; }
                 console.log("[Charting - Swap] Performing full swap.");
            }
            reportXAxisSelector.value = newX;
            reportFieldSelector.value = newY;
            renderChart(); // Re-render after swap
        });

        chartTickerSearchInput.addEventListener('input', () => {
            clearTimeout(chartSearchTimeout);
            const searchTerm = chartTickerSearchInput.value;
            console.log(`[Charting - Search] Input event: searchTerm = '${searchTerm}'`);
            chartSearchTimeout = setTimeout(() => {
                if (searchTerm.trim() === '') {
                    console.log("[Charting - Search] Search term empty, resetting highlight.");
                    resetChartHighlight();
                } else {
                    console.log(`[Charting - Search] Debounced search, calling highlightTickerOnChart('${searchTerm}')`);
                    highlightTickerOnChart(searchTerm);
                }
            }, 500); // Debounce
        });

        console.log("[Charting] Event listeners attached.");
        console.log("[Charting] Initialization complete.");
    }

    // --- Expose Public Methods ---
    window.AnalyticsChartingModule = {
        initChart: initChart,
        renderChart: renderChart
        // Add other functions here if needed (e.g., explicitly triggering highlight)
    };

    console.log("AnalyticsChartingModule loaded and exposed.");

})(); // End IIFE 