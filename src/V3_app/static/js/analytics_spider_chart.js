// Placeholder for spider chart logic

document.addEventListener('DOMContentLoaded', function() {
    console.log("analytics_spider_chart.js loaded");

    // Get references to UI elements within the Cross-Field Visualization tab
    const spiderTabPane = document.getElementById('cross-field-viz-tab-pane');
    if (!spiderTabPane) {
        console.warn('Spider chart tab pane (#cross-field-viz-tab-pane) not found. Aborting script.');
        return;
    }

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
    let spiderChartInstance = null;
    let mainModule = null; // To store reference to AnalyticsMainModule

    // --- Helper: Median Calculation ---
    function calculateMedian(numericValues) {
        if (!numericValues || numericValues.length === 0) {
            return null;
        }
        const sortedValues = [...numericValues].sort((a, b) => a - b);
        const mid = Math.floor(sortedValues.length / 2);
        return sortedValues.length % 2 !== 0 ? sortedValues[mid] : (sortedValues[mid - 1] + sortedValues[mid]) / 2;
    }

    // --- Initialization Logic --- 
    function initializeSpiderChartModule() {
        console.log("Initializing Spider Chart Module...");
        mainModule = window.AnalyticsMainModule;
        if (!mainModule) {
            console.error("AnalyticsMainModule not found! Cannot populate spider chart selectors or data.");
            if(statusDiv) statusDiv.textContent = 'Error: Core analytics module not loaded.';
            return;
        }
        populateSelectors();
    }

    // Listener for when the tab becomes active
    if (crossFieldVizTabTrigger) {
        crossFieldVizTabTrigger.addEventListener('shown.bs.tab', function (event) {
            console.log('Cross-Field Visualization tab shown.');
            initializeSpiderChartModule(); 
        });
    } else {
        console.warn('Cross-Field Visualization tab trigger (#cross-field-viz-tab) not found.');
        // Attempt initialization immediately in case the tab is already active on load
        // but check if the pane is visible
        if (spiderTabPane && (spiderTabPane.classList.contains('show') || spiderTabPane.classList.contains('active'))) {
             initializeSpiderChartModule();
        }
    }

    // --- Function to populate selectors ---
    function populateSelectors() {
        console.log("Populating spider chart selectors...");
        if (!mainModule || !tickerSelect || !fieldSelect) {
            console.error("Cannot populate selectors: Main module or select elements missing.");
            return;
        }

        const finalData = mainModule.getFinalDataForAnalysis ? mainModule.getFinalDataForAnalysis() : [];
        const finalFields = mainModule.getFinalAvailableFields ? mainModule.getFinalAvailableFields() : [];
        const finalMetadata = mainModule.getFinalFieldMetadata ? mainModule.getFinalFieldMetadata() : {};

        // Populate Ticker Select
        tickerSelect.innerHTML = ''; // Clear existing
        if (finalData && finalData.length > 0) {
            const tickers = [...new Set(finalData.map(item => item?.ticker).filter(Boolean))].sort();
            tickers.forEach(ticker => {
                const option = document.createElement('option');
                option.value = ticker;
                option.textContent = ticker;
                tickerSelect.appendChild(option);
            });
        } else {
            tickerSelect.innerHTML = '<option disabled>No data loaded</option>';
        }

        // Populate Field Select (only numeric fields)
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
        console.log("Selectors populated.");
    }

    // --- Function to calculate aggregates ---
    function calculateAggregates(fullData, selectedFields) {
        console.log("Calculating aggregates for selected fields:", selectedFields);
        if (!fullData || fullData.length === 0 || !selectedFields || selectedFields.length === 0) {
            return { aggregates: {}, overallMinMax: {} };
        }

        const aggregates = {}; // { field: { min, max, sum, count, values } }
        const overallMinMax = {}; // { field: { min, max } }

        selectedFields.forEach(field => {
            aggregates[field] = { min: Infinity, max: -Infinity, sum: 0, count: 0, values: [] };
            overallMinMax[field] = { min: Infinity, max: -Infinity }; // Initialize for normalization

            fullData.forEach(item => {
                if (item && item.processed_data && item.processed_data.hasOwnProperty(field)) {
                    const value = item.processed_data[field];
                    const num = Number(value);
                    if (value !== null && value !== undefined && String(value).trim() !== '' && !isNaN(num)) {
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
            // Remove raw values if not needed anymore? Or keep for potential later use?
            // For now, keep .values if median was calculated
             if (!medianCheckbox.checked) {
                 delete aggregates[field].values; // Clean up if median wasn't requested (optional)
             }
        });

        console.log("Aggregates calculated:", aggregates);
        console.log("Overall Min/Max for Normalization:", overallMinMax);
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
        console.log("Rendering spider chart...");
        if (!canvas) {
            console.error("Spider chart canvas not found!");
            if(statusDiv) statusDiv.textContent = 'Error: Chart canvas element missing.';
            return;
        }
        const ctx = canvas.getContext('2d');
        if (spiderChartInstance) {
            spiderChartInstance.destroy();
            console.log("Previous spider chart instance destroyed.");
        }

        // Define color palette
        const colorPalette = [
            'rgba(54, 162, 235, 0.4)', // Blue
            'rgba(255, 99, 132, 0.4)',  // Red
            'rgba(75, 192, 192, 0.4)', // Green
            'rgba(255, 206, 86, 0.4)', // Yellow
            'rgba(153, 102, 255, 0.4)',// Purple
            'rgba(255, 159, 64, 0.4)', // Orange
            'rgba(201, 203, 207, 0.4)', // Grey
            'rgba(0, 0, 0, 0.4)'       // Black (for aggregates?)
        ]; 
        const borderPalette = colorPalette.map(color => color.replace('0.4', '1'));

        // Assign colors dynamically
        datasets.forEach((ds, index) => {
            ds.backgroundColor = colorPalette[index % colorPalette.length];
            ds.borderColor = borderPalette[index % borderPalette.length];
            ds.borderWidth = 1.5;
            ds.pointBackgroundColor = borderPalette[index % borderPalette.length];
            ds.pointBorderColor = '#fff';
            ds.pointHoverBackgroundColor = '#fff';
            ds.pointHoverBorderColor = borderPalette[index % borderPalette.length];
            ds.pointRadius = 3;
            ds.pointHoverRadius = 5;
        });

        spiderChartInstance = new Chart(ctx, {
            type: 'radar',
            data: {
                labels: chartLabels,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    r: { // Radial axis (the spokes)
                        suggestedMin: 0,
                        suggestedMax: 1, // Data is normalized to 0-1 range
                        ticks: {
                            display: false, // Hide the numeric scale on spokes 
                            stepSize: 0.2 // Keep for structure, but hide
                        },
                        pointLabels: { // Labels at the end of spokes (Field Names)
                            font: {
                                size: 11 // Adjust font size if needed
                            }
                        },
                        grid: {
                            color: 'rgba(128, 128, 128, 0.2)' // Lighter grid lines
                        },
                        angleLines: {
                             color: 'rgba(128, 128, 128, 0.2)' // Lighter angle lines
                        }
                    }
                },
                plugins: {
                    legend: {
                        position: 'top',
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const datasetLabel = context.dataset.label || '';
                                const fieldLabel = context.label || ''; // This is the axis/field label
                                const dataIndex = context.dataIndex;
                                const datasetIndex = context.datasetIndex;

                                // Retrieve the original (non-normalized) value
                                const originalValue = originalValuesMap[datasetLabel]?.[fieldLabel]; 

                                let valueString = 'N/A';
                                if (originalValue !== undefined && originalValue !== null) {
                                    // Attempt to format numeric values nicely
                                    if (typeof originalValue === 'number') {
                                        // Basic formatting, could be enhanced using mainModule.formatNumericValue if needed
                                        valueString = originalValue.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 2 });
                                    } else {
                                        valueString = String(originalValue);
                                    }
                                } 
                                // Only show the field label in the tooltip body if it's different from the axis label? No, always show.
                                return `${datasetLabel}: ${valueString}`; // Combine dataset label and original value
                            },
                            title: function(tooltipItems) {
                                // Use the field name (axis label) as the title
                                if (tooltipItems.length > 0) {
                                    return tooltipItems[0].label;
                                }
                                return '';
                            }
                        }
                    }
                }
            }
        });
        console.log("Spider chart rendered/updated.");
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
            const originalValuesMap = {}; // To store original values for tooltips { datasetLabel: { fieldLabel: originalValue } }

            // Add Ticker Datasets
            selectedTickers.forEach(ticker => {
                const item = tickerData.find(d => d.ticker === ticker);
                const normalizedValues = [];
                const originalTickerValues = {}; // Store original values for this ticker

                selectedFields.forEach(field => {
                    let originalValue = null;
                    if (item && item.processed_data && item.processed_data.hasOwnProperty(field)) {
                        originalValue = item.processed_data[field];
                    }
                    originalTickerValues[field] = originalValue;
                    const min = overallMinMax[field]?.min;
                    const max = overallMinMax[field]?.max;
                    normalizedValues.push(normalizeData(originalValue, min, max));
                });
                datasets.push({ label: ticker, data: normalizedValues });
                originalValuesMap[ticker] = originalTickerValues; // Store original values under ticker label
            });

            // Add Aggregate Datasets if selected
            if (includeAvg) {
                const aggValues = selectedFields.map(field => normalizeData(aggregates[field]?.avg, overallMinMax[field]?.min, overallMinMax[field]?.max));
                datasets.push({ label: 'Average', data: aggValues });
                originalValuesMap['Average'] = Object.fromEntries(selectedFields.map(f => [f, aggregates[f]?.avg]));
            }
            if (includeMin) {
                const aggValues = selectedFields.map(field => normalizeData(aggregates[field]?.min, overallMinMax[field]?.min, overallMinMax[field]?.max));
                datasets.push({ label: 'Minimum', data: aggValues });
                 originalValuesMap['Minimum'] = Object.fromEntries(selectedFields.map(f => [f, aggregates[f]?.min]));
            }
             if (includeMax) {
                const aggValues = selectedFields.map(field => normalizeData(aggregates[field]?.max, overallMinMax[field]?.min, overallMinMax[field]?.max));
                datasets.push({ label: 'Maximum', data: aggValues });
                originalValuesMap['Maximum'] = Object.fromEntries(selectedFields.map(f => [f, aggregates[f]?.max]));
            }
            if (includeMedian) {
                const aggValues = selectedFields.map(field => normalizeData(aggregates[field]?.median, overallMinMax[field]?.min, overallMinMax[field]?.max));
                datasets.push({ label: 'Median', data: aggValues });
                originalValuesMap['Median'] = Object.fromEntries(selectedFields.map(f => [f, aggregates[f]?.median]));
            }

            // 6. Call renderSpiderChart
            renderSpiderChart(selectedFields, datasets, originalValuesMap);
        });
    } else {
        console.error("Generate spider chart button not found!");
    }

}); 