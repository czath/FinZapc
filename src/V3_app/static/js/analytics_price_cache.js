(function() {
    "use strict";
    const LOG_PREFIX = "[PriceCacheModule]";
    const priceCache = new Map(); // Key: `${ticker}_${interval}`
                                  // Value: {
                                  //    data: [{dateEpoch, Close, Open, High, Low, Volume, ...originalFields}], // Sorted by dateEpoch
                                  //    fetchedRange: {startEpoch, endEpoch, periodType: 'max' | 'specific', originalRequestParams: {requestedPeriod, requestedStartDateStr, requestedEndDateStr}}
                                  // }

    function dateStringToUtcEpoch(dateStr) {
        if (!dateStr) return null;
        const date = new Date(dateStr);
        // Ensure the date string is interpreted as UTC if it's just YYYY-MM-DD
        // by splitting and using Date.UTC
        const parts = dateStr.split('-');
        if (parts.length === 3) {
            const year = parseInt(parts[0], 10);
            const month = parseInt(parts[1], 10) - 1; // Month is 0-indexed
            const day = parseInt(parts[2], 10);
            if (!isNaN(year) && !isNaN(month) && !isNaN(day)) {
                return Date.UTC(year, month, day);
            }
        }
        // Fallback for other date formats, though YYYY-MM-DD is expected
        return date.getTime(); // This might use local timezone if not careful, prefer Date.UTC
    }

    function epochToDateString(epoch) {
        if (typeof epoch !== 'number') return 'N/A';
        return new Date(epoch).toISOString().split('T')[0];
    }

    function calculateDateRangeFromPeriod(period) {
        const today = new Date();
        // Set today to UTC midnight to ensure consistent date comparisons
        const todayEpoch = Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate());
        let sDateObj = new Date(todayEpoch); // Start with today UTC midnight

        switch (period) {
            case '1d': sDateObj.setUTCDate(sDateObj.getUTCDate() - 1); break;
            case '5d': sDateObj.setUTCDate(sDateObj.getUTCDate() - 5); break;
            case '1mo': sDateObj.setUTCMonth(sDateObj.getUTCMonth() - 1); break;
            case '3mo': sDateObj.setUTCMonth(sDateObj.getUTCMonth() - 3); break;
            case '6mo': sDateObj.setUTCMonth(sDateObj.getUTCMonth() - 6); break;
            case 'ytd': sDateObj = new Date(Date.UTC(today.getUTCFullYear(), 0, 1)); break;
            case '1y': sDateObj.setUTCFullYear(sDateObj.getUTCFullYear() - 1); break;
            case '2y': sDateObj.setUTCFullYear(sDateObj.getUTCFullYear() - 2); break;
            case '5y': sDateObj.setUTCFullYear(sDateObj.getUTCFullYear() - 5); break;
            case '10y': sDateObj.setUTCFullYear(sDateObj.getUTCFullYear() - 10); break;
            case 'max': return { startEpoch: -Infinity, endEpoch: Infinity }; // Special case for 'max'
            default: // Default to 1 year if unknown period
                console.warn(LOG_PREFIX, `Unknown period '${period}', defaulting to 1 year for cache range calculation.`);
                sDateObj.setUTCFullYear(sDateObj.getUTCFullYear() - 1);
                break;
        }
        return { startEpoch: sDateObj.getTime(), endEpoch: todayEpoch };
    }

    function storePriceData(ticker, interval, newDataPoints, requestedPeriod, requestedStartDateStr, requestedEndDateStr) {
        const cacheKey = `${ticker}_${interval}`;
        if (!ticker || !interval || !newDataPoints) {
            console.error(LOG_PREFIX, "storePriceData called with invalid arguments.", {ticker, interval, newDataPoints});
            return;
        }
        let existingEntry = priceCache.get(cacheKey);

        const standardizedNewData = newDataPoints.map(p => ({
            ...p,
            dateEpoch: new Date(p.Datetime || p.Date).getTime() // Assuming Datetime or Date is a string parsable by Date constructor
        })).sort((a, b) => a.dateEpoch - b.dateEpoch);

        if (standardizedNewData.length === 0) {
            console.log(LOG_PREFIX, `No data points to store for ${cacheKey}.`);
            // If an existing entry for 'max' had no data, and new data comes for 'max', still create/update it.
            // But if new data is empty and no prior 'max' entry, don't create an empty shell.
            if (!existingEntry && requestedPeriod !== 'max') return;
        }

        let newRangeStartEpoch, newRangeEndEpoch;
        let newPeriodType = requestedPeriod === 'max' ? 'max' : 'specific';

        if (requestedPeriod === 'custom' && requestedStartDateStr && requestedEndDateStr) {
            // For 'custom' periods, use the exact requested dates for the cache entry's range metadata
            newRangeStartEpoch = dateStringToUtcEpoch(requestedStartDateStr);
            newRangeEndEpoch = dateStringToUtcEpoch(requestedEndDateStr);
            newPeriodType = 'specific'; // Ensure it's specific
        } else if (requestedPeriod && requestedPeriod !== 'custom' && requestedPeriod !== 'max') { // Handles 'ytd', '1mo', etc.
            // For predefined periods, use the calculated range for that period as the cache metadata
            const calculatedRange = calculateDateRangeFromPeriod(requestedPeriod);
            newRangeStartEpoch = calculatedRange.startEpoch;
            newRangeEndEpoch = calculatedRange.endEpoch;
            newPeriodType = 'specific'; // These are specific period requests

            // Optional: Log if API data is unexpectedly outside the calculated period range
            if (standardizedNewData.length > 0) {
                const actualDataStart = standardizedNewData[0].dateEpoch;
                const actualDataEnd = standardizedNewData[standardizedNewData.length - 1].dateEpoch;
                // Check if actual data START is AFTER calculated period start OR actual data END is BEFORE calculated period end
                // This indicates the API provided less data than the period implies.
                // Or if actual data START is BEFORE calculated start AND actual data END is AFTER calculated end
                // This indicates API provided more data.
                // For caching purposes, we trust our calculated period for metadata.
                if (actualDataStart > newRangeStartEpoch || actualDataEnd < newRangeEndEpoch) {
                    // This log can be quite verbose if APIs often return slightly different ranges.
                    // console.warn(LOG_PREFIX, `For period '${requestedPeriod}', API data range (${epochToDateString(actualDataStart)}-${epochToDateString(actualDataEnd)}) does not perfectly match calculated period range (${epochToDateString(newRangeStartEpoch)}-${epochToDateString(newRangeEndEpoch)}). Cache metadata uses calculated range.`);
                }
            }
        } else if (standardizedNewData.length > 0) { 
            // This branch now primarily handles 'max' requests if data exists,
            // or if requestedPeriod was undefined/null but data was still fetched (less likely for current call patterns).
            newRangeStartEpoch = standardizedNewData[0].dateEpoch;
            newRangeEndEpoch = standardizedNewData[standardizedNewData.length - 1].dateEpoch;
            // newPeriodType is already correctly 'max' if requestedPeriod was 'max', or 'specific' by default.
        } else { 
            // Fallback if no data points AND not a 'custom' or predefined period that sets its own dates.
            // This usually means it was a 'max' request that returned no data, or an invalid request.
            newRangeStartEpoch = (requestedPeriod === 'max') ? -Infinity : (requestedStartDateStr ? dateStringToUtcEpoch(requestedStartDateStr) : -Infinity);
            newRangeEndEpoch = (requestedPeriod === 'max') ? Infinity : (requestedEndDateStr ? dateStringToUtcEpoch(requestedEndDateStr) : Infinity);
            if (requestedPeriod !== 'max' && (!requestedStartDateStr || !requestedEndDateStr)) {
                 newPeriodType = 'specific'; // Fallback for empty data on specific but incomplete requests
            }
        }
        
        // Ensure newRangeStartEpoch and newRangeEndEpoch are valid numbers, especially after dateStringToUtcEpoch
        if (typeof newRangeStartEpoch !== 'number' || isNaN(newRangeStartEpoch)) {
            newRangeStartEpoch = -Infinity; // Fallback for safety
            console.warn(LOG_PREFIX, `Invalid start epoch calculated for ${cacheKey}, defaulting to -Infinity. Original params:`, {requestedPeriod, requestedStartDateStr});
        }
        if (typeof newRangeEndEpoch !== 'number' || isNaN(newRangeEndEpoch)) {
            newRangeEndEpoch = Infinity; // Fallback for safety
             console.warn(LOG_PREFIX, `Invalid end epoch calculated for ${cacheKey}, defaulting to Infinity. Original params:`, {requestedPeriod, requestedEndDateStr});
        }

        // Store the parameters that led to this fetch for better 'max' handling later
        const originalRequestParams = { requestedPeriod, requestedStartDateStr, requestedEndDateStr };

        if (existingEntry) {
            const combinedDataMap = new Map(existingEntry.data.map(p => [p.dateEpoch, p]));
            standardizedNewData.forEach(p => combinedDataMap.set(p.dateEpoch, p));
            
            existingEntry.data = Array.from(combinedDataMap.values()).sort((a, b) => a.dateEpoch - b.dateEpoch);
            
            existingEntry.fetchedRange.startEpoch = Math.min(existingEntry.fetchedRange.startEpoch, newRangeStartEpoch);
            existingEntry.fetchedRange.endEpoch = Math.max(existingEntry.fetchedRange.endEpoch, newRangeEndEpoch);
            
            if (newPeriodType === 'max') {
                existingEntry.fetchedRange.periodType = 'max';
            }
            // If existing was 'max', it stays 'max'.
            // If existing was 'specific' and new is 'specific', it remains 'specific' but range might expand.
            // Keep track of original request parameters if it helps decide if a 'max' fetch needs to be re-done.
            existingEntry.fetchedRange.originalRequestParamsHistory = existingEntry.fetchedRange.originalRequestParamsHistory || [];
            existingEntry.fetchedRange.originalRequestParamsHistory.push(originalRequestParams);

        } else {
            existingEntry = { // Assign to existingEntry to use common log at the end
                data: standardizedNewData,
                fetchedRange: {
                    startEpoch: newRangeStartEpoch,
                    endEpoch: newRangeEndEpoch,
                    periodType: newPeriodType,
                    originalRequestParamsHistory: [originalRequestParams]
                }
            };
            priceCache.set(cacheKey, existingEntry);
        }
        console.log(LOG_PREFIX, `Data for ${cacheKey} stored/updated. Points: ${existingEntry.data.length}. Cached range: ${epochToDateString(existingEntry.fetchedRange.startEpoch)} to ${epochToDateString(existingEntry.fetchedRange.endEpoch)}, Type: ${existingEntry.fetchedRange.periodType}`);
    }

    function getPriceData(ticker, interval, requestedPeriod, requestedStartDateStr, requestedEndDateStr) {
        const cacheKey = `${ticker}_${interval}`;
        if (!ticker || !interval) {
            console.error(LOG_PREFIX, "getPriceData called with invalid ticker or interval.", {ticker, interval});
            return null;
        }
        const cachedEntry = priceCache.get(cacheKey);

        if (!cachedEntry) {
            console.log(LOG_PREFIX, `Cache miss for ${cacheKey}: No entry found.`);
            return null;
        }

        const { data: cachedData, fetchedRange } = cachedEntry;
        let targetStartEpoch, targetEndEpoch;

        // Determine the target date range for the current request
        if (requestedPeriod && requestedPeriod !== 'custom' && requestedPeriod !== 'max') { // e.g., 'ytd', '1mo', '2y'
            const range = calculateDateRangeFromPeriod(requestedPeriod);
            targetStartEpoch = range.startEpoch;
            targetEndEpoch = range.endEpoch;
        } else if (requestedPeriod === 'custom' && requestedStartDateStr && requestedEndDateStr) {
            targetStartEpoch = dateStringToUtcEpoch(requestedStartDateStr);
            targetEndEpoch = dateStringToUtcEpoch(requestedEndDateStr);
        } else if (requestedPeriod === 'max') {
            if (fetchedRange.periodType === 'max') {
                console.log(LOG_PREFIX, `Cache hit for ${cacheKey} (requested 'max', cached 'max'). Returning all ${cachedData.length} points.`);
                return [...cachedData];
            } else {
                console.log(LOG_PREFIX, `Cache partial miss for ${cacheKey} (requested 'max', but cached is 'specific' range: ${epochToDateString(fetchedRange.startEpoch)} to ${epochToDateString(fetchedRange.endEpoch)}). Fetching fresh 'max'.`);
                return null; // Miss: Encourage a true 'max' fetch if current cache isn't 'max' type
            }
        } else {
            console.warn(LOG_PREFIX, `Invalid parameters for getPriceData for ${cacheKey}:`, {requestedPeriod, requestedStartDateStr, requestedEndDateStr});
            return null;
        }

        // Ensure targetStartEpoch and targetEndEpoch are valid numbers after calculation/parsing
        if (typeof targetStartEpoch !== 'number' || typeof targetEndEpoch !== 'number' || isNaN(targetStartEpoch) || isNaN(targetEndEpoch)) {
             console.warn(LOG_PREFIX, `Could not determine valid target date range for ${cacheKey} from request.`, {targetStartEpoch, targetEndEpoch, requestedPeriod, requestedStartDateStr, requestedEndDateStr});
            return null;
        }
        
        // Now, check for containment based on the determined targetStartEpoch and targetEndEpoch
        let isRangeCovered = false;
        if (fetchedRange.periodType === 'max') {
            // If cache is 'max', it covers any request that starts on/after its beginning and starts before/on its end.
            // The filter will then correctly clip to the actual targetEndEpoch.
            isRangeCovered = fetchedRange.startEpoch <= targetStartEpoch && targetStartEpoch <= fetchedRange.endEpoch;
            if (isRangeCovered) {
                console.log(LOG_PREFIX, `Cache check for ${cacheKey}: Cached 'max' period (${epochToDateString(fetchedRange.startEpoch)}-${epochToDateString(fetchedRange.endEpoch)}) covers requested start ${epochToDateString(targetStartEpoch)}.`);
            } else {
                 console.log(LOG_PREFIX, `Cache check for ${cacheKey}: Cached 'max' period (${epochToDateString(fetchedRange.startEpoch)}-${epochToDateString(fetchedRange.endEpoch)}) does NOT cover requested start ${epochToDateString(targetStartEpoch)}.`);
            }
        } else { // fetchedRange.periodType is 'specific'
            isRangeCovered = fetchedRange.startEpoch <= targetStartEpoch && fetchedRange.endEpoch >= targetEndEpoch;
        }

        if (isRangeCovered) {
            const resultData = cachedData.filter(p => p.dateEpoch >= targetStartEpoch && p.dateEpoch <= targetEndEpoch);
            if (resultData.length > 0) {
                console.log(LOG_PREFIX, `Cache hit for ${cacheKey}. Requested range: ${epochToDateString(targetStartEpoch)}-${epochToDateString(targetEndEpoch)}. Cached type: ${fetchedRange.periodType}. Returning ${resultData.length} points.`);
                return [...resultData];
            } else {
                 console.log(LOG_PREFIX, `Cache hit for ${cacheKey} (range covered by ${fetchedRange.periodType}), but no data points for specific sub-range ${epochToDateString(targetStartEpoch)}-${epochToDateString(targetEndEpoch)}.`);
                return []; 
            }
        }
        
        console.log(LOG_PREFIX, `Cache miss for ${cacheKey}. Requested range ${epochToDateString(targetStartEpoch)}-${epochToDateString(targetEndEpoch)} not fully within cached range ${epochToDateString(fetchedRange.startEpoch)}-${epochToDateString(fetchedRange.endEpoch)} (Type: ${fetchedRange.periodType}).`);
        return null;
    }

    function clearCache() {
        priceCache.clear();
        console.log(LOG_PREFIX, "Price cache cleared.");
    }

    window.AnalyticsPriceCache = {
        storePriceData,
        getPriceData,
        clearCache,
        // Helper for debugging/inspection if needed
        _getCacheState: function() { return new Map(priceCache); }
    };
    console.log(LOG_PREFIX, "Price Cache Module loaded and attached to window.AnalyticsPriceCache.");

})(); 