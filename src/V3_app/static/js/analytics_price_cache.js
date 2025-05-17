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

        let newRangeStartEpoch = standardizedNewData.length > 0 ? standardizedNewData[0].dateEpoch : (requestedStartDateStr ? dateStringToUtcEpoch(requestedStartDateStr) : -Infinity) ;
        let newRangeEndEpoch = standardizedNewData.length > 0 ? standardizedNewData[standardizedNewData.length - 1].dateEpoch : (requestedEndDateStr ? dateStringToUtcEpoch(requestedEndDateStr) : Infinity);
        let newPeriodType = requestedPeriod === 'max' ? 'max' : 'specific';

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

        if (requestedPeriod && requestedPeriod !== 'custom' && requestedPeriod !== 'max') {
            const range = calculateDateRangeFromPeriod(requestedPeriod);
            targetStartEpoch = range.startEpoch;
            targetEndEpoch = range.endEpoch;
        } else if (requestedPeriod === 'custom' && requestedStartDateStr && requestedEndDateStr) {
            targetStartEpoch = dateStringToUtcEpoch(requestedStartDateStr);
            targetEndEpoch = dateStringToUtcEpoch(requestedEndDateStr);
        } else if (requestedPeriod === 'max') {
            // For a 'max' request, if the cache has *any* data and its type is 'max', return it.
            // If its type is 'specific' but it covers a very large range, one might also consider it a hit.
            // For simplicity now: if 'max' is requested and cache has 'max' type, it's a hit.
            // Otherwise, it's a miss for 'max' if the cached data isn't marked as 'max' period type.
            // This encourages re-fetching with 'period=max' if a more complete dataset is desired by the user.
            if (fetchedRange.periodType === 'max') {
                console.log(LOG_PREFIX, `Cache hit for ${cacheKey} (requested 'max', cached 'max'). Returning all ${cachedData.length} points.`);
                return [...cachedData]; // Return a copy
            } else {
                console.log(LOG_PREFIX, `Cache partial miss for ${cacheKey} (requested 'max', but cached is 'specific' range: ${epochToDateString(fetchedRange.startEpoch)} to ${epochToDateString(fetchedRange.endEpoch)}). Fetching fresh 'max'.`);
                return null; // Treat as miss to encourage a true 'max' fetch
            }
        } else {
            console.warn(LOG_PREFIX, `Invalid parameters for getPriceData for ${cacheKey}:`, {requestedPeriod, requestedStartDateStr, requestedEndDateStr});
            return null; // Not enough info or invalid combo
        }

        if (typeof targetStartEpoch !== 'number' || typeof targetEndEpoch !== 'number' || isNaN(targetStartEpoch) || isNaN(targetEndEpoch)) {
             console.warn(LOG_PREFIX, `Could not determine valid target date range for ${cacheKey} from request.`, {targetStartEpoch, targetEndEpoch});
            return null;
        }
        
        // Check for full containment for specific date ranges
        if (fetchedRange.startEpoch <= targetStartEpoch && fetchedRange.endEpoch >= targetEndEpoch) {
            const resultData = cachedData.filter(p => p.dateEpoch >= targetStartEpoch && p.dateEpoch <= targetEndEpoch);
            if (resultData.length > 0) {
                console.log(LOG_PREFIX, `Cache hit for ${cacheKey}. Requested specific range: ${epochToDateString(targetStartEpoch)}-${epochToDateString(targetEndEpoch)}. Cached range: ${epochToDateString(fetchedRange.startEpoch)}-${epochToDateString(fetchedRange.endEpoch)}. Returning ${resultData.length} points.`);
                return [...resultData]; // Return a copy
            } else {
                 console.log(LOG_PREFIX, `Cache hit for ${cacheKey} (range covered), but no data points for specific sub-range ${epochToDateString(targetStartEpoch)}-${epochToDateString(targetEndEpoch)}.`);
                return []; // Covered, but no data in this exact slice
            }
        }
        
        console.log(LOG_PREFIX, `Cache miss for ${cacheKey}. Requested specific range ${epochToDateString(targetStartEpoch)}-${epochToDateString(targetEndEpoch)} not fully within cached range ${epochToDateString(fetchedRange.startEpoch)}-${epochToDateString(fetchedRange.endEpoch)}.`);
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