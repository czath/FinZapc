// src/V3_app/static/js/analytics_ts_fund_adv.js
(function() {
    "use strict";
    const LOG_PREFIX_ADV = "[TimeseriesFundamentalsAdvModule]";

    // Define UI details for advanced synthetic studies
    // More studies can be added to this object later
    const advancedSyntheticStudiesUIDetails = {
        "FCF_MARGIN_TTM": {
            yAxisLabel: "FCF Margin % (TTM)"
            // Potentially other UI properties like chartType default, tooltip formatter, etc.
        },
        "GROSS_MARGIN_TTM": { 
            yAxisLabel: "Gross Margin % (TTM)"
        },
        "OPERATING_MARGIN_TTM": {
            yAxisLabel: "Operating Margin % (TTM)"
        },
        "NET_PROFIT_MARGIN_TTM": {
            yAxisLabel: "Net Profit Margin % (TTM)"
        }
        // Example for a future study:
        // "ANOTHER_ADV_RATIO": {
        //     yAxisLabel: "Another Ratio Value"
        // }
    };

    // Expose the module to the global window object
    if (!window.TimeseriesFundamentalsAdvModule) {
        window.TimeseriesFundamentalsAdvModule = {};
    }

    window.TimeseriesFundamentalsAdvModule.getSyntheticStudyUIDetails = function(ratioName) {
        if (advancedSyntheticStudiesUIDetails.hasOwnProperty(ratioName)) {
            return advancedSyntheticStudiesUIDetails[ratioName];
        }
        return null; // Return null if the ratio is not found in this advanced module
    };

    console.log(LOG_PREFIX_ADV, "Advanced synthetic fundamentals module loaded and exposed.");
})(); 