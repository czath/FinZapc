// src/V3_app/static/js/analytics_ts_fund_adv.js
(function() {
    "use strict";
    const LOG_PREFIX_ADV = "[TimeseriesFundamentalsAdvModule]";

    // Define UI details for advanced synthetic studies
    // More studies can be added to this object later
    const advancedSyntheticStudiesUIDetails = {
        // Standard Synthetic Studies
        "EPS_TTM": {
            yAxisLabel: "EPS Value (TTM)",
            percentChangeLabel: "EPS % Change"
        },
        "PE_TTM": {
            yAxisLabel: "P/E Ratio (TTM)",
            percentChangeLabel: "P/E % Change"
        },
        "EARNINGS_YIELD_TTM": {
            yAxisLabel: "Earnings Yield % (TTM)",
            percentChangeLabel: "Earnings Yield % Change"
        },
        "OPERATING_CF_PER_SHARE_TTM": {
            yAxisLabel: "Operating CF/Share (TTM)",
            percentChangeLabel: "Op CF/Share % Change"
        },
        "FCF_PER_SHARE_TTM": {
            yAxisLabel: "FCF/Share (TTM)",
            percentChangeLabel: "FCF/Share % Change"
        },
        "CASH_PER_SHARE": {
            yAxisLabel: "Cash/Share",
            percentChangeLabel: "Cash/Share % Change"
        },
        "CASH_PLUS_ST_INV_PER_SHARE": {
            yAxisLabel: "Cash+ST Inv/Share",
            percentChangeLabel: "Cash+ST Inv/Share % Change"
        },
        "PRICE_TO_CASH_PLUS_ST_INV": {
            yAxisLabel: "Price/Cash+ST Inv",
            percentChangeLabel: "Price/Cash+ST Inv % Change"
        },
        "BOOK_VALUE_PER_SHARE": {
            yAxisLabel: "Book Value/Share",
            percentChangeLabel: "Book Value/Share % Change"
        },
        "PRICE_TO_BOOK_VALUE": {
            yAxisLabel: "Price/Book Value",
            percentChangeLabel: "P/B % Change"
        },
        "P_OPER_CF_TTM": {
            yAxisLabel: "Price/Operating CF (TTM)",
            percentChangeLabel: "P/Op CF % Change"
        },
        "P_FCF_TTM": {
            yAxisLabel: "Price/FCF (TTM)",
            percentChangeLabel: "P/FCF % Change"
        },

        // Advanced Synthetic Studies
        "FCF_MARGIN_TTM": {
            yAxisLabel: "FCF Margin % (TTM)",
            percentChangeLabel: "FCF Margin % Change"
        },
        "GROSS_MARGIN_TTM": { 
            yAxisLabel: "Gross Margin % (TTM)",
            percentChangeLabel: "Gross Margin % Change"
        },
        "OPERATING_MARGIN_TTM": {
            yAxisLabel: "Operating Margin % (TTM)",
            percentChangeLabel: "Operating Margin % Change"
        },
        "NET_PROFIT_MARGIN_TTM": {
            yAxisLabel: "Net Profit Margin % (TTM)",
            percentChangeLabel: "Net Profit Margin % Change"
        },
        "PRICE_TO_SALES_TTM": {
            yAxisLabel: "Price/Sales (TTM)",
            percentChangeLabel: "Price/Sales % Change"
        },
        "INTEREST_TO_INCOME_TTM": {
            yAxisLabel: "Interest/Income % (TTM)",
            percentChangeLabel: "Interest/Income % Change"
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

    window.TimeseriesFundamentalsAdvModule.getYAxisLabelForSyntheticFundamental = function(ratioName, displayMode) {
        const details = advancedSyntheticStudiesUIDetails[ratioName];
        if (details) {
            return displayMode === 'percent_change' ? details.percentChangeLabel : details.yAxisLabel;
        }
        return null; // Return null if the ratio is not found
    };

    window.TimeseriesFundamentalsAdvModule.getSyntheticStudyUIDetails = function(ratioName) {
        if (advancedSyntheticStudiesUIDetails.hasOwnProperty(ratioName)) {
            return advancedSyntheticStudiesUIDetails[ratioName];
        }
        return null; // Return null if the ratio is not found in this advanced module
    };

    console.log(LOG_PREFIX_ADV, "Advanced synthetic fundamentals module loaded and exposed.");
})(); 