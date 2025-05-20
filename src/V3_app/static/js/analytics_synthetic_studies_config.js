// src/V3_app/static/js/analytics_synthetic_studies_config.js
(function() {
    "use strict";

    // Ensure TimeseriesFundamentalsAdvModule and its details are potentially available for yAxis mapping later if needed
    // but this list primarily drives the dropdown.
    // The text property includes icons for display.
    // The isAdvanced property can be used by the main script if specific handling is needed,
    // or to cross-reference with TimeseriesFundamentalsAdvModule.advancedSyntheticStudiesUIDetails for runtime properties like yAxisLabel.

    window.GlobalSyntheticStudiesList = [
        // Standard Synthetic Studies
        { value: "EPS_TTM", text: "🧱 Diluted EPS (TTM)" },
        { value: "PE_TTM", text: "📊 P/E (TTM)" },
        { value: "EARNINGS_YIELD_TTM", text: "📊 Earnings Yield (TTM)" },
        { value: "OPERATING_CF_PER_SHARE_TTM", text: "🧱 Operating CF/Share (TTM)" },
        { value: "FCF_PER_SHARE_TTM", text: "🧱 FCF/Share (TTM)" },
        { value: "CASH_PER_SHARE", text: "🧱 Cash/Share" },
        { value: "CASH_PLUS_ST_INV_PER_SHARE", text: "🧱 Cash+ST Inv/Share" },
        { value: "PRICE_TO_CASH_PLUS_ST_INV", text: "📊 Price/Cash+ST Inv" },
        { value: "BOOK_VALUE_PER_SHARE", text: "🧱 Book Value/Share" },
        { value: "PRICE_TO_BOOK_VALUE", text: "📊 Price/Book Value" },
        { value: "P_OPER_CF_TTM", text: "📊 P/Op CF (TTM)" },
        { value: "P_FCF_TTM", text: "📊 P/FCF (TTM)" },

        // Advanced Synthetic Studies (now also defined here for dropdown population consistency)
        // The yAxisLabel will still primarily be driven by analytics_ts_fund_adv.js at render time.
        { value: "FCF_MARGIN_TTM", text: "📊 FCF Margin % (TTM)", isAdvanced: true },
        { value: "GROSS_MARGIN_TTM", text: "📊 Gross Margin % (TTM)", isAdvanced: true },
        { value: "OPERATING_MARGIN_TTM", text: "📊 Operating Margin % (TTM)", isAdvanced: true },
        { value: "NET_PROFIT_MARGIN_TTM", text: "📊 Net Profit Margin % (TTM)", isAdvanced: true },
        { value: "PRICE_TO_SALES_TTM", text: "📊 Price/Sales (TTM)", isAdvanced: true }
        
        // Future studies will be added here, for example:
        // { value: "ANOTHER_RATIO_TTM", text: "📊 Another Ratio (TTM)", isAdvanced: true }
    ];
})(); 