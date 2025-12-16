from collections import namedtuple
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st
from great_tables import GT
import requests
import time


@st.cache_data
def load_data():
    """Load all parquet files and cache them"""
    gtin = pl.read_parquet('data/cstore_master_ctin.parquet')
    discounts = pl.read_parquet('data/cstore_discounts.parquet')
    stores = pl.read_parquet('data/cstore_stores.parquet')
    payments = pl.read_parquet('data/cstore_payments.parquet')
    daily = pl.read_parquet('data/cstore_transactions_daily_agg.parquet')
    shopper = pl.read_parquet('data/cstore_shopper.parquet')
    sets = pl.read_parquet('data/cstore_transaction_sets.parquet')
    status = pl.read_parquet('data/cstore_store_status.parquet')
    
    items = pl.concat([
        pl.read_parquet(f'data/transaction_items/part-0000{i}.parquet')
        for i in range(7)
    ])
    
    return {
        'gtin': gtin,
        'discounts': discounts,
        'stores': stores,
        'payments': payments,
        'daily': daily,
        'shopper': shopper,
        'sets': sets,
        'status': status,
        'items': items
    }


# NOTE: Cached data was the only way to improve performance that I found within my research. 
data = load_data()

# NOTE: Custom CSS to fix metric display truncation issues and to ensure that all of my text is easily visible, as well as universal between the pages.
st.markdown("""
    <style>
    /* Fix truncated metric values */
    [data-testid="stMetricValue"] {
        font-size: 20px;
        white-space: nowrap !important;
        overflow: visible !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 14px;
        white-space: normal;
    }
    div[data-testid="metric-container"] {
        min-width: fit-content !important;
        width: auto !important;
    }
    /* Ensure columns can grow to fit content */
    [data-testid="column"] {
        min-width: fit-content;
    }
    </style>
""", unsafe_allow_html=True)

# NOTE: Sidebar created to navigate my streamlit app.
page = st.sidebar.radio(
    "Navigation",
    ["Home", "Top 5 Products Weekly Sales", "Packaged Beverages: Recommended Product Drops", "Cash Versus Credit Customers", "Comparison Demographics of Shoppers: Store Level (Census Data)"]
)

# NOTE: Global filters in sidebar, as I wanted to ensure that this was something that was interactive and that could be adjusted within the sidebar instead of within each of the pages.
st.sidebar.divider()
st.sidebar.header("Global Filters")

daily_data = data["daily"]
min_year = int(daily_data["CALENDAR_YEAR"].min())
max_year = int(daily_data["CALENDAR_YEAR"].max())

# NOTE: Default filter that I created to accomondate for all the years, to ensure that this is what is showcased unless the global filter option is selected, which 3 years are selectable. 
year_options = ["All Years"] + list(range(min_year, max_year + 1))
year_selection = st.sidebar.selectbox("Year", year_options, index=0)
year = None if year_selection == "All Years" else year_selection

months = st.sidebar.multiselect("Month", list(range(1, 13)), default=list(range(1, 13)))

with st.sidebar.expander("Data Validation of the Tables"):
    st.write("**Master Tables:**")
    st.write(f"Stores: {len(data['stores']):,}")
    st.write(f"Products (GTIN): {len(data['gtin']):,}")
    st.write(f"Transaction Sets: {len(data['sets']):,}")
    st.write(f"Transaction Items: {len(data['items']):,}")
    st.write("")
    st.write("**Store Details:**")
    unique_states = data['stores'].select("STATE").n_unique()
    unique_chains = data['stores'].select("STORE_CHAIN_NAME").n_unique()
    st.write(f"States: {unique_states}")
    st.write(f"Chains: {unique_chains}")

# NOTE: Created a unified data feed for all pages to ensure consistent filtering as this was a critical first step in ensuring that there was 1) cached data and 2) that all pages consistently used the same source.
# NOTE: Removed @st.cache_data to prevent MemoryError - the underlying data is already cached by load_data()
def get_unified_data(_data_dict, year_filter, month_filter):
    """
    Filter all data sources consistently by year/month
    Returns unified dataset for all pages
    year_filter can be None for "All Years"
    """

    if year_filter is None:
        filtered_daily = _data_dict["daily"].filter(
            pl.col("CALENDAR_MONTH").is_in(month_filter)
        )


    else:
        filtered_daily = _data_dict["daily"].filter(
            (pl.col("CALENDAR_YEAR") == year_filter) &
            (pl.col("CALENDAR_MONTH").is_in(month_filter))
        )
    sets_with_date = _data_dict["sets"].with_columns([
        pl.col("DATE_TIME").dt.year().alias("year"),
        pl.col("DATE_TIME").dt.month().alias("month")
    ])
    
    # NOTE: Filter for ensuring that months and years are properly filtered.
    if year_filter is None:
        filtered_sets = sets_with_date.filter(
            pl.col("month").is_in(month_filter)
        )
    else:
        filtered_sets = sets_with_date.filter(
            (pl.col("year") == year_filter) &
            (pl.col("month").is_in(month_filter))
        )
    
    filtered_sets = filtered_sets.drop(["year", "month"])
    
    # NOTE: Get valid transaction IDs from filtered sets, as these are the only ones that should be considered.
    valid_txn_ids = filtered_sets.select("TRANSACTION_SET_ID").unique()
    
    # NOTE: Transactions for items based on filtered transaction sets
    filtered_items = _data_dict["items"].join(
        valid_txn_ids, 
        on="TRANSACTION_SET_ID", 
        how="inner"
    )
    
    # NOTE: Unified metrics for transactions
    total_revenue = filtered_sets.select(pl.col("GRAND_TOTAL_AMOUNT").sum()).item() or 0
    total_transactions = len(filtered_sets)
    unique_stores = filtered_sets.select(pl.col("STORE_ID").n_unique()).item() or 0
    
    return {
        'filtered_daily': filtered_daily,
        'filtered_sets': filtered_sets,
        'filtered_items': filtered_items,
        'total_revenue': total_revenue,
        'total_transactions': total_transactions,
        'unique_stores': unique_stores
    }

# NOTE: Unified datafeed for all pages to use so that there isn't redundant code everywhere and for performance to not get tanked as I originally had not used caching here.
unified = get_unified_data(data, year, months)
filtered_daily = unified['filtered_daily'] 

# Home Page
if page == "Home":

    st.markdown("""
    <h1 style='text-align: center; color: #2E86AB; font-family: Arial, sans-serif;'>
        C-Store Dashboard Overview
    </h1>
    <p style='text-align: center; color: #6B7280; font-size: 14px;'>
        Years 2022 - 2024
    </p>
""", unsafe_allow_html=True)
    
    st.markdown("""
    This dashboard provides comprehensive analytics for convenience store operations.
    Use the sidebar to navigate between different analyses.
    
    ### Available Pages:
    - **Top 5 Products Weekly Sales**: Identify best-selling products (excluding fuels)
    - **Packaged Beverages**: Recommendations for product drops
    - **Cash vs Credit**: Payment method comparison and customer behavior
    - **Demographics**: Store-level shopper demographics using Census data
    """)
    
    st.divider()
    
    # NOTE: Layout Container #1: columns
    st.subheader("Overview of Summary Statistics")
    stores_in_stores_table = len(data['stores'])
    stores_in_daily = daily_data.select("STORE_ID").n_unique()
    stores_in_filtered_daily = filtered_daily.select("STORE_ID").n_unique()
    
    with st.expander("Store Count Analysis"):
        st.write(f"**Stores in 'stores' table:** {stores_in_stores_table:,}")
        st.write(f"**Stores with transaction data (all years):** {stores_in_daily:,}")
        st.write(f"**Stores with transaction data (filtered year/month):** {stores_in_filtered_daily:,}")
        st.info("The 'Total Stores' metric shows stores from the master stores table. Some stores may not have transactions in the selected time period.")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Stores", f"{stores_in_stores_table:,}", 
                  delta=f"{stores_in_filtered_daily:,} active",
                  help=f"{stores_in_stores_table} stores in master table, {stores_in_filtered_daily} with transactions in selected period")
    with col2:
        st.metric("Total Products", f"{len(data['gtin']):,}")
    with col3:
        st.metric("Total Revenue", f"${unified['total_revenue']:,.2f}",
                  help="Based on actual transaction sets")
    with col4:
        st.metric("Total Transactions", f"{unified['total_transactions']:,.0f}",
                  help="Based on actual transaction sets")
    
    st.divider()
    
    # NOTE: Layout Container #2: expander to view the data dictionary.
    with st.expander("View Data Dictionary"):
        st.markdown("""
        **Available Datasets:**
        - `GTIN`: Product master data (80k+ products)
        - `Stores`: Store location and chain information (24k+ stores)
        - `Daily Transactions`: Aggregated daily sales data
        - `Payments`: Payment transaction details
        - `Shoppers`: Customer identification data
        - `Discounts`: Discount and promotion data
        """)

# Page #2
elif page == "Top 5 Products Weekly Sales":
    st.markdown("""
        <h1 style='text-align: center; color: #2E86AB; font-family: Arial, sans-serif;'>
            Top 5 Products Weekly Sales
        </h1>
        <p style='text-align: center; color: #6B7280; font-size: 14px;'>
            Years 2022 - 2024
        </p>
    """, unsafe_allow_html=True)
    st.markdown("*Excluding fuel products*")
    
    # Layout Container #1: columns for filters
    col1, col2 = st.columns([2, 1])
    with col1:
        categories = filtered_daily.filter(pl.col("CATEGORY") != "FUEL").select("CATEGORY").unique().sort("CATEGORY").to_series().to_list()
        selected_categories = st.multiselect("Filter by Category (Fuel Excluded)", categories, default=categories)
    
    if selected_categories:
        category_filtered = filtered_daily.filter(
            (pl.col("CATEGORY") != "FUEL") & 
            (pl.col("CATEGORY").is_in(selected_categories))
        )
    else:
        category_filtered = filtered_daily.filter(pl.col("CATEGORY") != "FUEL")
    
    # NOTE: Calculates top 5 products overall
    top5_overall = (
        category_filtered
        .filter(
            (pl.col("BRAND").is_not_null()) &
            (pl.col("SKUPOS_DESCRIPTION").is_not_null()) &
            (pl.col("CATEGORY").is_not_null())
        )
        .group_by(["BRAND", "SKUPOS_DESCRIPTION", "CATEGORY"])
        .agg([
            pl.sum("TOTAL_REVENUE_AMOUNT").alias("total_revenue"),
            pl.sum("QUANTITY").alias("total_units"),
            pl.sum("TRANSACTION_COUNT").alias("total_transactions")
        ])
        .with_columns([
            (pl.col("total_revenue") / pl.col("total_units")).alias("avg_price")
        ])
        .sort("total_revenue", descending=True)
        .limit(5)
    )
    
    # NOTE: Safety Check to ensure that there is data to work with.
    if len(top5_overall) == 0:
        st.warning("No data available for the selected filter. Looks like you may need to try again.")
        st.stop()
    
    # NOTE: weekly breakdown for top 5 items sold within the filtered categories, as fuel is always excluded. 
    top5_products = top5_overall.select("SKUPOS_DESCRIPTION").to_series().to_list()
    weekly_top5 = (
        category_filtered
        .filter(
            (pl.col("SKUPOS_DESCRIPTION").is_in(top5_products)) &
            (pl.col("WEEk").is_not_null())  
            # NOTE: Column is 'WEEk'
        )
        .group_by(["WEEk", "SKUPOS_DESCRIPTION", "BRAND"])
        .agg([
            pl.sum("TOTAL_REVENUE_AMOUNT").alias("weekly_revenue"),
            pl.sum("QUANTITY").alias("weekly_units")
        ])
        .sort(["WEEk", "weekly_revenue"], descending=[False, True])
    )
    
    # NOTE: KPIs - Layout Container #2: columns
    st.subheader("Key Performance Indicators")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_rev = top5_overall.select(pl.col("total_revenue").sum()).item()
        st.metric("Total Revenue (Top 5)", f"${total_rev:,.2f}")
    with col2:
        total_units = top5_overall.select(pl.col("total_units").sum()).item()
        st.metric("Total Units Sold", f"{total_units:,.0f}")
    with col3:
        avg_weekly_rev = weekly_top5.select(pl.col("weekly_revenue").mean()).item()
        st.metric("Avg Weekly Revenue", f"${avg_weekly_rev:,.2f}")
    with col4:
        num_weeks = weekly_top5.select(pl.col("WEEk").n_unique()).item()
        st.metric("Number of Weeks", f"{num_weeks}")
    
    st.divider()
    
    # NOTE: Great Tables summary of the Top 5 Products when they are explicity listed or selected.
    st.subheader("Top 5 Products Summary")
    gt_df = (
        top5_overall
        .select([
            pl.col("SKUPOS_DESCRIPTION").alias("Product"),
            pl.col("BRAND").alias("Brand"),
            pl.col("CATEGORY").alias("Category"),
            pl.col("total_revenue").round(2).alias("Revenue"),
            pl.col("total_units").alias("Units"),
            pl.col("avg_price").round(2).alias("Avg Price"),
            pl.col("total_transactions").alias("Transactions")
        ])
        .to_pandas()
    )
    
    gt_table = (
        GT(gt_df)
        .tab_header(
            title="Top 5 Products by Revenue",
            subtitle=f"Months: {', '.join(map(str, months))}"
        )
        .fmt_currency(columns=["Revenue", "Avg Price"], currency="USD")
        .fmt_number(columns=["Units", "Transactions"], decimals=0)
    )




    st.html(gt_table.as_raw_html())
    
    st.divider()
    
    # NOTE: Chart #1: Weekly trend line chart with optional target line that was included with a checkbox to disable if desired. 
    st.subheader("Weekly Revenue Trend")
    
    fig_line = px.line(
        weekly_top5.to_pandas(),
        x='WEEk',
        y='weekly_revenue',
        color='SKUPOS_DESCRIPTION',
        title='Weekly Revenue Trend for Top 5 Products',
        labels={'WEEk': 'Week Number', 'weekly_revenue': 'Revenue ($)', 'SKUPOS_DESCRIPTION': 'Product'},
        markers=True
    )
    

    fig_line.update_layout(hovermode='x unified')
    st.plotly_chart(fig_line, use_container_width=True)
    
    # NOTE: Chart #2: Bar chart comparing products by revenue, with labels to showcase this on the x and y axis.
    st.subheader("Revenue Comparison")
    
    fig_bar = px.bar(
        top5_overall.to_pandas(),
        x='SKUPOS_DESCRIPTION',
        y='total_revenue',
        color='CATEGORY',
        title='Top 5 Products by Total Revenue',
        labels={'SKUPOS_DESCRIPTION': 'Product', 'total_revenue': 'Total Revenue ($)'},
        text='total_revenue'
    )
    fig_bar.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
    fig_bar.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig_bar, use_container_width=True)

# Page #3
elif page == "Packaged Beverages: Recommended Product Drops":
    st.markdown("""
    <h1 style='text-align: center; color: #2E86AB; font-family: Arial, sans-serif;'>
        Packaged Beverages: Product Drop Recommendations Based off of Low Units Sold
    </h1>
    <p style='text-align: center; color: #6B7280; font-size: 14px;'>
        Years 2022 - 2024
    </p>
""", unsafe_allow_html=True)
    
    with st.expander("Available Categories"):
        available_categories = filtered_daily.select("CATEGORY").unique().sort("CATEGORY").to_series().to_list()
        st.write(f"Found {len(available_categories)} categories in filtered data")
        st.write(available_categories[:20])  # Show first 20
    
    col1, col2 = st.columns([2, 1])
    with col1:
        min_transactions = st.slider("Minimum Transaction Count", 0, 1000, 18)
    with col2:
        show_threshold = st.checkbox("Show Revenue Threshold Line", value=True)
        if show_threshold:
            revenue_threshold = st.number_input("Revenue Threshold ($)", min_value=0, value=10000, step=1000)
    


    bev_perf = (
        filtered_daily
        .filter(
            (pl.col("CATEGORY").is_not_null()) &
            (pl.col("BRAND").is_not_null()) &
            (
                (pl.col("CATEGORY").str.contains("(?i)BEVERAGE")) |
                (pl.col("CATEGORY").str.contains("(?i)DRINK")) |
                (pl.col("SUBCATEGORY").str.contains("(?i)BEVERAGE")) |
                (pl.col("SUBCATEGORY").str.contains("(?i)DRINK"))
            )
        )
        .group_by("BRAND")
        .agg([
            pl.sum("TOTAL_REVENUE_AMOUNT").alias("revenue"),
            pl.sum("QUANTITY").alias("units"),
            pl.sum("TRANSACTION_COUNT").alias("transactions")
        ])
        .with_columns([
            (pl.col("revenue") / pl.col("units")).alias("rev_per_unit"),
            (pl.col("revenue") / pl.col("transactions")).alias("rev_per_transaction")
        ])
        .filter(pl.col("transactions") >= min_transactions)
        .sort("revenue")
    )


    # NOTE: Safety Check to ensure that there is data to work with.
    if len(bev_perf) == 0:
        st.warning("No packaged beverage data available, I would try adjusting the filter (ie, lower or increase the minimum transaction count).")
        st.stop()
    
    # NOTE: KPIs - Layout Container #2: columns
    st.subheader("Beverage Category KPIs")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_brands = len(bev_perf)
        st.metric("Total Brands", f"{total_brands}")
    with col2:
        total_bev_revenue = bev_perf.select(pl.col("revenue").sum()).item()
        st.metric("Total Revenue", f"${total_bev_revenue:,.2f}")
    with col3:
        avg_rev_per_brand = bev_perf.select(pl.col("revenue").mean()).item()
        avg_rev_per_brand = avg_rev_per_brand if avg_rev_per_brand is not None else 0
        st.metric("Avg Revenue/Brand", f"${avg_rev_per_brand:,.2f}")
    with col4:
        low_performers = bev_perf.filter(pl.col("revenue") < revenue_threshold if show_threshold else pl.col("revenue") < 10000)
        st.metric("Brands Below Threshold", f"{len(low_performers)}", delta=f"-{len(low_performers)}", delta_color="inverse")
    
    st.divider()
    
    # NOTE: Great Tables - Bottom performers of the top 10 brands by revenue to consider for product drops.
    st.subheader("Recommended Products to Drop Based off of Low Units Sold as a Total Unit Analysis")
    bottom_10 = bev_perf.limit(10)
    
    gt_df = (
        bottom_10
        .select([
            pl.col("BRAND").alias("Brand"),
            pl.col("revenue").round(2).alias("Revenue"),
            pl.col("units").alias("Units Sold"),
            pl.col("transactions").alias("Transactions"),
            pl.col("rev_per_unit").round(2).alias("$/Unit"),
            pl.col("rev_per_transaction").round(2).alias("$/Transaction")
        ])
        .to_pandas()
    )
    
    gt_table = (
        GT(gt_df)
        .tab_header(
            title="Bottom 10 Beverage Brands",
            subtitle="Candidates for removal based on low revenue"
        )
        .fmt_currency(columns=["Revenue", "$/Unit", "$/Transaction"], currency="USD")
        .fmt_number(columns=["Units Sold", "Transactions"], decimals=0)
    )
    
    st.html(gt_table.as_raw_html())
    
    st.divider()
    # NOTE: Chart #1: Scatter plot with threshold line
    st.subheader("Performance Summary: Revenue vs Transactions")
    
    fig_scatter = px.scatter(
        bev_perf.to_pandas(),
        x="transactions",
        y="revenue",
        size="units",
        hover_name="BRAND",
        hover_data={"rev_per_unit": ':.2f', "transactions": True, "revenue": ':,.2f'},
        title="Beverage Brand Performance",
        labels={'transactions': 'Number of Transactions', 'revenue': 'Total Revenue ($)'}
    )
    
    # NOTE: Logic for the threshold line, replicated amongst the other tabs, as this was the best way to streamline this process. 
    if show_threshold:
        fig_scatter.add_hline(
            y=revenue_threshold,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Drop Threshold: ${revenue_threshold:,.0f}",
            annotation_position="right"
        )
    
    st.plotly_chart(fig_scatter, use_container_width=True)
    st.subheader("Lowest Performing of a 10 Brand Spread by Revenue")
    
    fig_bar = px.bar(
        bottom_10.to_pandas(),
        x="BRAND",
        y="revenue",
        title="Lowest Revenue Brands",
        labels={'BRAND': 'Brand', 'revenue': 'Total Revenue ($)'},
        color="revenue",
        color_continuous_scale="Reds"
    )
    fig_bar.update_layout(xaxis_tickangle=-45, showlegend=False)
    st.plotly_chart(fig_bar, use_container_width=True)

# Page #4
elif page == "Cash Versus Credit Customers":
    st.markdown("""
    <h1 style='text-align: center; color: #2E86AB; font-family: Arial, sans-serif;'>
        Cash vs Credit Customer Analysis
    </h1>
    <p style='text-align: center; color: #6B7280; font-size: 14px;'>
        Years 2022 - 2024
    </p>
""", unsafe_allow_html=True)
    
    payment_analysis = (
        unified['filtered_sets']
        .filter(
            (pl.col("PAYMENT_TYPE").is_not_null()) &
            (pl.col("PAYMENT_TYPE").is_in(["CASH", "CREDIT", "DEBIT"]))
        )
        .join(
            unified['filtered_items'].select(["TRANSACTION_SET_ID", "GTIN", "UNIT_QUANTITY", "UNIT_PRICE", "GRAND_TOTAL_AMOUNT"]),
            on="TRANSACTION_SET_ID",
            how="inner"
        )
        .join(
            data["gtin"].select(["GTIN", "CATEGORY", "SKUPOS_DESCRIPTION"]),
            on="GTIN",
            how="left"
        )
    )
    
    # NOTE: Layout Container #1: columns for filters
    col1, col2 = st.columns([2, 1])
    with col1:
        payment_types = st.multiselect("Payment Types", ["CASH", "CREDIT", "DEBIT"], default=["CASH", "CREDIT"])
    with col2:
        show_avg_line = st.checkbox("Show Average Purchase Line", value=True)
    
    filtered_payments = payment_analysis.filter(pl.col("PAYMENT_TYPE").is_in(payment_types))
    
    # NOTE: Summary by payment type (CARD vserus CASH)
    payment_summary = (
        filtered_payments
        .group_by("PAYMENT_TYPE")
        .agg([
            pl.col("TRANSACTION_SET_ID").n_unique().alias("num_transactions"),
            pl.col("GRAND_TOTAL_AMOUNT").sum().alias("total_spend"),
            pl.col("GRAND_TOTAL_AMOUNT").mean().alias("avg_ticket"),
            pl.col("UNIT_QUANTITY").sum().alias("total_items")
        ])
        .with_columns([
            (pl.col("total_items") / pl.col("num_transactions")).alias("avg_items_per_txn")
        ])
    )
    
    # NOTE: Top products by payment type (card versus CASH)
    top_products_by_payment = (
        filtered_payments
        .filter(pl.col("CATEGORY").is_not_null())
        .group_by(["PAYMENT_TYPE", "SKUPOS_DESCRIPTION", "CATEGORY"])
        .agg([
            pl.col("TRANSACTION_SET_ID").n_unique().alias("purchase_count"),
            pl.col("GRAND_TOTAL_AMOUNT").sum().alias("revenue")
        ])
        .sort(["PAYMENT_TYPE", "purchase_count"], descending=[False, True])
        .group_by("PAYMENT_TYPE")
        .head(5)
    )
    st.subheader("Payment Method Comparison")
    
    if len(payment_types) >= 2:
        cols = st.columns(len(payment_types))
        for idx, ptype in enumerate(payment_types):
            with cols[idx]:
                ptype_data = payment_summary.filter(pl.col("PAYMENT_TYPE") == ptype)
                if len(ptype_data) > 0:
                    st.markdown(f"### {ptype}")
                    num_txn = ptype_data.select("num_transactions").item()
                    total = ptype_data.select("total_spend").item()
                    avg_ticket = ptype_data.select("avg_ticket").item()
                    avg_items = ptype_data.select("avg_items_per_txn").item()
                    
                    st.metric("Transactions", f"{num_txn:,}")
                    st.metric("Total Spend", f"${total:,.2f}")
                    st.metric("Avg Ticket", f"${avg_ticket:.2f}")
                    st.metric("Avg Items/Transaction", f"{avg_items:.1f}")
    
    st.divider()
    st.subheader("Top 5 Products by Payment Type (ie, cash vs credit card)")
    
    for ptype in payment_types:
        with st.expander(f"Top Products for {ptype} Customers"):
            ptype_products = top_products_by_payment.filter(pl.col("PAYMENT_TYPE") == ptype)
            
            gt_df = (
                ptype_products
                .select([
                    pl.col("SKUPOS_DESCRIPTION").alias("Product"),
                    pl.col("CATEGORY").alias("Category"),
                    pl.col("purchase_count").alias("Purchase Count"),
                    pl.col("revenue").round(2).alias("Revenue")
                ])
                .to_pandas()
            )
            
            gt_table = (
                GT(gt_df)
                .tab_header(
                    title=f"Top 5 Products - {ptype}",
                    subtitle="Most frequently purchased items"
                )
                .fmt_currency(columns=["Revenue"], currency="USD")
                .fmt_number(columns=["Purchase Count"], decimals=0)
            )
            
            st.html(gt_table.as_raw_html())
    
    st.divider()
    
    # NOTE: Chart #1: Transaction amount comparison with average line incorporated to showcase the overall average ticket size.
    st.subheader("Average Transaction Amount Comparison")
    
    avg_ticket_value = payment_summary.select(pl.col("avg_ticket").mean()).item()
    
    fig_bar = px.bar(
        payment_summary.to_pandas(),
        x="PAYMENT_TYPE",
        y="avg_ticket",
        title="Average Ticket Size by Payment Type",
        labels={'PAYMENT_TYPE': 'Payment Type', 'avg_ticket': 'Average Ticket ($)'},
        color="PAYMENT_TYPE",
        text="avg_ticket"
    )
    fig_bar.update_traces(texttemplate='$%{text:.2f}', textposition='outside')
    
    if show_avg_line:
        fig_bar.add_hline(
            y=avg_ticket_value,
            line_dash="dash",
            line_color="green",
            annotation_text=f"Overall Avg: ${avg_ticket_value:.2f}",
            annotation_position="right"
        )
    
    st.plotly_chart(fig_bar, use_container_width=True)
    
    # NOTE: 2nd Chart - Items versus the total transaction as a comparison to evaluate customer behavior in credit cards versus carash payments - overall historical assessment. 
    st.subheader("Items per Transaction Comparison")
    
    fig_items = px.bar(
        payment_summary.to_pandas(),
        x="PAYMENT_TYPE",
        y="avg_items_per_txn",
        title="Average Items per Transaction by Payment Type",
        labels={'PAYMENT_TYPE': 'Payment Type', 'avg_items_per_txn': 'Avg Items per Transaction'},
        color="PAYMENT_TYPE",
        text="avg_items_per_txn"
    )
    fig_items.update_traces(texttemplate='%{text:.1f}', textposition='outside')
    st.plotly_chart(fig_items, use_container_width=True)

# Page #5
elif page == "Comparison Demographics of Shoppers: Store Level (Census Data)":
    st.markdown("""
    <h1 style='text-align: center; color: #2E86AB; font-family: Arial, sans-serif;'>
        Store Demographics Analysis (Census Data)
    </h1>
    <p style='text-align: center; color: #6B7280; font-size: 14px;'>
        Years 2022 - 2024
    </p>
""", unsafe_allow_html=True)
    
    
    API_KEY = "551c09e32d473ee287b8d267cfee54aa81c502d9"
    ACS_BASE_URL = "https://api.census.gov/data/2023/acs/acs5"
    
    ACS_VARS = [
        "B01003_001E",  # Total Population
        "B01001_001E",  # Age/Sex Total Population
        "B19019_001E",  # Median Household Income
        "B15003_025E",  # Professional Degree
        "B17001_002E",  # Below Poverty Level
        "B25077_001E",  # Median Home Value
        "B25064_001E",  # Median Gross Rent
        "B08301_001E",  # Total Workers (Commute)
        "B23025_004E",  # Unemployed Population
        "B08201_001E"   # Households with Vehicles
    ]
    
    @st.cache_data(ttl=7200)
    def geocode_store(lat, lon):
        """Get Census tract for store location"""
        geocode_url = (
            "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
            f"?x={lon}&y={lat}&benchmark=Public_AR_Census2020"
            "&vintage=Census2020_Census2020&format=json"
        )
        try:
            r = requests.get(geocode_url, timeout=10)
            r.raise_for_status()
            geo = r.json()["result"]["geographies"]["Census Tracts"][0]
            return {
                "STATE": geo["STATE"],
                "COUNTY": geo["COUNTY"],
                "TRACT": geo["TRACT"]
            }
        except Exception as e:
            st.warning(f"Geocoding failed for ({lat}, {lon}): {str(e)}")
            return {"STATE": None, "COUNTY": None, "TRACT": None}
    
    @st.cache_data(ttl=7200)
    def fetch_tract_acs(state, county, tract):
        """Fetch ACS data for a specific tract"""
        acs_var_str = ",".join(ACS_VARS)
        url = (
            f"{ACS_BASE_URL}"
            f"?get={acs_var_str}"
            f"&for=tract:{tract}"
            f"&in=state:{state}+county:{county}"
            f"&key={API_KEY}"
        )
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            data = r.json()
            if len(data) > 1:
                # NOTE: Return values (excluding state, county, tract codes at end)
                return data[1][:-3]
            return None
        except Exception as e:
            st.warning(f"ACS fetch failed for tract {state}-{county}-{tract}: {str(e)}")
            return None
    
    @st.cache_data(ttl=7200)
    def fetch_county_acs():
        """Fetch ACS data for all counties"""
        acs_var_str = ",".join(ACS_VARS)
        url = (
            f"{ACS_BASE_URL}"
            f"?get={acs_var_str},NAME"
            f"&for=county:*"
            f"&in=state:*"
            f"&key={API_KEY}"
        )
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            data = r.json()
            
            # NOTE: Converts to Polars DataFrame (same logic that was applied when I created this API in PySpark)
            header = data[0]
            rows = data[1:]
            county_df = pl.DataFrame(rows, schema=header, orient="row")
            
            # NOTE: Cast state and county to string for joining
            county_df = county_df.with_columns([
                pl.col("state").cast(pl.Utf8),
                pl.col("county").cast(pl.Utf8)
            ])
            
            return county_df
        except Exception as e:
            st.error(f"Failed to fetch county ACS data: {str(e)}")
            return None
        
    st.subheader("Geocoding Stores")
    
    # NOTE: casts STORE_ID to integer then string to ensure consistent format (avoids "1.0" vs "1" mismatch)
    stores_df = data['stores'].select(["STORE_ID", "LATITUDE", "LONGITUDE", "STATE", "CITY"]).with_columns(
        pl.col("STORE_ID").cast(pl.Int64).cast(pl.Utf8)
    )
    
    tract_cache_file = "data/census_tract_geocoded.parquet"
    
    # NOTE: Tries to load from cache first
    if 'tract_geocoded' not in st.session_state:
        try:
            import os
            if os.path.exists(tract_cache_file):
                st.session_state.tract_df = pl.read_parquet(tract_cache_file)
                st.session_state.tract_geocoded = True
                st.success(f"Loaded {len(st.session_state.tract_df)} geocoded stores from cache!")
            else:
                st.session_state.tract_geocoded = False
        except:
            st.session_state.tract_geocoded = False
    
    col1, col2 = st.columns([3, 1])
    with col1:
        if st.session_state.tract_geocoded:
            st.info(f"{len(stores_df)} stores already geocoded. Data cached in {tract_cache_file}")
        else:
            st.info(f"Found {len(stores_df)} stores. Geocoding will take ~45 seconds (0.25s per store).")
    with col2:
        if st.button("Geocode Stores", disabled=st.session_state.tract_geocoded):
            with st.spinner("Geocoding stores..."):
                tract_data = []
                progress_bar = st.progress(0)
                
                for idx, row in enumerate(stores_df.iter_rows(named=True)):
                    geo_result = geocode_store(row['LATITUDE'], row['LONGITUDE'])
                    tract_data.append({
                        "STORE_ID": row['STORE_ID'],
                        "STATEFP": geo_result['STATE'],
                        "COUNTYFP": geo_result['COUNTY'],
                        "TRACT": geo_result['TRACT']
                    })
                    time.sleep(0.05)  # NOTE: Rate limiting to ensure that the Census API is not overwhelmed, I haven't had any issues in the past as of yet, but my experience with webscraping tells me otherwise. 
                    progress_bar.progress((idx + 1) / len(stores_df))
                
                st.session_state.tract_df = pl.DataFrame(tract_data)
                st.session_state.tract_df.write_parquet(tract_cache_file)
                st.session_state.tract_geocoded = True
                st.success(f"Geocoded {len(tract_data)} stores and saved to {tract_cache_file}!")
    
    if st.session_state.tract_geocoded and 'tract_df' in st.session_state:
        st.divider()
        st.subheader("Fetch Tract-Level ACS Data")
        
        tract_df = st.session_state.tract_df
        unique_tracts = (
            tract_df
            .filter(
                (pl.col("STATEFP").is_not_null()) &
                (pl.col("COUNTYFP").is_not_null()) &
                (pl.col("TRACT").is_not_null())
            )
            .select(["STATEFP", "COUNTYFP", "TRACT"])
            .unique()
        )
        
        tract_acs_cache_file = "data/census_tract_acs.parquet"
        
        if 'acs_tract_fetched' not in st.session_state:
            try:
                import os
                if os.path.exists(tract_acs_cache_file):
                    st.session_state.acs_tract_df = pl.read_parquet(tract_acs_cache_file)
                    st.session_state.acs_tract_fetched = True
                    st.success(f"Loaded tract ACS data from cache ({len(st.session_state.acs_tract_df)} tracts)!")
                else:
                    st.session_state.acs_tract_fetched = False
            except:
                st.session_state.acs_tract_fetched = False
        
        if st.session_state.acs_tract_fetched:
            st.write(f"ACS data for {len(unique_tracts)} unique tracts already cached.")
        else:
            st.write(f"Found {len(unique_tracts)} unique tracts to fetch")
        
        if st.button("Fetch Tract ACS Data", disabled=st.session_state.acs_tract_fetched):
            with st.spinner("Fetching ACS data for tracts..."):
                acs_results = []
                progress_bar = st.progress(0)
                
                for idx, row in enumerate(unique_tracts.iter_rows(named=True)):
                    acs_values = fetch_tract_acs(
                        row['STATEFP'],
                        row['COUNTYFP'],
                        row['TRACT']
                    )
                    
                    if acs_values:
                        result_dict = {
                            "STATEFP": row['STATEFP'],
                            "COUNTYFP": row['COUNTYFP'],
                            "TRACT": row['TRACT']
                        }
                        # NOTE: Adds ACS variables as I needed to still add them
                        for var_idx, var_name in enumerate(ACS_VARS):
                            result_dict[var_name] = acs_values[var_idx]
                        
                        acs_results.append(result_dict)
                    
                    time.sleep(0.05)  # NOTE: Added Rate limiting to ensure that I don't get kicked off the Census API for too many requests in a short period of time.
                    progress_bar.progress((idx + 1) / len(unique_tracts))
                
                st.session_state.acs_tract_df = pl.DataFrame(acs_results)
                st.session_state.acs_tract_df.write_parquet(tract_acs_cache_file)
                st.session_state.acs_tract_fetched = True
                st.success(f"Fetched ACS data for {len(acs_results)} tracts and saved to {tract_acs_cache_file}!")
    
    if st.session_state.get('acs_tract_fetched', False):
        st.divider()
        st.subheader("Fetch County-Level ACS Data")
        
        county_acs_cache_file = "data/census_county_acs.parquet"
        
        if 'county_acs_fetched' not in st.session_state:
            try:
                import os
                if os.path.exists(county_acs_cache_file):
                    st.session_state.county_acs_df = pl.read_parquet(county_acs_cache_file)
                    st.session_state.county_acs_fetched = True
                    st.success(f"Loaded county ACS data from cache ({len(st.session_state.county_acs_df)} counties)!")
                else:
                    st.session_state.county_acs_fetched = False
            except:
                st.session_state.county_acs_fetched = False
        
        if st.button("Fetch County ACS Data", disabled=st.session_state.county_acs_fetched):
            with st.spinner("Fetching county ACS data..."):
                county_df = fetch_county_acs()
                
                if county_df is not None:
                    st.session_state.county_acs_df = county_df
                    st.session_state.county_acs_df.write_parquet(county_acs_cache_file)
                    st.session_state.county_acs_fetched = True
                    st.success(f"Fetched ACS data for {len(county_df)} counties and saved to {county_acs_cache_file}!")
        
        # NOTE: Joins stores with tract data
        stores_tract = (
            stores_df
            .join(st.session_state.tract_df, on="STORE_ID", how="left")
        )
        
        # NOTE: Joins with tract-level ACS
        stores_tract_acs = (
            stores_tract
            .join(
                st.session_state.acs_tract_df,
                on=["STATEFP", "COUNTYFP", "TRACT"],
                how="left"
            )
        )
        
        # NOTE: Renamed county columns for joining
        county_df = st.session_state.county_acs_df
        for var in ACS_VARS:
            county_df = county_df.with_columns(
                pl.col(var).alias(f"county_{var}")
            )
        county_df = county_df.with_columns(
            pl.col("NAME").alias("county_NAME")
        )
        
        # NOTE: Joins with county-level ACS
        stores_enriched = (
            stores_tract_acs
            .join(
                county_df,
                left_on=["STATEFP", "COUNTYFP"],
                right_on=["state", "county"],
                how="left"
            )
        )
        
        
        # NOTE: This was resolved as an error for mismatches - cast STORE_ID to Int64 first, then string to match stores format
        store_perf = (
            unified['filtered_daily']
            .with_columns(pl.col("STORE_ID").cast(pl.Int64).cast(pl.Utf8))
            .group_by("STORE_ID")
            .agg([
                pl.sum("TOTAL_REVENUE_AMOUNT").alias("revenue"),
                pl.sum("TRANSACTION_COUNT").alias("transactions")
            ])
        )
        
        stores_with_perf = (
            stores_enriched
            .join(store_perf, on="STORE_ID", how="left")
        )
        

        st.divider()
        
        valid_stores = stores_with_perf.filter(
            (pl.col("B01003_001E").is_not_null()) &
            (pl.col("B19019_001E").is_not_null())
        )
        
        if len(valid_stores) > 0:

            st.subheader("Demographics Summary (Idaho Stores)")
            
            state_summary = (
                valid_stores
                .with_columns([
                    pl.col("B01003_001E").cast(pl.Float64).alias("population"),
                    pl.col("B19019_001E").cast(pl.Float64).alias("income"),
                    pl.col("B17001_002E").cast(pl.Float64).alias("poverty"),
                    pl.col("B25077_001E").cast(pl.Float64).alias("home_value")
                ])
                .group_by("STATE")
                .agg([
                    pl.count("STORE_ID").alias("store_count"),
                    pl.mean("population").alias("avg_tract_pop"),
                    pl.mean("income").alias("avg_income"),
                    pl.mean("poverty").alias("avg_poverty"),
                    pl.mean("home_value").alias("avg_home_value")
                ])
                .sort("store_count", descending=True)
            )
    
            gt_df = (
                state_summary
                .select([
                    pl.col("STATE").alias("State"),
                    pl.col("store_count").alias("Stores"),
                    pl.col("avg_tract_pop").round(0).alias("Avg Tract Pop"),
                    pl.col("avg_income").round(0).alias("Avg Median Income"),
                    pl.col("avg_poverty").round(0).alias("Avg Below Poverty"),
                    pl.col("avg_home_value").round(0).alias("Avg Home Value")
                ])
                .to_pandas()
            )
            
            gt_table = (
                GT(gt_df)
                .tab_header(
                    title="Demographics Summary by State",
                    subtitle="Census Tract-Level Averages"
                )
                .fmt_currency(columns=["Avg Median Income", "Avg Home Value"], currency="USD")
                .fmt_number(columns=["Stores", "Avg Tract Pop", "Avg Below Poverty"], decimals=0)
            )
            
            st.html(gt_table.as_raw_html())
            
            st.divider()

            col1, col2 = st.columns(2)
            
            with col1:
     
                plot_df = valid_stores.select([
                    "STORE_ID",
                    "STATE",
                    "CITY",
                    pl.col("B01003_001E").cast(pl.Float64).alias("population"),
                    pl.col("B19019_001E").cast(pl.Float64).alias("income"),
                    pl.col("B17001_002E").cast(pl.Float64).alias("poverty")
                ]).to_pandas()
                
                fig1 = px.scatter(
                    plot_df,
                    x="population",
                    y="income",
                    color="STATE",
                    hover_data=["STORE_ID", "CITY", "poverty"],
                    title="Median Income vs Tract Population",
                    labels={"population": "Tract Population", "income": "Median Household Income ($)"}
                )
                st.plotly_chart(fig1, use_container_width=True)
            
            with col2:
                fig2 = px.scatter(
                    plot_df,
                    x="poverty",
                    y="income",
                    color="STATE",
                    hover_data=["STORE_ID", "CITY", "population"],
                    title="Median Income vs Poverty Level",
                    labels={"poverty": "Population Below Poverty", "income": "Median Household Income ($)"}
                )
                st.plotly_chart(fig2, use_container_width=True)
            
            st.divider()
            st.subheader("Home Values and Income Distribution")
            
            col3, col4 = st.columns(2)
            
            with col3:
                # NOTE: Home value vs income
                plot_df2 = valid_stores.select([
                    "STORE_ID",
                    "STATE",
                    pl.col("B19019_001E").cast(pl.Float64).alias("income"),
                    pl.col("B25077_001E").cast(pl.Float64).alias("home_value")
                ]).to_pandas()
                
                fig3 = px.scatter(
                    plot_df2,
                    x="income",
                    y="home_value",
                    color="STATE",
                    hover_data=["STORE_ID"],
                    title="Home Value vs Median Income",
                    labels={"income": "Median Household Income ($)", "home_value": "Median Home Value ($)"}
                )
                st.plotly_chart(fig3, use_container_width=True)
            
            with col4:
                # NOTE: Income distribution by state (I hadn't figured out that there was only 1 state for this distribution of the dataset, but leaving it in for future use cases)
                fig4 = px.box(
                    plot_df,
                    x="STATE",
                    y="income",
                    color="STATE",
                    title="Income Distribution by State",
                    labels={"income": "Median Household Income ($)", "STATE": "State"}
                )
                st.plotly_chart(fig4, use_container_width=True)
        else:
            st.warning("No stores with complete demographic data available.")
    
    else:
        st.info("Click the buttons above to start the geocoding and data fetching process.")
    
    # NOTE: If the API needs to be re-queried for any reason, as I had a couple of instances where not all of the data was populated, as such I added a way to clear the cache entirely and overwrite it.
    # NOTE: This was a recommended addition based on issues that I kept having with geocoder timing out within streamlit sessions.
    st.divider()
    with st.expander("Cache Management"):
        st.write("Census data is cached in parquet files to avoid re-fetching:")
        st.write("- `data/census_tract_geocoded.parquet` - Geocoded store locations")
        st.write("- `data/census_tract_acs.parquet` - Tract-level demographics")
        st.write("- `data/census_county_acs.parquet` - County-level demographics")
        
        if st.button("Clear Census Cache"):
            import os
            files_to_delete = [
                "data/census_tract_geocoded.parquet",
                "data/census_tract_acs.parquet",
                "data/census_county_acs.parquet"
            ]
            
            deleted = []
            for file in files_to_delete:
                if os.path.exists(file):
                    os.remove(file)
                    deleted.append(file)
            
            # NOTE: This session_state will clear the entire session set and apply a master reset.
            for key in ['tract_geocoded', 'tract_df', 'acs_tract_fetched', 'acs_tract_df', 'county_acs_fetched', 'county_acs_df']:
                if key in st.session_state:
                    del st.session_state[key]
            
            if deleted:
                st.success(f"Deleted {len(deleted)} cache file(s). Refresh the page to re-fetch data from geocoder.")
            else:
                st.info("No cache files found to delete from the data directory.")

