import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
from datetime import datetime, timedelta
import folium
from streamlit_folium import folium_static
import random

# ---------------------------- CONFIGURATION ----------------------------
st.set_page_config(
    page_title="Flight Analytics Pro ✈️",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ---------------------------- SNOWFLAKE CONNECTION ----------------------------
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        account="LHHKQJH-IK34611",
        user="SALAH",
        password="Zfhu2YrQ3YjVmwQ",
        warehouse="COMPUTE_WH",
        database="ANALYTICS_DB"
    )

conn = init_connection()

@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

@st.cache_data(ttl=600)
def get_dataframe(query):
    with conn.cursor() as cur:
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])
    return df

# ---------------------------- HELPER FUNCTIONS ----------------------------
@st.cache_data(ttl=600)
def get_city_name(airport_code):
    """Convert airport code to city name using STG_AIRPORTS table"""
    if pd.isna(airport_code) or airport_code is None:
        return "Unknown"
    try:
        df = get_dataframe(f"SELECT CITY_NAME FROM ANALYTICS_DB.STAGING.STG_AIRPORTS WHERE AIRPORT_ID = '{airport_code}'")
        if not df.empty and df['CITY_NAME'].iloc[0] is not None:
            return df['CITY_NAME'].iloc[0]
        return airport_code
    except:
        return airport_code

@st.cache_data(ttl=600)
def format_route_name(route_id):
    """Convert ROUTE_ID (e.g., JFK-LAX) to city names (e.g., New York - Los Angeles)"""
    if pd.isna(route_id) or route_id is None or '-' not in route_id:
        return route_id
    parts = route_id.split('-')
    if len(parts) >= 2:
        origin = get_city_name(parts[0])
        dest = get_city_name(parts[1])
        return f"{origin} - {dest}"
    return route_id

# ---------------------------- AIRLINE NAME MAPPING ----------------------------
def get_airline_full_name(airline_code):
    """Convert airline code to full name"""
    airline_names = {
        'AA': 'American Airlines',
        'AF': 'Air France',
        'AZ': 'Alitalia',
        'BA': 'British Airways',
        'CX': 'Cathay Pacific',
        'DL': 'Delta Air Lines',
        'EK': 'Emirates',
        'EY': 'Etihad Airways',
        'KL': 'KLM Royal Dutch Airlines',
        'LH': 'Lufthansa',
        'MS': 'EgyptAir',
        'QR': 'Qatar Airways',
        'SQ': 'Singapore Airlines',
        'TK': 'Turkish Airlines',
        'UA': 'United Airlines',
    }
    return airline_names.get(airline_code, airline_code)

# ---------------------------- CUSTOM CSS ----------------------------
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: 900;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 0;
        padding-bottom: 0;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #6B7280;
        text-align: center;
        margin-top: 0;
        padding-top: 0;
    }
    .metric-card {
        background-color: #F3F4F6;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        height: 100%;
        margin-bottom: 10px;
    }
    .metric-value {
        font-size: 2.2rem;
        font-weight: 800;
        color: #1E3A8A;
        line-height: 1.2;
        margin-bottom: 10px;
    }
    .metric-label {
        font-size: 1rem;
        color: #6B7280;
        text-transform: uppercase;
        line-height: 1.4;
    }
    .stColumn {
        padding: 0 10px;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------- SIDEBAR ----------------------------
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/airplane-take-off.png", width=80)
    st.markdown("# ✈️ Flight Analytics")
    st.markdown("---")
    
    # Filters
    st.markdown("## 🎯 Filters")
    
    # Get airlines for filter - FIXED: STAGING instead of SILVER_STAGING
    airlines_df = get_dataframe("SELECT DISTINCT AIRLINE_NAME FROM ANALYTICS_DB.STAGING.STG_AIRLINES ORDER BY AIRLINE_NAME")
    
    # Convert airline codes to full names for display
    airlines_full = []
    for airline in airlines_df['AIRLINE_NAME'].tolist():
        airlines_full.append(get_airline_full_name(airline))
    
    airlines = ['All'] + airlines_full
    selected_airline_display = st.selectbox("Airline", airlines)
    
    # Get the original code for the selected airline (if not All)
    if selected_airline_display != 'All':
        # Reverse mapping to find code
        reverse_mapping = {v: k for k, v in {
            'AA': 'American Airlines',
            'AF': 'Air France',
            'AZ': 'Alitalia',
            'BA': 'British Airways',
            'CX': 'Cathay Pacific',
            'DL': 'Delta Air Lines',
            'EK': 'Emirates',
            'EY': 'Etihad Airways',
            'KL': 'KLM Royal Dutch Airlines',
            'LH': 'Lufthansa',
            'MS': 'EgyptAir',
            'QR': 'Qatar Airways',
            'SQ': 'Singapore Airlines',
            'TK': 'Turkish Airlines',
            'UA': 'United Airlines',
        }.items()}
        selected_airline = reverse_mapping.get(selected_airline_display, 'All')
    else:
        selected_airline = 'All'
    
    # Date range - FIXED: SILVER is correct for fact table
    dates_df = get_dataframe("SELECT MIN(DEPARTURE_DATE) as MIN_DATE, MAX(DEPARTURE_DATE) as MAX_DATE FROM ANALYTICS_DB.SILVER.FCT_DAILY_FLIGHT_OFFERS")
    
    if not dates_df.empty and dates_df['MIN_DATE'].iloc[0] is not None:
        min_date = pd.to_datetime(dates_df['MIN_DATE'].iloc[0]).date()
        max_date = pd.to_datetime(dates_df['MAX_DATE'].iloc[0]).date()
    else:
        min_date = datetime.now().date() - timedelta(days=30)
        max_date = datetime.now().date()
    
    date_range = st.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # Minimum price - FIXED: SILVER is correct for fact table
    price_stats = get_dataframe("SELECT MIN(PRICE_USD) as MIN_PRICE, MAX(PRICE_USD) as MAX_PRICE FROM ANALYTICS_DB.SILVER.FCT_DAILY_FLIGHT_OFFERS")
    
    if not price_stats.empty and price_stats['MIN_PRICE'].iloc[0] is not None:
        min_price = int(price_stats['MIN_PRICE'].iloc[0])
        max_price = int(price_stats['MAX_PRICE'].iloc[0])
    else:
        min_price = 0
        max_price = 10000
    
    price_threshold = st.slider(
        "Maximum Price ($)",
        min_value=min_price,
        max_value=max_price,
        value=max_price
    )
    
    st.markdown("---")
    st.markdown("### 📊 Quick Stats")
    
    # Quick stats - FIXED: SILVER is correct for fact table
    total_flights_df = get_dataframe("SELECT COUNT(*) as CNT FROM ANALYTICS_DB.SILVER.FCT_DAILY_FLIGHT_OFFERS")
    total_flights = total_flights_df['CNT'].iloc[0] if not total_flights_df.empty else 0
    
    total_routes_df = get_dataframe("SELECT COUNT(DISTINCT ROUTE_ID) as CNT FROM ANALYTICS_DB.SILVER.FCT_DAILY_FLIGHT_OFFERS")
    total_routes = total_routes_df['CNT'].iloc[0] if not total_routes_df.empty else 0
    
    total_airlines_df = get_dataframe("SELECT COUNT(DISTINCT AIRLINE_ID) as CNT FROM ANALYTICS_DB.SILVER.FCT_DAILY_FLIGHT_OFFERS")
    total_airlines = total_airlines_df['CNT'].iloc[0] if not total_airlines_df.empty else 0
    
    col_s1, col_s2 = st.columns(2)
    with col_s1:
        st.metric("Total Flights", f"{total_flights:,}")
        st.metric("Airlines", total_airlines)
    with col_s2:
        st.metric("Unique Routes", f"{total_routes:,}")
    
    st.markdown("---")
    st.markdown("📅 March 2026")

# ---------------------------- MAIN CONTENT ----------------------------
st.markdown('<p class="main-header">✈️ Flight Analytics Pro</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Interactive Dashboard for 100,000+ Daily Flights</p>', unsafe_allow_html=True)

# ---------------------------- TOP METRICS ----------------------------
col1, col2, col3, col4 = st.columns(4)

with col1:
    # FIXED: GOLD for airline_performance, STAGING for stg_airlines
    cheapest_airline = get_dataframe("""
        SELECT a.AIRLINE_NAME, ROUND(AVG(f.AVG_PRICE), 2) as AVG_PRICE
        FROM ANALYTICS_DB.GOLD.AIRLINE_PERFORMANCE f
        JOIN ANALYTICS_DB.STAGING.STG_AIRLINES a ON f.AIRLINE_ID = a.AIRLINE_ID
        GROUP BY a.AIRLINE_NAME
        ORDER BY AVG_PRICE
        LIMIT 1
    """)
    if not cheapest_airline.empty:
        airline_code = cheapest_airline['AIRLINE_NAME'].iloc[0]
        airline_full = get_airline_full_name(airline_code)
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{airline_full}</div>
            <div class="metric-label">💰 Cheapest Airline (${cheapest_airline['AVG_PRICE'].iloc[0]})</div>
        </div>
        """, unsafe_allow_html=True)

with col2:
    # FIXED: GOLD for route_pricing_analysis
    most_volatile = get_dataframe("""
        SELECT 
            ROUTE_ID,
            PRICE_VOLATILITY_INDEX
        FROM ANALYTICS_DB.GOLD.ROUTE_PRICING_ANALYSIS
        WHERE PRICE_VOLATILITY_INDEX IS NOT NULL
        ORDER BY PRICE_VOLATILITY_INDEX DESC
        LIMIT 1
    """)
    if not most_volatile.empty:
        route_name = format_route_name(most_volatile["ROUTE_ID"].iloc[0])
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{route_name}</div>
            <div class="metric-label">📈 Most Volatile ({most_volatile['PRICE_VOLATILITY_INDEX'].iloc[0]:.2f}%)</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="metric-card">
            <div class="metric-value">-</div>
            <div class="metric-label">📈 Most Volatile</div>
        </div>
        """, unsafe_allow_html=True)

with col3:
    # FIXED: SILVER is correct for fact table
    top_route = get_dataframe("""
        SELECT 
            ROUTE_ID,
            COUNT(*) as FLIGHT_COUNT
        FROM ANALYTICS_DB.SILVER.FCT_DAILY_FLIGHT_OFFERS
        GROUP BY ROUTE_ID
        ORDER BY FLIGHT_COUNT DESC
        LIMIT 1
    """)
    if not top_route.empty:
        route_name = format_route_name(top_route["ROUTE_ID"].iloc[0])
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{route_name}</div>
            <div class="metric-label">🔥 Busiest Route ({top_route['FLIGHT_COUNT'].iloc[0]} flights)</div>
        </div>
        """, unsafe_allow_html=True)

with col4:
    # FIXED: GOLD for airline_performance, STAGING for stg_airlines
    market_leader = get_dataframe("""
        SELECT a.AIRLINE_NAME, ROUND(AVG(f.MARKET_SHARE_PERCENTAGE), 2) as AVG_SHARE
        FROM ANALYTICS_DB.GOLD.AIRLINE_PERFORMANCE f
        JOIN ANALYTICS_DB.STAGING.STG_AIRLINES a ON f.AIRLINE_ID = a.AIRLINE_ID
        GROUP BY a.AIRLINE_NAME
        ORDER BY AVG_SHARE DESC
        LIMIT 1
    """)
    if not market_leader.empty:
        airline_code = market_leader['AIRLINE_NAME'].iloc[0]
        airline_full = get_airline_full_name(airline_code)
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{airline_full}</div>
            <div class="metric-label">👑 Market Leader ({market_leader['AVG_SHARE'].iloc[0]}%)</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("---")

# ---------------------------- MAIN CHARTS ----------------------------
tab1, tab2, tab3, tab4 = st.tabs(["🗺️ Global Flight Map", "📊 Airline Performance", "🛣️ Route Analysis", "🔄 Alternative Airports"])

with tab1:
    st.subheader("🌍 Interactive Flight Routes Map")
    st.markdown("*Click on routes to see details*")
    
    # Get sample of routes for map
    map_data = get_dataframe("""
        SELECT 
            CONCAT(ORIGIN, '-', DESTINATION) as ROUTE_ID,
            ORIGIN,
            DESTINATION,
            AVG(PRICE) as AVG_PRICE,
            COUNT(*) as FLIGHT_COUNT
        FROM ANALYTICS_DB.RAW.REAL_FLIGHTS
        GROUP BY ORIGIN, DESTINATION
        ORDER BY FLIGHT_COUNT DESC
        LIMIT 500
    """)
    
    if not map_data.empty:
        # Create map
        m = folium.Map(location=[20, 0], zoom_start=2, tiles='CartoDB dark_matter')
        
        # Add routes as lines
        for _, row in map_data.iterrows():
            # Get city names for display
            origin_city = get_city_name(row['ORIGIN'])
            dest_city = get_city_name(row['DESTINATION'])
            route_display = f"{origin_city} - {dest_city}"
            
            # Random coordinates for demo (in production, use actual lat/lon)
            origin_coords = [random.uniform(-40, 60), random.uniform(-120, 150)]
            dest_coords = [random.uniform(-40, 60), random.uniform(-120, 150)]
            
            folium.PolyLine(
                [origin_coords, dest_coords],
                color='#FF4B4B',
                weight=1,
                opacity=0.5,
                popup=f"Route: {route_display}<br>Avg Price: ${row['AVG_PRICE']:.0f}<br>Flights: {row['FLIGHT_COUNT']}"
            ).add_to(m)
            
            # Add markers for major airports with city names
            if row['FLIGHT_COUNT'] > 100:
                folium.CircleMarker(
                    origin_coords,
                    radius=2,
                    color='#3388ff',
                    fill=True,
                    popup=f"{origin_city}",
                    tooltip=origin_city
                ).add_to(m)
        
        folium_static(m, width=1200, height=600)
    else:
        st.warning("No route data available for map visualization")

with tab2:
    st.subheader("📊 Airline Performance Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # FIXED: GOLD for airline_performance, STAGING for stg_airlines
        airline_pricing = get_dataframe("""
            SELECT 
                a.AIRLINE_NAME,
                ROUND(AVG(f.AVG_PRICE), 2) as AVG_PRICE,
                ROUND(AVG(f.MARKET_SHARE_PERCENTAGE), 2) as MARKET_SHARE
            FROM ANALYTICS_DB.GOLD.AIRLINE_PERFORMANCE f
            JOIN ANALYTICS_DB.STAGING.STG_AIRLINES a ON f.AIRLINE_ID = a.AIRLINE_ID
            GROUP BY a.AIRLINE_NAME
            ORDER BY AVG_PRICE
            LIMIT 15
        """)
        
        if not airline_pricing.empty:
            # Add full airline names
            airline_pricing['FULL_NAME'] = airline_pricing['AIRLINE_NAME'].apply(get_airline_full_name)
            
            fig = px.bar(
                airline_pricing,
                x='FULL_NAME',
                y='AVG_PRICE',
                color='AVG_PRICE',
                title='Average Ticket Price by Airline',
                labels={'FULL_NAME': 'Airline', 'AVG_PRICE': 'Avg Price ($)'},
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No airline pricing data available")
    
    with col2:
        # Market share pie chart
        if not airline_pricing.empty and len(airline_pricing) > 0:
            fig = px.pie(
                airline_pricing.head(8),
                values='MARKET_SHARE',
                names='FULL_NAME',
                title='Market Share Distribution (Top 8 Airlines)',
                hole=0.4
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No market share data available")
    
    # Price vs Market chart - FIXED: GOLD for airline_performance, STAGING for stg_airlines
    price_vs_market = get_dataframe("""
        SELECT 
            a.AIRLINE_NAME,
            ROUND(AVG(f.PRICE_VS_MARKET), 2) as PRICE_VS_MARKET
        FROM ANALYTICS_DB.GOLD.AIRLINE_PERFORMANCE f
        JOIN ANALYTICS_DB.STAGING.STG_AIRLINES a ON f.AIRLINE_ID = a.AIRLINE_ID
        GROUP BY a.AIRLINE_NAME
        ORDER BY PRICE_VS_MARKET
    """)
    
    if not price_vs_market.empty:
        price_vs_market['FULL_NAME'] = price_vs_market['AIRLINE_NAME'].apply(get_airline_full_name)
        
        fig = px.bar(
            price_vs_market,
            x='FULL_NAME',
            y='PRICE_VS_MARKET',
            title='Price vs Market Average ($)',
            color='PRICE_VS_MARKET',
            color_continuous_scale='RdYlGn',
            labels={'FULL_NAME': 'Airline', 'PRICE_VS_MARKET': 'Price Difference ($)'}
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No price vs market data available")

with tab3:
    st.subheader("🛣️ Route Pricing Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # FIXED: GOLD for route_pricing_analysis
        volatility_data = get_dataframe("""
            SELECT VOLATILITY_CATEGORY, COUNT(*) as COUNT
            FROM ANALYTICS_DB.GOLD.ROUTE_PRICING_ANALYSIS
            GROUP BY VOLATILITY_CATEGORY
        """)
        
        if not volatility_data.empty:
            fig = px.pie(
                volatility_data,
                values='COUNT',
                names='VOLATILITY_CATEGORY',
                title='Route Volatility Distribution',
                color='VOLATILITY_CATEGORY',
                color_discrete_map={'Low': '#00CC96', 'Medium': '#FFA15A', 'High': '#EF553B'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No volatility data available")
    
    with col2:
        # FIXED: GOLD for route_pricing_analysis
        top_volatile = get_dataframe("""
            SELECT 
                ROUTE_ID,
                PRICE_VOLATILITY_INDEX,
                AVG_PRICE
            FROM ANALYTICS_DB.GOLD.ROUTE_PRICING_ANALYSIS
            WHERE PRICE_VOLATILITY_INDEX IS NOT NULL
            ORDER BY PRICE_VOLATILITY_INDEX DESC
            LIMIT 10
        """)
        
        if not top_volatile.empty:
            # Add city names column
            top_volatile['ROUTE_DISPLAY'] = top_volatile['ROUTE_ID'].apply(format_route_name)
            
            fig = px.bar(
                top_volatile,
                x='ROUTE_DISPLAY',
                y='PRICE_VOLATILITY_INDEX',
                title='Top 10 Most Volatile Routes',
                color='AVG_PRICE',
                labels={'ROUTE_DISPLAY': 'Route', 'PRICE_VOLATILITY_INDEX': 'Volatility Index (%)', 'AVG_PRICE': 'Avg Price ($)'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No volatile routes data available")
    
    # FIXED: GOLD for route_pricing_analysis
    window_data = get_dataframe("""
        SELECT 
            ROUTE_ID,
            CHEAPEST_WINDOW_START,
            CHEAPEST_WINDOW_END,
            AVG_PRICE
        FROM ANALYTICS_DB.GOLD.ROUTE_PRICING_ANALYSIS
        WHERE CHEAPEST_WINDOW_START IS NOT NULL 
          AND CHEAPEST_WINDOW_END IS NOT NULL
        ORDER BY AVG_PRICE
        LIMIT 20
    """)
    
    if not window_data.empty:
        # Add city names column
        window_data['ROUTE_DISPLAY'] = window_data['ROUTE_ID'].apply(format_route_name)
        
        fig = px.scatter(
            window_data,
            x='CHEAPEST_WINDOW_START',
            y='AVG_PRICE',
            size='CHEAPEST_WINDOW_END',
            color='AVG_PRICE',
            hover_name='ROUTE_DISPLAY',
            title='Optimal Booking Windows vs Price',
            labels={
                'CHEAPEST_WINDOW_START': 'Booking Window Start (days)',
                'AVG_PRICE': 'Average Price ($)',
                'CHEAPEST_WINDOW_END': 'Window End (days)'
            }
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No booking window data available")

with tab4:
    st.subheader("🔄 Alternative Airports")
    
    # FIXED: GOLD for alternative_airports
    alt_airports = get_dataframe("""
        SELECT 
            CITY_NAME,
            COUNTRY_CODE,
            PRIMARY_AIRPORT,
            ALTERNATIVE_AIRPORT,
            PRIMARY_AVG_PRICE,
            ALTERNATIVE_AVG_PRICE,
            (ALTERNATIVE_AVG_PRICE - PRIMARY_AVG_PRICE) as PRICE_DIFF
        FROM ANALYTICS_DB.GOLD.ALTERNATIVE_AIRPORTS
        WHERE ALTERNATIVE_AIRPORT IS NOT NULL
        ORDER BY ABS(PRICE_DIFF) DESC
    """)
    
    if not alt_airports.empty:
        st.success(f"✅ Found {len(alt_airports)} cities with alternative airports")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                alt_airports.head(10),
                x='CITY_NAME',
                y='PRICE_DIFF',
                title='Price Difference: Alternative vs Primary Airport',
                color='PRICE_DIFF',
                labels={'CITY_NAME': 'City', 'PRICE_DIFF': 'Price Difference ($)'},
                color_continuous_scale='RdYlGn'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.dataframe(
                alt_airports[['CITY_NAME', 'PRIMARY_AIRPORT', 'ALTERNATIVE_AIRPORT', 
                             'PRIMARY_AVG_PRICE', 'ALTERNATIVE_AVG_PRICE', 'PRICE_DIFF']],
                use_container_width=True,
                hide_index=True
            )
    else:
        st.info("ℹ️ No alternative airports found in current dataset")
        
        # Show sample of airports with city names
        with st.expander("🔍 Show available airports"):
            sample_airports = get_dataframe("""
                SELECT AIRPORT_ID, CITY_NAME 
                FROM ANALYTICS_DB.STAGING.STG_AIRPORTS 
                WHERE CITY_NAME IS NOT NULL 
                LIMIT 10
            """)
            if not sample_airports.empty:
                st.dataframe(sample_airports)

# ---------------------------- FOOTER ----------------------------
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #6B7280; padding: 20px;'>
        ✈️ Flight Analytics Pro • Powered by Python + Streamlit + Snowflake • © 2026
    </div>
    """,
    unsafe_allow_html=True
)