import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

# Page configuration
st.set_page_config(page_title="BigQuery Explorer", page_icon="📊")
st.title("BigQuery Data Explorer")
st.write("This app securely connects to BigQuery and displays data.")

# Create API client
@st.cache_resource
def get_client():
    """Create an authenticated BigQuery client"""
    try:
        # Create credentials from secrets
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        
        # Create and return the BigQuery client
        return bigquery.Client(credentials=credentials)
    except Exception as e:
        st.error(f"Error creating BigQuery client: {e}")
        return None

# Initialize the client
client = get_client()

if client is not None:
    # Example query section
    st.header("Sample Query")
    
    st.write("Let's query a public dataset as an example:")
    
    # Define a simple query
    default_query = """
        SELECT name, gender, SUM(number) as total_count
        FROM `bigquery-public-data.usa_names.usa_1910_2013`
        WHERE state = 'TX'
        GROUP BY name, gender
        ORDER BY total_count DESC
        LIMIT 10
    """
    
    # Create a text area for the query
    query = st.text_area("SQL Query", value=default_query, height=200)
    
    # Run query button
    if st.button("Run Query"):
        try:
            st.info("Running query...")
            
            # Execute the query
            df = client.query(query).to_dataframe()
            
            # Display results
            st.success(f"Query returned {len(df)} rows")
            st.dataframe(df)
            
            # Display a chart
            if len(df) > 0:
                st.subheader("Visualization")
                chart_type = st.selectbox(
                    "Select chart type", 
                    ["Bar Chart", "Line Chart", "Area Chart"]
                )
                
                if chart_type == "Bar Chart":
                    st.bar_chart(df.set_index('name')['total_count'])
                elif chart_type == "Line Chart":
                    st.line_chart(df.set_index('name')['total_count'])
                elif chart_type == "Area Chart":
                    st.area_chart(df.set_index('name')['total_count'])
                
        except Exception as e:
            st.error(f"Error executing query: {e}")
    
    # Custom query section
    st.header("Parameterized Query Example")
    st.write("This is a safer way to query with user inputs:")
    
    # Get user input
    state = st.selectbox("Select state:", ["TX", "CA", "NY", "FL", "IL"])
    name_prefix = st.text_input("Name starts with:", "Jo")
    limit = st.slider("Number of results:", 1, 50, 10)
    
    if st.button("Run Parameterized Query"):
        try:
            # Define the query with parameters
            query_with_params = """
                SELECT name, gender, SUM(number) as total_count
                FROM `bigquery-public-data.usa_names.usa_1910_2013`
                WHERE state = @state
                AND name LIKE @name_prefix
                GROUP BY name, gender
                ORDER BY total_count DESC
                LIMIT @limit
            """
            
            # Configure the query parameters
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("state", "STRING", state),
                    bigquery.ScalarQueryParameter("name_prefix", "STRING", name_prefix + "%"),
                    bigquery.ScalarQueryParameter("limit", "INT64", limit),
                ]
            )
            
            # Run the query
            st.info("Running parameterized query...")
            df = client.query(query_with_params, job_config=job_config).to_dataframe()
            
            # Display results
            st.success(f"Query returned {len(df)} rows")
            st.dataframe(df)
            
        except Exception as e:
            st.error(f"Error executing query: {e}")

else:
    st.error("Failed to initialize BigQuery client. Please check your credentials.")

# Add some additional information
st.sidebar.header("About")
st.sidebar.info("""
This app demonstrates how to securely connect a Streamlit application to Google BigQuery.
It uses service account authentication and demonstrates both direct and parameterized queries.
""")

st.sidebar.header("Security Notes")
st.sidebar.warning("""
- Credentials are stored in .streamlit/secrets.toml
- Never commit this file to version control
- Use parameterized queries to prevent SQL injection
- Grant minimal necessary permissions to your service account
""")