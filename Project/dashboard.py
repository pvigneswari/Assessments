import psycopg2
import pandas as pd
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.express as px
import numpy as np

# Database connection parameters
host = "ec2-18-132-73-146.eu-west-2.compute.amazonaws.com"     # Your PostgreSQL server address
port = "5432"          # Your PostgreSQL port
dbname = "testdb"   # Your database name
user = "consultants" # Your PostgreSQL username
password = "WelcomeItc@2022"   # Your PostgreSQL password

# Establish a connection to the PostgreSQL database and fetch data
try:
    connection = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    cursor = connection.cursor()
    print("Connected to the database successfully")

    # Fetch data from the fact table
    query = """
    SELECT  
        patient_id, 
        age, 
        sex, 
        bmi,
        children,
        smoker,
        region,
        medical_cost,
        insurance_company,
        policy_number,
        coverage_amount,
        insurance_cost,
        treatment,
        treatment_cost,
        hospital,
        date_of_treatment
    FROM fact_medical_data
    """

    df = pd.read_sql(query, connection)

    # Create age groups
    df['age_group'] = np.where(df['age'].between(0, 18), '0-18',
                    np.where(df['age'].between(19, 35), '19-35',
                    np.where(df['age'].between(36, 50), '36-50',
                    np.where(df['age'].between(51, 65), '51-65', '66+'))))

    # Perform aggregation to analyze average and total medical and treatment costs by age group and treatment
    age_group_analysis = df.groupby(['age_group', 'treatment']).agg(
        avg_medical_cost=pd.NamedAgg(column='medical_cost', aggfunc='mean'),
        total_medical_cost=pd.NamedAgg(column='medical_cost', aggfunc='sum'),
        avg_treatment_cost=pd.NamedAgg(column='treatment_cost', aggfunc='mean'),
        total_treatment_cost=pd.NamedAgg(column='treatment_cost', aggfunc='sum')
    ).reset_index()

    # Create BMI categories
    df['bmi_category'] = np.where(df['bmi'] < 18.5, 'Underweight',
                        np.where((df['bmi'] >= 18.5) & (df['bmi'] < 25), 'Normal weight',
                        np.where((df['bmi'] >= 25) & (df['bmi'] < 30), 'Overweight', 'Obesity')))

    # Perform aggregation to analyze average medical costs by BMI category
    bmi_cost_analysis = df.groupby('bmi_category').agg(
        avg_medical_cost=pd.NamedAgg(column='medical_cost', aggfunc='mean'),
        medical_count=pd.NamedAgg(column='medical_cost', aggfunc='count')
    ).reset_index()

    # Perform aggregation to analyze medical costs for smokers vs non-smokers
    smoker_cost_analysis = df.groupby('smoker').agg(
        avg_medical_cost=pd.NamedAgg(column='medical_cost', aggfunc='mean'),
        total_medical_cost=pd.NamedAgg(column='medical_cost', aggfunc='sum'),
        count=pd.NamedAgg(column='medical_cost', aggfunc='count')
    ).reset_index()

    # Perform aggregation for insurance premium estimation based on risk factors
    premium_estimation_df = df.groupby(['age', 'smoker']).agg(
        avg_coverage_amount=pd.NamedAgg(column='coverage_amount', aggfunc='mean'),
        avg_treatment_cost=pd.NamedAgg(column='treatment_cost', aggfunc='mean')
    ).reset_index()

    print("Insurance Premium Estimation based on risk factors:")
    print(premium_estimation_df)

    # Step 3: Determine the threshold for high-cost patients (top 10% by medical cost)
    percentile_threshold = np.percentile(df['medical_cost'], 90)  # 90th percentile

    # Step 4: Filter high-cost patients based on the determined threshold
    high_cost_patients_df = df[df['medical_cost'] >= percentile_threshold]

    # Step 5: Analyze contributing factors for high-cost patients
    # Select relevant columns for analysis
    high_cost_analysis_df = high_cost_patients_df[['patient_id', 'age', 'bmi', 'smoker', 'region', 'medical_cost', 'treatment_cost', 'treatment']]
    high_cost_analysis_df = high_cost_analysis_df.sort_values(by='medical_cost', ascending=False)

    # Step 3: Group data by the number of children to calculate average and total medical costs
    family_size_cost_analysis_df = df.groupby("children").agg(
        avg_medical_cost=('medical_cost', 'mean'),
        total_medical_cost=('medical_cost', 'sum'),
        count=('medical_cost', 'count')
    ).reset_index()

    # Round values for better readability
    family_size_cost_analysis_df['avg_medical_cost'] = family_size_cost_analysis_df['avg_medical_cost'].round(2)
    family_size_cost_analysis_df['total_medical_cost'] = family_size_cost_analysis_df['total_medical_cost'].round(2)
    


except Exception as e:
    print("Error while connecting to PostgreSQL", e)
finally:
    # Close the database connection
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")

# Initialize Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define the layout of the dashboard
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H1("Medical Cost Analysis Dashboard", className="text-center"), className="mb-4 mt-4")
    ]),
    
    # Age Group Analysis
    dbc.Row([
        dbc.Col([
            html.H5("Average Medical Costs by Age Group", className="text-center"),
            dcc.Graph(
                id='avg-medical-costs-by-age',
                figure=px.bar(
                    age_group_analysis, 
                    x='age_group', 
                    y='avg_medical_cost', 
                    color='treatment',
                    title="Average Medical Costs by Age Group and Treatment"
                )
            )
        ], width=6),

        dbc.Col([
            html.H5("Total Medical Costs by Age Group", className="text-center"),
            dcc.Graph(
                id='total-medical-costs-by-age',
                figure=px.bar(
                    age_group_analysis, 
                    x='age_group', 
                    y='total_medical_cost', 
                    color='treatment',
                    title="Total Medical Costs by Age Group and Treatment"
                )
            )
        ], width=6)
    ]),
    
    # BMI Category Analysis
    dbc.Row([
        dbc.Col(html.H1("Impact of BMI on Medical Costs", className="text-center"), className="mb-4 mt-4")
    ]),
    dbc.Row([
        dbc.Col([
            html.H5("Average Medical Costs by BMI Category", className="text-center"),
            dcc.Graph(
                id='avg-medical-costs-by-bmi',
                figure=px.bar(
                    bmi_cost_analysis,
                    x='bmi_category',
                    y='avg_medical_cost',
                    title="Average Medical Costs by BMI Category"
                )
            )
        ], width=6),
        
        dbc.Col([
            html.H5("Medical Count by BMI Category", className="text-center"),
            dcc.Graph(
                id='medical-count-by-bmi',
                figure=px.bar(
                    bmi_cost_analysis,
                    x='bmi_category',
                    y='medical_count',
                    title="Medical Count by BMI Category"
                )
            )
        ], width=6)
    ]),

    # Smoker Analysis
    dbc.Row([
        dbc.Col(html.H1("Medical Costs for Smokers vs Non-Smokers", className="text-center"), className="mb-4 mt-4")
    ]),
    dbc.Row([
        dbc.Col([
            html.H5("Average Medical Costs by Smoking Status", className="text-center"),
            dcc.Graph(
                id='avg-medical-costs-by-smoker',
                figure=px.bar(
                    smoker_cost_analysis,
                    x='smoker',
                    y='avg_medical_cost',
                    title="Average Medical Costs by Smoking Status"
                )
            )
        ], width=6),
        
        dbc.Col([
            html.H5("Total Medical Costs by Smoking Status", className="text-center"),
            dcc.Graph(
                id='total-medical-costs-by-smoker',
                figure=px.bar(
                    smoker_cost_analysis,
                    x='smoker',
                    y='total_medical_cost',
                    title="Total Medical Costs by Smoking Status"
                )
            )
        ], width=6)
    ]),

    # Premium Estimation Analysis
    dbc.Row([
        dbc.Col(html.H1("Insurance Premium Estimation", className="text-center"), className="mb-4 mt-4")
    ]),
    dbc.Row([
        dbc.Col([
            html.H5("Average Coverage Amount by Age and Smoking Status", className="text-center"),
            dcc.Graph(
                id='avg-coverage-amount-by-age-smoker',
                figure=px.bar(
                    premium_estimation_df,
                    x='age',
                    y='avg_coverage_amount',
                    color='smoker',
                    title="Average Coverage Amount by Age and Smoking Status"
                )
            )
        ], width=6),
        
        dbc.Col([
            html.H5("Average Treatment Cost by Age and Smoking Status", className="text-center"),
            dcc.Graph(
                id='avg-treatment-cost-by-age-smoker',
                figure=px.bar(
                    premium_estimation_df,
                    x='age',
                    y='avg_treatment_cost',
                    color='smoker',
                    title="Average Treatment Cost by Age and Smoking Status"
                )
            )
        ], width=6)
    ]),
    # High-Cost Patient Analysis
    dbc.Row([
        dbc.Col(html.H1("High-Cost Patient Analysis", className="text-center"), className="mb-4 mt-4")
    ]),

    dbc.Row([
        dbc.Col([
            html.H5("High-Cost Patients", className="text-center"),
            dcc.Graph(
                id='high-cost-patients',
                figure=px.bar(
                    high_cost_analysis_df, 
                    x='patient_id', 
                    y='medical_cost', 
                    color='treatment',
                    title="High-Cost Patients with Medical Costs"
                )
            )
        ], width=12)
    ]),
    # Family Size Cost Analysis
    dbc.Row([
        dbc.Col(html.H1("Family Size and Medical Costs Analysis", className="text-center"), className="mb-4 mt-4")
    ]),
    dbc.Row([
        dbc.Col([
            html.H5("Average and Total Medical Costs by Number of Children", className="text-center"),
            dcc.Graph(
                id='family-size-cost-analysis',
                figure=px.bar(
                    family_size_cost_analysis_df, 
                    x='children', 
                    y=['avg_medical_cost', 'total_medical_cost'], 
                    barmode='group',
                    title="Medical Costs by Number of Children"
                )
            )
        ], width=12)
    ])
        
], fluid=True)

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)
