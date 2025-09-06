import pandas as pd
from sqlalchemy import create_engine

def clean_luxury_housing_dataset(file_path):
    # Load dataset
    df = pd.read_csv(file_path)
    print(f"Loaded dataset with {df.shape[0]} rows and {df.shape[1]} columns\n")

    # --- Step 1: Unique Values BEFORE Cleaning ---
    categorical_cols = [
        'Micro_Market',
        'Developer_Name',
        'Configuration',
        'Transaction_Type',
        'Buyer_Type',
        'Possession_Status',
        'Sales_Channel',
        'NRI_Buyer'
    ]

    print("Unique Value Summary (Before Cleaning - Top 10 per column):")
    for col in categorical_cols:
        print(f"\n🔹 Column: {col}")
        print(f"Unique values count: {df[col].nunique()}")
        print(df[col].value_counts().head(10))

    # --- Step 2: Standardize text formatting ---
    df['Micro_Market'] = df['Micro_Market'].str.strip().str.title()
    df['Developer_Name'] = df['Developer_Name'].str.strip().str.title()
    df['Configuration'] = df['Configuration'].str.upper().str.replace(" ", "")

    # --- Step 3: Handle missing values & numeric conversion ---
    df['Ticket_Price_Cr'] = pd.to_numeric(df['Ticket_Price_Cr'], errors='coerce')
    df['Unit_Size_Sqft'] = pd.to_numeric(df['Unit_Size_Sqft'], errors='coerce')

    # Fill Amenity_Score with median
    df['Amenity_Score'] = df['Amenity_Score'].fillna(df['Amenity_Score'].median())

    # Drop rows with missing critical values
    df = df.dropna(subset=['Unit_Size_Sqft', 'Ticket_Price_Cr'])


    # --- Step 4: Feature Engineering ---
    df['Ticket_Price_INR'] = df['Ticket_Price_Cr'] * 1e7
    df['Price_per_Sqft'] = df['Ticket_Price_INR'] / df['Unit_Size_Sqft']
    df['Purchase_Quarter'] = pd.to_datetime(df['Purchase_Quarter'], errors='coerce')
    df['Quarter_Number'] = df['Purchase_Quarter'].dt.to_period("Q").astype(str)
    df['Quarter_Number'] = df['Quarter_Number'].astype(str)

    df['Booking_Flag'] = df['Transaction_Type'].apply(lambda x: 1 if str(x).strip().lower() == "primary" else 0)

    # --- Step 5: Unique Values AFTER Cleaning ---
    print("\nUnique Value Summary (After Cleaning - Top 10 per column):")
    for col in categorical_cols:
        print(f"\n🔹 Column: {col}")
        print(f"Unique values count: {df[col].nunique()}")
        print(df[col].value_counts().head(10))

    # --- Step 6: Outlier Detection ---
    print("\nOutlier Ranges:")
    print("Ticket Price (Cr):", df['Ticket_Price_Cr'].min(), "-", df['Ticket_Price_Cr'].max())
    print("Unit Size (Sqft):", df['Unit_Size_Sqft'].min(), "-", df['Unit_Size_Sqft'].max())
    print("Price per Sqft:", df['Price_per_Sqft'].min(), "-", df['Price_per_Sqft'].max())

    return df

file_path = "Luxury_Housing_Bangalore.csv"
cleaned_df = clean_luxury_housing_dataset(file_path)

# Db insertion
username = "postgres"
password = "postgres"
host = "localhost"
port = "5432"
database = "luxury_housing"

# Create connection engine
engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}")

# Insert into Postgres (replace 'luxury_housing' with your table name)
cleaned_df.columns = [col.lower() for col in cleaned_df.columns]

cleaned_df.to_sql("luxury_housing", engine, if_exists="replace", index=False)

print("✅ Data loaded into PostgreSQL successfully!")

# Query 1: Total row count
query1 = "SELECT COUNT(*) FROM luxury_housing;"
rows_count = pd.read_sql(query1, engine)
print("Total Rows:\n", rows_count)

# Query 2: Count by booking_flag
query2 = 'SELECT booking_flag, COUNT(*) FROM luxury_housing GROUP BY booking_flag;'
booking_flag_count = pd.read_sql(query2, engine)
print("\nBooking Flag Distribution:\n", booking_flag_count)

# Query 3: Average price per builder
query3 = """
SELECT developer_name, AVG(ticket_price_cr) AS avg_ticket_price
FROM luxury_housing
GROUP BY developer_name
ORDER BY avg_ticket_price DESC
LIMIT 10;
"""
avg_price_per_builder = pd.read_sql(query3, engine)
print("\nTop 10 Builders by Avg Ticket Price:\n", avg_price_per_builder)

