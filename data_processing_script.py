import pandas as pd
import psycopg2
import numpy as np
import re
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

# Load the JSON data into a pandas DataFrame
df = pd.read_json('amazon_data_ext.json')

# Define variations of NaN or missing values and replace in customer comment columns
nan_variants = [np.nan, 'NaN', 'nan', 'None', 'none', 'N/A', 'n/a', 'NA', 'na', 'null', '']

# Replace NaN values with 'Unavailable' in specific columns
columns_to_replace_nan = [
    'Critical_Review_Cust_ID', 'Critical_Review_Cust_Name', 'Critical_Review_Cust_Comment',
    'Critical_Review_Cust_Comment_Title', 
    'Top_Positive_Review_Cust_ID', 'Top_Positive_Review_Cust_Name', 'Top_Positive_Review_Cust_Comment',
    'Top_Positive_Review_Cust_Comment_Title' 
]
for column in columns_to_replace_nan:
    if column in df.columns:
        df[column] = df[column].replace('(', '').replace(')', '').replace(nan_variants, "Unavailable").fillna("Unavailable")
    else:
        print(f"Column '{column}' not found in the DataFrame.")

# Rest of the code remains unchanged

# ---- START OF INSERTED CODE ----
def fix_products_length(df):
    """Ensure each record has a length of 42 by appending None values."""
    max_len = df.shape[1]
    if max_len < 42:
        for _ in range(42 - max_len):
            df[f'Extra_Column_{_}'] = None
    return df

# Call the function to ensure data consistency
df = fix_products_length(df)
# ---- END OF INSERTED CODE ----

# Check if specific columns are in the DataFrame
columns_to_check = ['Critical_Review_Cust_Influenced', 'Top_Positive_Review_Cust_Influenced']
for column in columns_to_check:
    if column not in df.columns:
        logging.warning(f"Column '{column}' not found in the DataFrame. Please check the column name in the JSON file.")

# Convert date columns to datetime objects and then to 'yyyy-mm-dd' string format
date_columns = ['Critical_Review_Cust_Date', 'Top_Positive_Review_Cust_Date'] + [f'Customer_{i}_Date' for i in range(1, 6)]
for column in date_columns:
    df[column] = pd.to_datetime(df[column], errors='coerce', format='%Y-%m-%dT%H:%M:%S')
    df[column].fillna(pd.NaT, inplace=True)
    df[column] = df[column].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '1677-09-21')

# Replace NaN values with 'None' in specific columns
columns_to_replace_nan = [
    'Critical_Review_Cust_ID', 'Critical_Review_Cust_Name', 'Critical_Review_Cust_Comment',
    'Critical_Review_Cust_Comment_Title', 'Critical_Review_Cust_Influenced',
    'Top_Positive_Review_Cust_ID', 'Top_Positive_Review_Cust_Name', 'Top_Positive_Review_Cust_Comment',
    'Top_Positive_Review_Cust_Comment_Title', 'Top_Positive_Review_Cust_Influenced'
]
for column in columns_to_replace_nan:
    df[column] = df[column].replace({np.nan: 'None'})



# Remove any duplicates that may have been created due to URL changes
df = df.drop_duplicates(subset=['Product_ID'], keep='first')

# Replace NaN values with 'None' in customer comment and ID columns
for i in range(1, 6):
    df[f'Customer_{i}_Comment'] = df[f'Customer_{i}_Comment'].replace({np.nan: 'Unavailable'})
    df[f'Customer_{i}_ID'] = df[f'Customer_{i}_ID'].replace({np.nan: 'Unavailable'})

# Define variations of NaN or missing values and replace in customer comment columns
nan_variants = [np.nan, 'NaN', 'nan', 'None', 'none', 'N/A', 'n/a', 'NA', 'na', 'null', '']
for i in range(1, 6):
    col_name = f'Customer_{i}_Comment'
    df[col_name] = df[col_name].astype(str).replace(nan_variants, 'None')


# Update the 'Critical_Review_Cust_Influenced' and 'Top_Positive_Review_Cust_Influenced' columns
for column in ['Critical_Review_Cust_Influenced', 'Top_Positive_Review_Cust_Influenced']:
    df[column] = df[column].replace({'"NaN"': 0.0, 'NaN': 0.0, 'None': 0.0})

# Drop the 'review_responders' column if it exists
if 'review_responders' in df.columns:
    df.drop(columns=['review_responders'], inplace=True)

# Clean other columns
def safe_float_conversion(value):
    try:
        return float(value)
    except:
        return 0.0

df['price'] = df['price'].apply(safe_float_conversion)
df['ratings'] = df['ratings'].apply(lambda x: float(x) if pd.notna(x) and x != '' else None)
df['reviews'] = df['reviews'].replace(nan_variants, 0).astype(int)

df['ratings'] = df['ratings'].replace('(', '').replace(')', '').replace(nan_variants, 0).fillna(0).astype(float)

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="demopass",
    client_encoding='utf8'
)
cur = conn.cursor()

# Modify the CREATE TABLE query to include additional columns
create_table_query = """
DROP TABLE IF EXISTS amazon_data_ext;
CREATE TABLE IF NOT EXISTS amazon_data_ext (
    Product_ID TEXT NOT NULL,
    product TEXT NOT NULL,
    price NUMERIC NULL,
    ratings NUMERIC NULL,
    reviews INTEGER NOT NULL,
    category TEXT NOT NULL,
    url TEXT NOT NULL,
    Top_Positive_Review_Cust_ID TEXT,
    Top_Positive_Review_Cust_Name TEXT,
    Top_Positive_Review_Cust_Date DATE,
    Top_Positive_Review_Cust_Comment TEXT,
    Top_Positive_Review_Cust_Comment_Title TEXT,
    Top_Positive_Review_Cust_Influenced INTEGER,
    Top_Positive_Review_Cust_Star_Rating NUMERIC,
    Critical_Review_Cust_ID TEXT,
    Critical_Review_Cust_Name TEXT,
    Critical_Review_Cust_Date DATE,
    Critical_Review_Cust_Comment TEXT,
    Critical_Review_Cust_Comment_Title TEXT,
    Critical_Review_Cust_Influenced INTEGER,
    Critical_Review_Cust_Star_Rating NUMERIC,
    """ + ",\n    ".join([f"Customer_{i}_ID TEXT, Customer_{i}_Star_Rating NUMERIC, Customer_{i}_Comment TEXT, Customer_{i}_buying_influence INTEGER, Customer_{i}_Date DATE" for i in range(1, 6)]) + """
)
"""
cur.execute(create_table_query)
conn.commit()

def clean_format_data(row):
    # Extract values directly, as they are already cleaned
    ratings = row['ratings']
    price = row['price']
    reviews = row['reviews']
    product_id = row['Product_ID']
    product = row['product']
    category = row['category']
    url = row['url']
      
    critical_review_id = row['Critical_Review_Cust_ID'] if row['Critical_Review_Cust_ID'] != 'None' else None
    critical_review_cust_name = row['Critical_Review_Cust_Name'] if row['Critical_Review_Cust_Name'] != 'None' else None
    critical_review_cust_comment = row['Critical_Review_Cust_Comment']
    if critical_review_cust_comment in nan_variants or critical_review_cust_comment == 'None':
        critical_review_cust_comment = 'Unavailable'
    
    critical_review_cust_comment_title = row['Critical_Review_Cust_Comment_Title'] if row['Critical_Review_Cust_Comment_Title'] != 'None' else None
    critical_review_cust_influenced = row['Critical_Review_Cust_Influenced'] if row['Critical_Review_Cust_Influenced'] != 'None' else 0  # Correctly handle NaN values
    critical_review_star_rating = row['Critical_Review_Cust_Star_Rating'] if pd.notna(row['Critical_Review_Cust_Star_Rating']) else 0.0
    critical_review_cust_date = row['Critical_Review_Cust_Date'] if row['Critical_Review_Cust_Date'] != 'None' else '0001-01-01'  # Correctly handle NaN values

    top_positive_review_id = row['Top_Positive_Review_Cust_ID'] if row['Top_Positive_Review_Cust_ID'] != 'None' else None
    top_positive_review_cust_name = row['Top_Positive_Review_Cust_Name'] if row['Top_Positive_Review_Cust_Name'] != 'None' else None
    top_positive_review_cust_comment = row['Critical_Review_Cust_Comment']
    if top_positive_review_cust_comment in nan_variants or top_positive_review_cust_comment == 'None':
        top_positive_review_cust_comment = None    
    top_positive_review_cust_comment_title = row['Top_Positive_Review_Cust_Comment_Title'] if row['Top_Positive_Review_Cust_Comment_Title'] != 'None' else None
    top_positive_review_cust_influenced = row['Top_Positive_Review_Cust_Influenced'] if row['Top_Positive_Review_Cust_Influenced'] != 'None' else 0  # Correctly handle NaN values
    top_positive_review_star_rating = row['Top_Positive_Review_Cust_Star_Rating'] if pd.notna(row['Top_Positive_Review_Cust_Star_Rating']) else 0.0
    top_positive_review_cust_date = row['Top_Positive_Review_Cust_Date'] if row['Top_Positive_Review_Cust_Date'] != 'None' else '0001-01-01'  # Correctly handle NaN values
    
    top_positive_date = row['Top_Positive_Review_Cust_Date']
    critical_review_date = row['Critical_Review_Cust_Date']

    def format_date(date_value):
        if isinstance(date_value, str) and re.match(r'\d{4}-\d{2}-\d{2}', date_value):
            return date_value
        else:
            return '0001-01-01'  # Default value for invalid date formats or non-string values

    top_positive_review_cust_date = format_date(top_positive_date)
    critical_review_cust_date = format_date(critical_review_date)



    # Handle additional customer information
    customer_data = []
    for i in range(1, 6):
        customer_id = row[f'Customer_{i}_ID'] if row[f'Customer_{i}_ID'] != 'None' else "Unavailable"
        star_rating = row[f'Customer_{i}_Star_Rating'] if pd.notna(row[f'Customer_{i}_Star_Rating']) else 0.0
        comment = row[f'Customer_{i}_Comment'] if row[f'Customer_{i}_Comment'] != 'None' else "Unavailable"
        buying_influence = row[f'Customer_{i}_buying_influence'] if pd.notna(row[f'Customer_{i}_buying_influence']) else 0
        customer_date = row[f'Customer_{i}_Date'] if row[f'Customer_{i}_Date'] != 'None' else '0001-01-01'  # Correctly handle NaN values
        customer_data.extend([customer_id, star_rating, comment, buying_influence, customer_date])

    # Construct the return tuple
    result_tuple = (product_id, product, price, ratings, reviews, category, url, 
                   top_positive_review_id, top_positive_review_cust_name, top_positive_review_cust_date, 
                   top_positive_review_cust_comment, top_positive_review_cust_comment_title, 
                   top_positive_review_cust_influenced, top_positive_review_star_rating, 
                   critical_review_id, critical_review_cust_name, critical_review_cust_date, critical_review_cust_comment, 
                   critical_review_cust_comment_title, critical_review_cust_influenced, 
                   critical_review_star_rating, *customer_data)
    
    if not result_tuple:
        logging.error(f"Failed to construct tuple for row: {row}")
        return None
    
    return result_tuple

# Check for the presence of the column `Customer_{i}_buying_influence` in the DataFrame
for i in range(1, 6):
    if f'Customer_{i}_buying_influence' not in df.columns:
        logging.error(f"Column 'Customer_{i}_buying_influence' not found in the DataFrame.")


# Define the INSERT query
insert_query = """
INSERT INTO amazon_data_ext (
    Product_ID, product, price, ratings, reviews, category, url,
    Top_Positive_Review_Cust_ID, Top_Positive_Review_Cust_Name, Top_Positive_Review_Cust_Date, Top_Positive_Review_Cust_Comment, Top_Positive_Review_Cust_Comment_Title, Top_Positive_Review_Cust_Influenced, Top_Positive_Review_Cust_Star_Rating, Critical_Review_Cust_ID, Critical_Review_Cust_Name, Critical_Review_Cust_Date, Critical_Review_Cust_Comment, Critical_Review_Cust_Comment_Title, Critical_Review_Cust_Influenced, Critical_Review_Cust_Star_Rating,
    """ + ", ".join([f"Customer_{i}_ID, Customer_{i}_Star_Rating, Customer_{i}_Comment, Customer_{i}_buying_influence, Customer_{i}_Date" for i in range(1, 6)]) + """
) VALUES (""" + ", ".join(["%s"] * (21 + 25)) + ")"


# Count the number of placeholders in the SQL query
num_placeholders = insert_query.count('%s')

for index, row in df.iterrows():
    tuple_values = clean_format_data(row)
    if not tuple_values:
        logging.warning(f"Skipping row at index {index} due to errors in data processing.")
        continue
    num_tuple_values = len(tuple_values)
    
    # Check for mismatch between placeholders and tuple values
    if num_placeholders != num_tuple_values:
        logging.error(f"Mismatch at index {index}! Number of placeholders: {num_placeholders}, Number of tuple values: {num_tuple_values}")
        logging.error(f"Tuple values: {tuple_values}")
        
        # Expected columns based on the INSERT query
        # Expected columns based on the INSERT query
        expected_columns = [
            "Product_ID", "product", "price", "ratings", "reviews", "category", "url",
            "Top_Positive_Review_Cust_ID", "Top_Positive_Review_Cust_Name", "Top_Positive_Review_Cust_Date", "Top_Positive_Review_Cust_Comment", "Top_Positive_Review_Cust_Comment_Title", "Top_Positive_Review_Cust_Influenced",
            "Top_Positive_Review_Cust_Star_Rating", "Critical_Review_Cust_ID", "Critical_Review_Cust_Name", "Critical_Review_Cust_Date", "Critical_Review_Cust_Comment", "Critical_Review_Cust_Comment_Title",
            "Critical_Review_Cust_Influenced", "Critical_Review_Cust_Star_Rating"
        ] + [f"Customer_{i}_ID" for i in range(1, 6)] + [f"Customer_{i}_Star_Rating" for i in range(1, 6)] + [f"Customer_{i}_Comment" for i in range(1, 6)] + [f"Customer_{i}_buying_influence" for i in range(1, 6)] + [f"Customer_{i}_Date" for i in range(1, 6)]



        # In the section where you're logging the mismatch error, add this:
        for col, val in zip(expected_columns, tuple_values):
            print(f"{col}: {val}")

        
        continue  # Skip this iteration


    try:
        cur.execute(insert_query, tuple_values)
    except Exception as e:
        logging.error(f"Error inserting row at index {index}: {e}")
        logging.debug(f"Row data: {row}")
        conn.rollback()

        

conn.commit()
cur.close()
conn.close()
# Rename the columns in the DataFrame
df.rename(columns={'ratings': 'star_ratings', 'reviews': 'total_ratings', 'price': 'price_dollars'}, inplace=True)

# Save the DataFrame to a CSV file with updated column names
df.to_csv('amazon_data_ext.csv', index=False, encoding='utf-8')