import psycopg2
from psycopg2 import sql
from psycopg2 import extras
import pandas as pd
from tqdm import tqdm
import json

def db_connection():
    connection_url = "postgres://wireuser:IjXSnSsZF54ubK3De9IeEwJkgLKlWtIw@dpg-cnucii5jm4es739up960-a.oregon-postgres.render.com/wirenews"

    # Connect to your PostgreSQL database
    conn = psycopg2.connect(connection_url)

    # Create a cursor object
    cur = conn.cursor()

    return conn, cur



def create_table():
    
    try:
        conn, cur = db_connection()

        # Create a table
        create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS wirenews (
            id SERIAL PRIMARY KEY, 
            title TEXT, 
            url VARCHAR(255),
            date TIMESTAMP WITHOUT TIME ZONE, 
            company VARCHAR(255), 
            symbol VARCHAR(50), 
            exchange VARCHAR(20), 
            region VARCHAR(20), 
            market_size VARCHAR(20), 
            sector VARCHAR(255), 
            industry VARCHAR(255), 
            logo_url VARCHAR(255), 
            raw_text TEXT, 
            content TEXT, 
            html_content TEXT, 
            article_url VARCHAR(255),
            date_str VARCHAR(50)
            
        )
        """)

        cur.execute(create_table_query)

        # Commit the transaction
        conn.commit()

        # Close the cursor and the connection
        cur.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"An error occurred: {e}")



def drop_table():

    # Connect to your PostgreSQL database
    conn, cursor = db_connection()
    
    # SQL statement to drop the table
    drop_query = "DROP TABLE IF EXISTS wirenews"
    
    # Execute the DROP TABLE query
    cursor.execute(drop_query)
    
    # Commit the changes
    conn.commit()

    cursor.close()
    conn.close()

    return None




def insert_from_df():
    # PostgreSQL connection 
    data = pd.read_csv('test_data_postgres.csv')
    data = data.drop(columns=[data.columns[0]])

    conn, cursor = db_connection()
    
    # Prepare the INSERT statement
    # Assuming all columns in 'data' DataFrame match the table's column names and order
    columns = list(data.columns)
    values_placeholder = ','.join(['%s'] * len(columns))
    insert_query = f"INSERT INTO wirenews ({', '.join(columns)}) VALUES ({values_placeholder})"
    
    print('Insert to DB')
    
    for index, row in tqdm(data.iterrows(), total=data.shape[0]):
        try:
            # Convert row to a tuple of values
            values = tuple(row.values)
            cursor.execute(insert_query, values)
        except psycopg2.DataError as e:
            print(f"DataError: {e}")
            # Handle the DataError (e.g., log the error and continue)
        except psycopg2.IntegrityError as e:
            print(f"IntegrityError: {e}")
            conn.rollback()  # Rollback the transaction on error
        except Exception as e:
            print(f"An error occurred: {e}")
            # Handle other possible exceptions
        else:
            conn.commit()  # Commit each insertion
    
    # Close the cursor and connection
    cursor.close()
    conn.close()

    return None



def get_data():
    # PostgreSQL connection 
    conn, cursor = db_connection()
    
    # Query to select all rows from the table
    select_query = "SELECT * FROM wirenews"
    
    # Execute the SELECT query
    cursor.execute(select_query)
    
    # Fetch all rows from the result
    rows = cursor.fetchall()
    
    # Get the column names from the cursor description
    columns = [desc[0] for desc in cursor.description]
    
    # Create a DataFrame from the rows and column names
    df = pd.DataFrame(rows, columns=columns)
    
    # Close the cursor and connection
    cursor.close()
    conn.close()
    
    # Convert the DataFrame to a JSON string
    json_str = df.to_json(orient='records')
    
    # Optionally, convert the JSON string to a Python dictionary
    # data_dict = json.loads(json_str)
    
    return json_str  # or return data_dict if you prefer a dictionary





