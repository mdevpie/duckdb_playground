import pandas as pd
import duckdb
import matplotlib.pyplot as plt

# Load the CSV file into a Pandas DataFrame


# Create a DuckDB connection
con = duckdb.connect()

def main():
    # Register the DataFrame as a DuckDB table


    def load_and_process_data(con):
        # Load data from CSV into a DataFrame
        df = con.sql("""
                    SELECT * from read_csv_auto('./Code_Violations_20241027.csv', 
                     header=True,
                     filename=True,
                     types={'Zone': 'VARCHAR'})
                """).df()
        print(df.head())
        
        # Process data to get top 20 zones and crosstab
        top_20_zones = df['Zone'].value_counts().nlargest(20).index
        filtered_df = df[df['Zone'].isin(top_20_zones)]
        crosstab = pd.crosstab(filtered_df['Zone'], filtered_df['Nuisance'])
        crosstab.columns = [col[:10] for col in crosstab.columns]  # Shorten column names to first 10 characters
        print(crosstab)
        
        # Query to get violation count by Nuisance
        df2 = con.sql("""
                    SELECT Nuisance, COUNT(Nuisance) as violation_count
                    FROM df
                    GROUP BY Nuisance
                    ORDER BY violation_count DESC
                """).df()
        print(df2.head())
        
        # Query to get violation count by Zone
        df3 = con.sql("""
                    SELECT CAST(Zone as VARCHAR) as Zipcode, COUNT(Zone) as violation_count
                    FROM df
                    GROUP BY Zone
                    ORDER BY violation_count DESC
                """).df()
        print(df3.head())
        
        return df, df2, df3

    # Example 1: Group by 'Department' and count the number of violations per department
    load_and_process_data(con)


    # Close the DuckDB connection
    con.close()


if __name__ == "__main__":
    main()
