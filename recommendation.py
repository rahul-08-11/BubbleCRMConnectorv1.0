import pandas as pd
import re
from fuzzywuzzy import fuzz
import numpy as np
import logging

class BuyerRecommendation:

    def __init__(self):
        """ Buyer Recommendation Class """
        logging.info("--Getting Buyer Recommendation---")

    ## matrix Trim Score
    def trim_m(self, df :pd.DataFrame ,vehicle_trim : str  )  -> pd.DataFrame:
        df['Trim Score'] = df['Trim'].apply(lambda x: fuzz.ratio(x, vehicle_trim)/10 if isinstance(x, str) else 0)
        return df
#    

    ## matrix year Score
    def year_m(self, df : pd.DataFrame, vehicle_year : str)->pd.DataFrame:
        # Convert vehicle_year to numeric, if it's not already
        try:
            vehicle_year = int(vehicle_year)  # Assuming vehicle_year should be an integer
        except ValueError:
            raise ValueError("vehicle_year must be a valid integer.")

        # Ensure 'Year' column is numeric and handle non-numeric values
        df['Year'] = pd.to_numeric(df['Year'], errors='coerce')

        # Optionally drop rows with NaN values in 'Year' column
        df = df.dropna(subset=['Year'])
        # Calculate the absolute difference between each year and the input year
        df['year_diff'] = abs(df['Year'] - vehicle_year)

        # Find the maximum difference to normalize scores
        max_year_diff = df['year_diff'].max()

        # Define the scoring logic based on the absolute year difference
        def calculate_year_score(year_diff, max_diff):
            if max_diff == 0:
                return 10
            score = 10 - (year_diff / max_diff) * 10
            # Ensure the score is not negative
            return max(score, 0)

        # Apply the scoring function to calculate YearScore
        df['Year Score'] = df['year_diff'].apply(calculate_year_score, max_diff=max_year_diff)

        return df

    ## matrix mileage Score
    def mileage_m(self, df : pd.DataFrame, vehicle_mileage : str)->pd.DataFrame:
        # calculate mileage difference to get deviation
        df['Mileage_diff'] = abs(df['Mileage'] - vehicle_mileage)
        # calculate max mileage
        max_mileage_diff = df['Mileage_diff'].max()

        def calculate_mileage_score(mileage_diff, max_diff):
            if max_diff == 0:
                return 10
            score = 10 - (mileage_diff / max_diff) * 10
            return max(score, 0)

        df['Mileage Score'] = df['Mileage_diff'].apply(calculate_mileage_score, max_diff=max_mileage_diff)
        return df

    
    ## matrix apprasail 
    def appraisal_m(self, df : pd.DataFrame)->pd.DataFrame:
        def cal_appraisal_score(purchase_price, app_90, app_95, app_100):
            if purchase_price > app_90:
                return 10
            elif purchase_price > app_95:
                return 8
            elif purchase_price > app_100:
                return 6
            else:
                return 0

        df['Appraisal Score'] = df[['Purchase Price', '90', '95', '100']].apply(
            lambda row: cal_appraisal_score(row['Purchase Price'], row['90'], row['95'], row['100']), axis=1
        )

        df['Count'] = 1

        return df
    
    # Categorizer
    def categorize_intensity(self, percentage: float) -> str:
        if percentage < 12.5:
            return 'Cold'
        elif percentage < 25:
            return 'Moderate'
        elif percentage < 37.5:
            return 'Warm'
        else:
            return 'Hot'
        


    def recommend_buyers(self,vehicle_row, df_sold,vehicle_source,avg_price_df):
        ## extract the required attributes
        try:
            df_sold['Make'] = df_sold['Make'].str.lower().str.strip()
            df_sold['Model'] = df_sold['Model'].str.lower().str.strip()
            df_sold['Trim'] = df_sold['Trim'].str.lower().str.strip()
            df_sold['Mileage'] = pd.to_numeric(df_sold['Mileage'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce').fillna(0)
            df_sold['Year'] = pd.to_numeric(df_sold['Year'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce').fillna(0)
                
            vehicle_make = vehicle_row['Make'].lower().strip()
            vehicle_model = vehicle_row['Model'].lower().strip()
            vehicle_trim = vehicle_row['Trim'].lower().strip()
            vehicle_mileage = int(re.sub(r'[^\d]', '', str(vehicle_row['Mileage'])).strip())
            vehicle_year = int(re.sub(r'[^\d]', '', str(vehicle_row['Year'])).strip())

        except Exception as e:

            logging.error(e)
            
        try:
            filtered_df = df_sold[
                (df_sold['Make'] == vehicle_make) &
                (df_sold['Model'] == vehicle_model) 
            ]
            filtered_df = self.trim_m(filtered_df, vehicle_trim)
            filtered_df = self.year_m(filtered_df, vehicle_year)
            filtered_df = self.mileage_m(filtered_df, vehicle_mileage)
            filtered_df = self.appraisal_m(filtered_df)

            buyers = filtered_df.groupby('Buyer').agg({
                "Trim Score":"mean",
                "Year Score":"mean",
                'Mileage Score':'mean',
                "Appraisal Score":"mean",
                "Count":"sum",
                "Platform":"first"
            }).reset_index()

            ## scale count score
            buyers['Count Score'] = buyers['Count'].apply(lambda x : (x/buyers['Count'].max()) * 10)
            buyers['BScore'] = buyers['Trim Score'] + buyers['Year Score'] + buyers['Mileage Score'] + buyers['Appraisal Score'] + buyers['Count Score']
            buyers.sort_values(by='BScore',inplace=True, ascending=False)
            buyers['Score'] = buyers['BScore'].apply(lambda x : self.categorize_intensity(x))
            final_leads=self.update_lead_score(buyers,vehicle_source,avg_price_df)

            logging.info(final_leads)
       
            return final_leads
        except Exception as e:

            logging.error(e)
            return []
           



    ## standardize company name by removing potentially conflicting characters
    def standardize_cname(self, name):
        # Capitalize the first letter of each word
        name = name.title()
        # Remove punctuation except hyphens
        name = re.sub(r'[^\w\s-]', '', name)
        # Remove extra spaces
        name = name.strip()
        return name


    def update_lead_score(self,leads,vehicle_source,avg_price_df):
        """plateform : which sold data we are using for updating the score"""
        """source of vehicle such as eblock,traderev etc"""
        """A warm lead can turn into hot if the recommended buyer has an average purchase price of 105% or more. 
        A cold/warm lead can turn into a hot if it’s a TradeRev only buyer and the car is coming from eblock."""

        leads['Buyer'] = leads['Buyer'].apply(self.standardize_cname)
        leads = leads.merge(avg_price_df[['Buyer', 'Average Purchase Price']], on='Buyer', how='left').fillna({'Average Purchase Price': 0})
    # Define conditions
        conditions = [
            (leads['Average Purchase Price'] >= 105) & (leads['Score'] == 'Cold'),
            (leads['Average Purchase Price'] >= 105) & (leads['Score'] == 'Warm'),
            (leads['Average Purchase Price'] >= 105) & (leads['Platform'] == 'Only Traderev') & (vehicle_source in ['Run List', 'If Bid']),
            (leads['Average Purchase Price'] >= 105) & (leads['Platform'] == 'Only Eblock') & (vehicle_source== 'TR Upcoming')
        ]

        # Step 3: Define corresponding results
        choices = [
            'Warm',  # If avg_price >= 105 and initial score is 'Cold'
            'Hot',   # If avg_price >= 105 and initial score is 'Warm'
            'Hot',   # Traderev platform with specific vehicle sources and avg_price >= 105
            'Hot'    # Eblock platform with specific vehicle sources and avg_price >= 105
        ]

        #  Apply the conditions
        leads['Score'] = np.select(conditions, choices, default=leads['Score'])
        logging.info(f"length of leads Updated {len(leads)}")

        return leads 
