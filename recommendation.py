import pandas as pd
import re
import hashlib
import string


def generate_unique_id(text,other=''):
        # Remove brackets and punctuation
    text = text.translate(str.maketrans('', '', string.punctuation)) ## remove punctuations
    other = other.translate(str.maketrans('', '', string.punctuation))
    # Combine company name and address
    unique_string = f"{text}{other}"
    unique_string.replace(",",'').replace(" ",'').replace("'",'').replace('"','').replace(".",'')
    
    # Create an MD5 hash object
    hash_object = hashlib.md5(unique_string.encode())
    
    # Generate the hexadecimal representation of the hash
    unique_id = hash_object.hexdigest()
    
    return unique_id


def get_leads_for_vehicle(vehicle_row, df_sold,vehicle_source,avg_price_df):
    ## extract the required attributes
    df_sold['Make'] = df_sold['Make'].str.lower().str.strip()
    df_sold['Model'] = df_sold['Model'].str.lower().str.strip()
    df_sold['Trim'] = df_sold['Trim'].str.lower().str.strip()
    
    year = int(vehicle_row['Year'].strip())
    make = vehicle_row['Make'].lower().strip()
    model = vehicle_row['Model'].lower().strip()
    trim = vehicle_row['Trim'].lower().strip()
    mileage = int(str(vehicle_row['Mileage']).strip())
    print(year,make,model,trim,mileage)
    """
    Get recommendations for hot, warm, and cold leads for a given vehicle.
    """
    hot_lead = None
    warm_lead = None
    cold_lead = None
    try:
        hot_lead, hot_score = get_buyer_recommendations_very_hot(df_sold, make, model, year, trim, mileage, trim_similarity=80)
        hot_lead['Score'] = hot_score
    except Exception as e:
        print(f"No hot leads: {e}")
    try:
        warm_lead, warm_score = get_buyer_recommendations_hot(df_sold, make, model, year, trim, mileage, trim_similarity=80)
        warm_lead['Score'] = warm_score
        # warm_lead = warm_lead[:10]
    except Exception as e:
        print(f"No warm leads: {e}")
    try:
        cold_lead, cold_score = get_buyer_recommendations_warm(df_sold, make, model, year, trim, mileage, trim_similarity=80)
        cold_lead['Score'] = cold_score
        # cold_lead = cold_lead[:5]
    except Exception as e:
        print(f"No cold leads: {e}")

    try:
        low_lead, low_score = get_buyer_recommendations_cold(df_sold, make, model, year, trim, mileage, trim_similarity=80)
        low_lead['Score'] = low_score

    except Exception as e:
        print(f"No low leads")

    try:
        lowest_lead,lowest_score = get_buyer_recommendations_low(df_sold, make, model, year, trim, mileage, trim_similarity=80)
        lowest_lead['Score']=lowest_score
    except Exception as e:
        print(f"No lowest leads")

    all_leads = pd.concat([hot_lead, warm_lead, cold_lead,low_lead], ignore_index=True)
    print("all leads are ",all_leads)
    # updated_leads.drop_duplicates(inplace=True)
    updated_leads=update_lead_score(all_leads,vehicle_source,avg_price_df) ## update score based on avg price and source of the vehicle and buyers
    final_leads= sort_leads(updated_leads)  ## arange the leads based on score to get hot - > warm - > cold
    print(f"-----------------------------------final leads : {len(final_leads) }---------------------")
    return final_leads
        
def sort_leads(result):
    ## it will prioratize lead if the lead are same but has different score than prioratize hot--->warm-->cold
    ## create a map
    category_values = {'Very Hot':5,'Hot': 4, 'Warm': 2, 'Cold': 1,'Low':0}
    result['ScoreNumeric'] = result['Score'].map(category_values)
    result['Purhcase Score'] = (result['ScoreNumeric'] * 0.7) * (result['Purchase Count'] * 0.3)
    result.sort_values(by='Purhcase Score', ascending=False, inplace=True)
    
    # Drop duplicates, keeping the highest 'Purchase Score' within each group
    result = result.drop_duplicates(subset=['Buyer'], keep='first')
    return result



## standardize company name by removing potentially conflicting characters
def clean_company_name(name):
    # Capitalize the first letter of each word
    name = name.title()
    # Remove punctuation except hyphens
    name = re.sub(r'[^\w\s-]', '', name)
    # Remove extra spaces
    name = name.strip()
    return name


def update_lead_score(leads,vehicle_source,avg_price_df):
    """plateform : which sold data we are using for updating the score"""
    """source of vehicle such as eblock,traderev etc"""
    """A warm lead can turn into hot if the recommended buyer has an average purchase price of 105% or more. 
    A cold/warm lead can turn into a hot if it’s a TradeRev only buyer and the car is coming from eblock."""
    print(f"--------------Number of leads for vehicle : {len(leads)} and vehicle source : {vehicle_source}----------------------------------------")
    for index, row in leads.iterrows():
        buyer = clean_company_name(row['Buyer'])
        initial_score = row['Score']
        try:
            avg_price = avg_price_df[avg_price_df['Buyer'] == buyer]['Average Purchase Price'].values[0]
        except Exception as e:
            avg_price = 0
    
        print(f"coming from update lead score",avg_price)
        print(f"Initial lead status for {buyer} ==> Score: {row['Score']} ==> VehicleSource: {vehicle_source}")
        platform = row['Platform']
        if avg_price >=105:
            if initial_score == 'Cold':
                leads.loc[index, 'Score'] = 'Warm'
            elif initial_score == 'Warm':
                leads.loc[index, 'Score'] = 'Hot'
                print(f"Lead updated for {buyer} ==> Score: Hot ==> VehicleSource: {vehicle_source} ==> Reason: Average Purchase Price equal or above 105% and already had Warm Score")
            elif platform == 'Only Traderev' and vehicle_source in ['Run List', 'If Bid']:
                leads.loc[index, 'Score'] = 'Hot'
                print(f"Lead updated for {buyer} ==> Score: Hot ==> VehicleSource: {vehicle_source} ==> Reason: Is only Traderev and Vehicle coming from eblock as well as High Spender above 105%")
            elif platform == 'Only Eblock' and vehicle_source == 'TR Upcoming':
                leads.loc[index, 'Score'] = 'Hot'
                print(f"Lead updated for {buyer} ==> Score: Hot ==> VehicleSource: {vehicle_source} ==> Reason: Is only Eblock and Vehicle coming from traderev as well as High Spender above 105%")
        else:
            print(f"Lead not updated for {buyer} ==> Score: {row['Score']} ==> Vehicle Source: {vehicle_source} ==> Reason: Not a spender above 105%")
    return leads


def filter_dataset(df: pd.DataFrame, make: str, model: str):
    # Filter the DataFrame for the specified make and model
    filtered_df = df[(df['Make'] == make) & (df['Model'] == model)]
    print(filtered_df)
    # Group by 'Buyer' and calculate the mean of other columns
    result = filtered_df.groupby(['Buyer', 'Make', 'Model']).agg({
        'Purchase Price': 'mean',  # Calculate the mean purchase price
        "90": 'mean',                 # Calculate the mean for column '90'
        "95": 'mean',                 # Calculate the mean for column '95'
        "100": 'mean',                # Calculate the mean for column '100'
        "105": 'mean',                # Calculate the mean for column '105'
        "110": 'mean'                 # Calculate the mean for column '110'
    }).reset_index()

    # Count the number of purchases per buyer
    purchase_count = filtered_df['Buyer'].value_counts().reset_index()
    purchase_count.columns = ['Buyer', 'Purchase Count']  # Rename columns for clarity

    # Merge the results
    result = pd.merge(result, purchase_count, on='Buyer', how='left')  # Merge on 'Buyer' column
    return result

def filter_data_low(data: pd.DataFrame, make: str, model: str, year: float, trim: str, mileage: float):
  """
  Filter data based on make, model, year, mileage, and trim.
  """
  print("\n\nfilter_data_low",type(mileage),mileage)
  try:
    lower_bound = float(mileage) * 0
    upper_bound = float(mileage) * 3
    
    try:
        filtered_data = data[(data['Make'] == make) &
                        (data['Model'] == model) &
                        (data["Mileage"].between(lower_bound, upper_bound)) &
                        (data['Year'].between(float(year)-3,float(year)+3))
                        ]
        lead_score="Low"
        print("low Filter:",filtered_data)
    except Exception as e:
        print(e)
        pass
    
    filtered_data['Date'] = pd.to_datetime(filtered_data['Date'], errors='coerce')
    result = filtered_data.groupby(['Buyer']).agg({
          "Date":"max",
        #   "Greater90": "sum",
           "Platform" : "first",
      }).reset_index()
    
    purchase_count = filtered_data['Buyer'].value_counts().reset_index()
    purchase_count.columns = ['Buyer', 'Purchase Count']
    result = pd.merge(result, purchase_count, on='Buyer', how='left')
   
    a=pd.api.types.is_datetime64_any_dtype(data['Date'])
    #print(a)
    result['Date'] = pd.to_datetime(result['Date'], errors='coerce')
    a=pd.api.types.is_datetime64_any_dtype(data['Date'])
 
    result.reset_index(drop=True, inplace=True)
    result.sort_values(by="Purchase Count", ascending=False, inplace=True)
    #print(result)
    print("Result of low lead filter:",result)
    return result,lead_score
  except Exception as e:
    print(e)
    return None



def filter_data_cold(data: pd.DataFrame, make: str, model: str, year: float, trim: str, mileage: float):
  """
  Filter data based on make, model, year, mileage, and trim.
  """
  print("\n\nfilter_data_cold",type(mileage),mileage)
  try:
    lower_bound = float(mileage) * 0
    upper_bound = float(mileage) * 2.5
    
    try:
        filtered_data = data[(data['Make'] == make) &
                        (data['Model'] == model) &
                        (data["Mileage"].between(lower_bound, upper_bound)) &
                        (data['Year'].between(float(year)-2,float(year)+2))
                        ]
        lead_score="Cold"
        print("Cold Filter:",filtered_data)
    except Exception as e:
        print(e)
        pass
    
    filtered_data['Date'] = pd.to_datetime(filtered_data['Date'], errors='coerce')
    result = filtered_data.groupby(['Buyer']).agg({
          "Date":"max",
        #   "Greater90": "sum",
           "Platform" : "first",
      }).reset_index()
    
    purchase_count = filtered_data['Buyer'].value_counts().reset_index()
    purchase_count.columns = ['Buyer', 'Purchase Count']
    result = pd.merge(result, purchase_count, on='Buyer', how='left')
   
    a=pd.api.types.is_datetime64_any_dtype(data['Date'])
    #print(a)
    result['Date'] = pd.to_datetime(result['Date'], errors='coerce')
    a=pd.api.types.is_datetime64_any_dtype(data['Date'])
 
    result.reset_index(drop=True, inplace=True)
    result.sort_values(by="Purchase Count", ascending=False, inplace=True)
    #print(result)
    print("Result of low lead filter:",result)
    return result,lead_score
  except Exception as e:
    print(e)
    return None



def filter_data_warm(data: pd.DataFrame, make: str, model: str, year: float, trim: str, mileage: float) -> pd.DataFrame:
  """
  Filter data based on make, model, year, mileage, and trim.
  """
  print("\n\nfilter_data_warm",type(mileage),mileage)
  try:
    lower_bound = float(mileage) * 0
    upper_bound = float(mileage) * 1.9
    
    try:
        filtered_data = data[(data['Make'] == make) &
                        (data['Model'] == model) &
                        (data["Mileage"].between(lower_bound, upper_bound)) &
                        (data['Year'].between(float(year)-2,float(year)+2))
                        ]
        lead_score="Warm"
        print("Warm Filter:",filtered_data)
    except Exception as e:
        print(e)
        pass
    
    filtered_data['Date'] = pd.to_datetime(filtered_data['Date'], errors='coerce')
    result = filtered_data.groupby(['Buyer']).agg({
          "Date":"max",
        #   "Greater100": "sum",
           "Platform" : "first",
          #"Greater105": "sum",
          #"Greater110": "sum",
      }).reset_index()
    
    purchase_count = filtered_data['Buyer'].value_counts().reset_index()
    purchase_count.columns = ['Buyer', 'Purchase Count']
    result = pd.merge(result, purchase_count, on='Buyer', how='left')
   
    a=pd.api.types.is_datetime64_any_dtype(data['Date'])
    #print(a)
    result['Date'] = pd.to_datetime(result['Date'], errors='coerce')
    a=pd.api.types.is_datetime64_any_dtype(data['Date'])
 
    result.reset_index(drop=True, inplace=True)
    result.sort_values(by="Purchase Count", ascending=False, inplace=True)
    #print(result)
    print("Result of cold lead filter:",result)
    return result,lead_score
  except Exception as e:
    print(e)
    return None

def filter_data_hot(data: pd.DataFrame, make: str, model: str, year: float, trim: str, mileage: float) -> pd.DataFrame:
  """
  Filter data based on make, model, year, mileage, and trim.
  """
  print("\n\nfilter_data_hot",type(mileage),mileage)
  try:
    lower_bound = float(mileage) * 0
    upper_bound = float(mileage) * 1.6
    
    try:
        filtered_data = data[(data['Make'] == make) &
                        (data['Model'] == model) &
                        (data['Trim'] == trim) &
                        (data["Mileage"].between(lower_bound, upper_bound)) &
                        (data['Year'].between(float(year)-1,float(year)+1))
                        ]
        lead_score="Hot"
        print("Hot Filter:",filtered_data)
    except Exception as e:
        print("Hot Filter Error:",e)
        pass
    filtered_data['Date'] = pd.to_datetime(filtered_data['Date'], errors='coerce')
    result = filtered_data.groupby(['Buyer']).agg({
          "Date":"max",
        #   "Greater100": "sum",
           "Platform" : "first",
          #"Greater105": "sum",
          #"Greater110": "sum",
      }).reset_index()
    
    purchase_count = filtered_data['Buyer'].value_counts().reset_index()
    purchase_count.columns = ['Buyer', 'Purchase Count']
    result = pd.merge(result, purchase_count, on='Buyer', how='left')
    # print("Result:",result)
    a=pd.api.types.is_datetime64_any_dtype(data['Date'])
    # print(a)
    result['Date'] = pd.to_datetime(result['Date'], errors='coerce')
    a=pd.api.types.is_datetime64_any_dtype(data['Date'])
    
    # print(a)
   # result['Recent'] = 100/((datetime.now().date() - result['Date'].dt.date).dt.days)
    #result['HVP'] = result['Greater100']+result['Greater105']+result['Greater110']
    result.reset_index(drop=True, inplace=True)
    result.sort_values(by="Purchase Count", ascending=False, inplace=True)
    print("result of cold lead filter ",result)
    return result,lead_score
  except Exception as e:
    print(e)
    return None


    
def filter_data_very_hot(data: pd.DataFrame, make: str, model: str, year: float, trim: str, mileage: float) -> pd.DataFrame:
  """
  Filter data based on make, model, year, mileage, and trim.
  """
  print("\n\nfilter_data_very_hot",type(mileage),mileage)
  try:
    lower_bound = float(mileage) * 0
    upper_bound = float(mileage) * 1.3
    
    try:
        filtered_data = data[(data['Make'] == make) &
                        (data['Model'] == model) & 
                        (data['Trim'] == trim) &
                        (data["Mileage"].between(lower_bound, upper_bound)) &
                        (data['Year'] == year)
                        ]
        print("Filtered Data First Test",filtered_data)
        lead_score="Very Hot"
    except Exception as e:
        print(e)
        pass
    
    filtered_data['Date'] = pd.to_datetime(filtered_data['Date'], errors='coerce')
    result = filtered_data.groupby(['Buyer']).agg({
          "Date":"max",
        #   "Greater100": "sum",
          "Platform" : "first",
          #"Greater105": "sum",
          #"Greater110": "sum",
      }).reset_index()
    
    purchase_count = filtered_data['Buyer'].value_counts().reset_index()
    purchase_count.columns = ['Buyer', 'Purchase Count']
    result = pd.merge(result, purchase_count, on='Buyer', how='left')
    print("Result of hot lead filter:",result)
    a=pd.api.types.is_datetime64_any_dtype(data['Date'])
    print(a)
    result['Date'] = pd.to_datetime(result['Date'], errors='coerce')
    a=pd.api.types.is_datetime64_any_dtype(data['Date'])
    
    print(a)

    result.reset_index(drop=True, inplace=True)
    result.sort_values(by="Purchase Count", ascending=False, inplace=True)
    # print(result)
    return result,lead_score
  except Exception as e:
    print(e)
    return None



def get_buyer_recommendations_warm(data: pd.DataFrame, make: str, model: str, year: float, trim: str, mileage: float, trim_similarity=80) -> pd.DataFrame:
  """
  Get buyer recommendations based on make, model, year, mileage, and trim.
  """
  print("get_buyer_recommendations_warm data: \n",data.columns, len(data))
  # Convert 'Date' column to datetime format
  data['Date'] = pd.to_datetime(data['Date'].astype(str), errors='coerce')

  # Format 'Date' column as '%m/%d/%y'
  data['Date'] = data['Date'].dt.strftime('%m/%d/%y')

  data['Mileage']=data['Mileage'].replace('[^\d.]','',regex=True)
  data['Mileage']=pd.to_numeric(data['Mileage'])
  data['Year']=pd.to_numeric(data['Year'])
  data["Greater100"] = data["Purchase Price"] > data["100"]
  data = data[data["Greater100"]]
  print("After Cleaning data:",len(data),data.columns)
  print("Here")
  recommendations_warm,score = filter_data_warm(data, make, model, year, trim, mileage)
  print("len of recommendations",len(recommendations_warm))

 # recommendations_hot.to_csv("hotrec.csv")
  return recommendations_warm,score


def get_buyer_recommendations_cold(data: pd.DataFrame, make: str, model: str, year: float, trim: str, mileage: float, trim_similarity=80):
  """
  Get buyer recommendations based on make, model, year, mileage.
  """
  print("get_buyer_recommendations_cold data: \n",data.columns, len(data))
  # Convert 'Date' column to datetime format
  data['Date'] = pd.to_datetime(data['Date'].astype(str), errors='coerce')

  # Format 'Date' column as '%m/%d/%y'
  data['Date'] = data['Date'].dt.strftime('%m/%d/%y')

  data['Mileage']=data['Mileage'].replace('[^\d.]','',regex=True)
  data['Mileage']=pd.to_numeric(data['Mileage'])
  data['Year']=pd.to_numeric(data['Year'])
  data["Greater100"] = data["Purchase Price"] > data["100"]
  data = data[data["Greater100"]]
  print("After Cleaning data:",len(data),data.columns)
  print("Here")
  recommendations_cold,score = filter_data_cold(data, make, model, year, trim, mileage)
  print("len of recommendations",len(recommendations_cold))

 # recommendations_hot.to_csv("hotrec.csv")
  return recommendations_cold,score


def get_buyer_recommendations_low(data: pd.DataFrame, make: str, model: str, year: float, trim: str, mileage: float, trim_similarity=80):
  """
  Get buyer recommendations based on make, model, year, mileage.
  """
  print("get_buyer_recommendations_low data: \n",data.columns, len(data))
  # Convert 'Date' column to datetime format
  data['Date'] = pd.to_datetime(data['Date'].astype(str), errors='coerce')

  # Format 'Date' column as '%m/%d/%y'
  data['Date'] = data['Date'].dt.strftime('%m/%d/%y')

  data['Mileage']=data['Mileage'].replace('[^\d.]','',regex=True)
  data['Mileage']=pd.to_numeric(data['Mileage'])
  data['Year']=pd.to_numeric(data['Year'])
  data["Greater90"] = data["Purchase Price"] > data["90"]
  data = data[data["Greater90"]]
  print("After Cleaning data:",len(data),data.columns)
  print("Here")
  recommendations_low,score = filter_data_low(data, make, model, year, trim, mileage)
  print("len of recommendations",len(recommendations_low))

 # recommendations_hot.to_csv("hotrec.csv")
  return recommendations_low,score


def get_buyer_recommendations_hot(data: pd.DataFrame, make: str, model: str, year: float, trim: str, mileage: float, trim_similarity=80) -> pd.DataFrame:
  """
  Get buyer recommendations based on make, model, year, mileage, and trim.
  """
  print("get_buyer_recommendations_warm data: \n",data.columns, len(data))
  # Convert 'Date' column to datetime format
  print("Cleaning Date")
  data['Date'] = pd.to_datetime(data['Date'].astype(str), errors='coerce')

  # Format 'Date' column as '%m/%d/%y'
  data['Date'] = data['Date'].dt.strftime('%m/%d/%y')
  print("Cleaning Mileage")
  data['Mileage']=data['Mileage'].replace('[^\d.]','',regex=True)
  data['Mileage']=pd.to_numeric(data['Mileage'])
  data['Year']=pd.to_numeric(data['Year'])
  data["Greater100"] = data["Purchase Price"] > data["100"]
  data = data[data["Greater100"]]
  print("After Cleaning data:",len(data),data.columns)
  print("Here")
  recommendations_hot ,score= filter_data_hot(data, make, model, year, trim, mileage)
  print("len of recommendations",len(recommendations_hot))

 # recommendations_hot.to_csv("hotrec.csv")
  return recommendations_hot,score



def get_buyer_recommendations_very_hot(data: pd.DataFrame, make: str, model: str, year: float, trim: str, mileage: float, trim_similarity=80) -> pd.DataFrame:
  """
  Get buyer recommendations based on make, model, year, mileage, and trim.
  """
  print("get_buyer_recommendations_hot data: \n",data.columns, len(data))
  # Convert 'Date' column to datetime format
  data['Date'] = pd.to_datetime(data['Date'].astype(str), errors='coerce')

  # Format 'Date' column as '%m/%d/%y'
  data['Date'] = data['Date'].dt.strftime('%m/%d/%y')

  data['Mileage']=data['Mileage'].replace('[^\d.]','',regex=True)
  data['Mileage']=pd.to_numeric(data['Mileage'])
  data['Year']=pd.to_numeric(data['Year'])
  data["Greater100"] = data["Purchase Price"] > data["100"]
  data =data[data["Greater100"]]
  print("After Cleaning data:",len(data),data.columns)
  print("Here")
  recommendations_very_hot ,score= filter_data_very_hot(data, make, model, year, trim, mileage)
  print("len of recommendations",len(recommendations_very_hot))

 # recommendations_hot.to_csv("hotrec.csv")
  return recommendations_very_hot,score



