import requests

def get_account_id(access_token, unique_identifier, field_name):
    """
    unique_identifier : Primary key to search
    field_name : Name of the field to be searched

    """
    # API endpoint
    url = "https://www.zohoapis.ca/crm/v2/Accounts/search"

    params = {"criteria": f"{field_name}:equals:{unique_identifier}"}

    # Authorization Header
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
    }

    # Make GET request to fetch id
    response = requests.get(url, params=params, headers=headers)

    # Check if request was successful
    if response.status_code == 200 or response.status_code == 201:
        data = response.json()
        if data["data"]:
            company_id = data["data"][0]["id"]
            return company_id
        else:
            return None
    else:
        print(response)


## batch request
def add_leads(recommendation_df, vehicle_id, token, vehicle_name):
    url = "https://www.zohoapis.ca/crm/v2/Leads"

    headers = {
        "Authorization": f"Zoho-oauthtoken {token}",
        "Content-Type": "application/json",
    }

    data = []
    success_leads = 0

    for index, row in recommendation_df.iterrows():
        try:
            buyer_name = row["Buyer"]
            buyer_id = get_account_id(token, buyer_name, "Account_Name")

            if buyer_id:
                lead_data = {
                    "Last_Name": vehicle_name,
                    "Lead_Score": row["Score"],
                    "Vehicle_State": "Available",
                    "Buyer_Text": buyer_name,
                    "Vehicle_id": vehicle_id,
                    "Progress_Status": "To Be Contacted",
                    "buyer_id": buyer_id,
                }
                data.append(lead_data)
                success_leads += 1
        except Exception as e:
            print(e)

    payload = {"data": data}

    response = requests.post(url, headers=headers, json=payload)
    data = response.json()
    try:
        if response.status_code in [200, 201]:
            print(f"Successfully added {success_leads} leads for {vehicle_name}")
            for i, lead in enumerate(data["data"]):
                lead_id = lead["details"]["id"]
                recommendation_df.loc[i, "Lead_ID"] = lead_id

        else:
            print(f"Failed to add leads for {response.json()}")

    except Exception as e:
        print(f"Error sending request: {e}")

    return data


def get_specific_lead(access_token, unique_identifier, field_name):
    """
    unique_identifier : Primary key to search
    field_name : Name of the field to be searched

    """
    # API endpoint
    url = "https://www.zohoapis.ca/crm/v2/Leads/search"

    params = {"criteria": f"{field_name}:equals:{unique_identifier}"}

    # Authorization Header
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
    }

    # Make GET request to fetch id
    response = requests.get(url, params=params, headers=headers)

    # Check if request was successful
    if response.status_code == 200 or response.status_code == 201:
        data = response.json()
        if data["data"]:
            # print(data['data'])
            return data["data"]

        else:
            return None
    else:
        print("Error:", response.text)
