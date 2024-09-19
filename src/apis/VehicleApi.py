import os
import requests
from utils.helpers import *
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
        print("Error:", response)


def search_duplicates(access_token, unique_identifier, field_name):
    """
    unique_identifier : Primary key to search
    field_name : Name of the field to be searched

    """
    # API endpoint
    url = "https://www.zohoapis.ca/crm/v2/Vehicles/search"

    params = {"criteria": f"{field_name}:equals:{unique_identifier}"}

    # Authorization Header
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
    }

    # Make GET request to fetch id
    response = requests.get(url, params=params, headers=headers)

    # Check if request was successful
    if response.status_code == 204: # content not found
        return "NOT FOUND"
    else:
        return "FOUND"

## attach main or thumbnail image onto the record
def attach_main_image_to_vehicle(access_token, vehicle_id, image_url):
    # Download the image content
    img_data = requests.get(image_url).content
    with open(f"{vehicle_id}.jpg", "wb") as handler:
        handler.write(img_data)

    # Attach the image to the vehicle record :  image endpoint
    ## doc https://www.bigin.com/developer/docs/apis/upload-org-img.html
    url = f"https://www.zohoapis.ca/crm/v2/Vehicles/{vehicle_id}/photo"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    files = {"file": ("image_name.jpg", open(f"{vehicle_id}.jpg", "rb"), "image/jpeg")}

    attach_response = requests.post(url, headers=headers, files=files)
    # Delete the file from the system as we don't really require it on local system and it is available on internet
    os.remove(f"{vehicle_id}.jpg")

    return attach_response.json()


def add_form_vehicle_into_crm(access_token, bubble_vehicle, main_image_url):
    url = "https://www.zohoapis.ca/crm/v2/Vehicles"
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json",
    }
    try:
    ## check for duplicate records
        if search_duplicates(access_token,bubble_vehicle.get("VIN",''),"VIN") == "FOUND":
            return {"message": "Vehicle already exists."}
    except Exception as e:
        print(f"error while checking for duplicates {str(e)}")
    ## add seller id based on name if seller id is not provided
    try:
        if bubble_vehicle.get("Seller_Name",'') != '':
            seller_id = get_account_id(access_token, bubble_vehicle.get("Seller_Name", ''), 'Account_Name')
            bubble_vehicle['Seller_ID'] = seller_id
    except Exception as e:
        print(f"error while fetching seller id {str(e)}")

    data = {"data": [bubble_vehicle]}

    response = requests.post(url, headers=headers, json=data)
    print(response.json())
    if response.status_code == 201 or response.status_code == 200:
        vehicle_id = response.json()["data"][0]["details"]["id"]
        print(f"vehicle id is {vehicle_id}")
        try:
            attach_main_image_to_vehicle(
                access_token, vehicle_id, main_image_url
            )  ## attach vehicle main image
        except Exception as e:
            pass

        return response.json()

    else:
        return response.json()


def update_vehicle(access_token, data):
    try:
        ## if vehicle record ID is not passed, fetch it using Vin Match
        if "id" not in data.keys():
            vehicle_id = get_vehicle_id(access_token, data['data'][0]['Vin'], "VIN")
            data['data'][0]['id'] = vehicle_id
            print(vehicle_id)
    except Exception as e:
        print(e)
    url = f"https://www.zohoapis.ca/crm/v2/Vehicles"
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json",
    }

    response = requests.patch(url, headers=headers, json=data)
    return response.json()


def get_vehicle_id(access_token, unique_identifier, field_name):
    """
    Fetch the Vehicle ID based on a unique identifier (like VIN).
    
    """
    # API endpoint
    url = "https://www.zohoapis.ca/crm/v2/Vehicles/search"

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
        if data.get("data"):
            vehicle_id = data["data"][0]["id"]
            return vehicle_id
        else:
            return None
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None