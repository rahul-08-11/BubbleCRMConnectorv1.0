import os
import requests

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


def add_form_vehicle_into_crm(access_token, bubble_vehicle_payload, main_image_url):
    url = "https://www.zohoapis.ca/crm/v2/Vehicles"
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json",
    }

    data = {"data": [bubble_vehicle_payload]}

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 201 or response.status_code == 200:
        vehicle_id = response.json()["data"][0]["details"]["id"]

        print("Vehicle added successfully.")
        print(f"vehicle id is {vehicle_id}")

        # if
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
    url = f"https://www.zohoapis.ca/crm/v2/Vehicles"
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json",
    }

    response = requests.patch(url, headers=headers, json=data)
    return response.json()
