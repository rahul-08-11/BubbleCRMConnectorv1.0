import logging
from recommendation import get_leads_for_vehicle
from src.apis import *
from models import *
from utils import *
import azure.functions as func
import json
import asyncio


global token_instance
token_instance = TokenManager()

async def process_leads(access_token,vehicle_id,vehicle_row,VIN,Buyer_name=None,sold_database=None,avg_purchase_price_df=None):
    try:
        ## update URL field with bubble app vehicle listing link 
        url = f"https://tradegeek.io/vehicle_details/{vehicle_id}"

        vehicle_url_data = {
        "data": [
            {
                "id": vehicle_id,
                "URL":url
            }
        ]
    }
        
        VehicleApi.update_vehicle(access_token,vehicle_url_data)
        ## get the recommendation or leads ,provide the source of vehicle,sold database ,Vehicle details and average purchase price to update lead score
        recommendations_df = get_leads_for_vehicle(vehicle_row,sold_database,"Website Input",avg_purchase_price_df  )
        logging.info(f"length of recommendation received for RUNLIST vehicle  :  {len(recommendations_df)}")
        if Buyer_name is not None:
            logging.info("Getting new set of Potential Buyers")
            recommendations_df=recommendations_df[~recommendations_df['Buyer'].isin(Buyer_name)]
            logging.info(f"length of new recommendation received :  {len(recommendations_df)}")
        vehicle_name = vehicle_row['Make'] + " " + vehicle_row['Model'] + " " + vehicle_row['Trim'] + " " + str(vehicle_row['Year']) + "-" + str(vehicle_row['Mileage']) + "Km - " + VIN
        recommendations_df = recommendations_df[:15]
        batchresponse = LeadApi.add_leads(recommendations_df,vehicle_id,access_token,vehicle_name) ## add lead into zoho crm
        logging.info(f"Lead Batch Response : {batchresponse}")
    except Exception as e:
        logging.error(f"Error Occured While Adding Leads {e}")





async def process_vehicle_and_lead(req : func.HttpRequest,sold_df=None,average_price_df=None) -> func.HttpResponse:
        """ Processes vehicle and lead request"""
        try:
            # Get the access token
            access_token=token_instance.get_access_token()

            # Get the form data
            body = req.form
            logging.info(f"received form data: {body}")

            # Create Vehicle instance from form data
            vehicle = Vehicle(
                Carfax_URL=body.get('Carfax_URL', ''),
                Vehicle_Image_Url=body.get('Vehicle_Image_Url', ''),
                Mileage=body.get('Mileage',''),
                Number_of_Cylinders=body.get('Number_of_Cylinders',''),
                Price=body.get('Price',''),
                Number_of_Passengers=body.get('Number_of_Passengers',''),
                Number_of_Doors=body.get('Number_of_Doors',''),
                Name=body.get('Name',''),
                Make=body.get('Make',''),
                Model=body.get('Model',''),
                Year=body.get('Year',''),
                VIN=body.get('VIN',''),
                Notes=body.get('Notes',''),
                Body_Type=body.get('Body_Type',''),
                Pickup_Location=body.get('Pickup_Location',''),
                DisplacementL=body.get('DisplacementL',''),
                Drivetrain=body.get('Drivetrain',''),
                Transmission=body.get('Transmission',''),
                Tires=body.get('Tires',''),
                Tire_Condition=body.get('Tire_Condition',''),
                Trim=body.get('Trim',''),
                Options=body.get('Options',''),  # If Options is a list
                Declarations=body.get('Declarations',''),
                Source=body.get('Source',''),
                Seller_ID=body.get('Seller_ID',''),
                Seller_Name=body.get('SellerName',''),
                VehicleDescription=body.get('VehicleDescription',''),
                VehicleCaptureType=body.get('VehicleCaptureType',''),
                VehicleConditionScore=body.get('VehicleConditionScore',''),
                Auction_URL=body.get('Auction_URL',''),
                Auction_Date=format_datetime(body.get('Auction_Date','')),
            )
            # Convert Vehicle instance to a dictionary
            bubble_vehicle = dict(vehicle)
            # Process the vehicle data further if needed
            Carfax_url = process_carfax_url(vehicle.Carfax_URL)
            main_image_url = process_main_img(bubble_vehicle.get('Vehicle_Image_Url', ''))


            bubble_vehicle.update({
            "Carfax_URL": Carfax_url,
            "Image_Link": main_image_url,
            "Status": "Available",
            "Exterior_colour": bubble_vehicle.get('Exterior_Color', ''),
        })
            vehicle_response = VehicleApi.add_form_vehicle_into_crm(access_token,bubble_vehicle,main_image_url)
            try:
                vehicle_id =  vehicle_response['data'][0]['details']['id']
                logging.info(f"Vehicle ID is obtained {vehicle_id}")

            except Exception as e:
                logging.info(f"Error adding submitted vehicle in zoho {e}")
                return func.HttpResponse(json.dumps(vehicle_response), status_code=500)
            
            vehicle_row = {
                "Make": vehicle.Make,
                "Model": vehicle.Model,
                "Year": vehicle.Year,
                "Trim": vehicle.Trim,
                "Mileage": vehicle.Mileage,
            }
            asyncio.create_task(process_leads(access_token,vehicle_id, vehicle_row, vehicle.VIN, None, sold_database=sold_df, avg_purchase_price_df=average_price_df))


            return func.HttpResponse(json.dumps({"details":vehicle_row,"Vehicle ID":vehicle_id,"code":"SUCCESS"}), status_code=200) 
        
        except Exception as e:
              
            return func.HttpResponse(json.dumps({"error":str(e),"code":"Failure"}), status_code=500)
        

async def reactivate_vehicle(req : func.HttpRequest,sold_df=None,average_price_df=None) -> func.HttpResponse:
    """ Reactivate vehicle """
    try:
        # Get access token
        access_token=token_instance.get_access_token()

        # Get form data
        body = req.form
        logging.info(f"received form data: {body}")

        # Create Vehicle instance from form data
        ActiveVehicle = ActivationVehicle(
            Vehicle_ID=body.get('Vehicle_ID'),
            Vehicle_VIN=body.get('Vehicle_VIN'),
            Make=body.get('Make'),
            Model=body.get('Model'),
            Year=body.get('Year'),
            Trim=body.get('Trim'),
            Mileage=body.get('Mileage'),
            Price=body.get('Price')
        )

        reactivate_data = {
        "data": [
            {
                "id": ActiveVehicle.Vehicle_ID,
                'reactivate':True,
                "price":ActiveVehicle.Price,
                "Status":"Available"
            }
        ]
    }

        # Reactivate vehicle in CRM
        response = VehicleApi.update_vehicle(access_token,reactivate_data)
        logging.info(f"Vehicle Reactivation response {response}")

        # Create vehicle required details placeholder
        vehicle_row = {
                "Make": ActiveVehicle.Make,
                "Model": ActiveVehicle.Model,
                "Year": ActiveVehicle.Year,
                "Mileage": ActiveVehicle.Mileage,
                "Trim": ActiveVehicle.Trim,
        }

        # Get existing leads
        existing_leads = []
        try:
            lead_data = LeadApi.get_specific_lead(access_token,ActiveVehicle.Vehicle_ID,'Vehicle_id')
            for data in lead_data:
                existing_leads.append(data['buyer_id']['name'])
        except Exception as e:
            logging.warning(f"Error fetching existings leads from zoho {e}")

        # Add new set of buyers
        asyncio.create_task(process_leads(access_token,ActiveVehicle.Vehicle_ID, vehicle_row,ActiveVehicle.Vehicle_VIN, existing_leads, sold_database=sold_df, avg_purchase_price_df=average_price_df))
        return func.HttpResponse(json.dumps({"Vehicle ID":ActiveVehicle.Vehicle_ID,"code":"SUCCESS"}), status_code=200) 

    except Exception as e:
        logging.error(f" Error processing reactivation request: {str(e)}")
        return func.HttpResponse(json.dumps({"error":str(e),"code":"Failure"}), status_code=500)

async def update_vehicle(req : func.HttpRequest):
    """ Update a vehicle """
    try:
        # Get the access token
        access_token=token_instance.get_access_token()

        # Get the form data
        body = req.get_json()
        logging.info(f"received form data: {body}")


        # Call the Update Api
        response = VehicleApi.update_vehicle(access_token,body)
        logging.info(f"Vehicle update response {response}")

        # Return the response
        return func.HttpResponse(json.dumps(response), status_code=200)
    
    except Exception as e:
        logging.error(f"error while updating the vehicle {str(e)}")

        return func.HttpResponse(json.dumps({"error":str(e)}), status_code=500)


async def delete_vehicle(req : func.HttpRequest):
    """ Delete Vehicle from Zoho"""
        # Get the access token

    try:
        access_token=token_instance.get_access_token()

        # Get the form data
        body = req.get_json()
        logging.info(f"received form data: {body}")


        # Call the Update Api
        response = VehicleApi.delete_vehicle(access_token,body)
        logging.info(f"Vehicle update response {response}")

        # Return the response
        return func.HttpResponse(json.dumps(response), status_code=200)
    
    except Exception as e:
        logging.error(f"error while updating the vehicle {str(e)}")

        return func.HttpResponse(json.dumps({"error":str(e)}), status_code=500)
