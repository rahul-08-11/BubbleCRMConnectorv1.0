import azure.functions as func
import logging
import pandas as pd
from src.funcmain import *

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# currently reading saved csv files
global sold_df, average_price_df
sold_df = pd.read_csv("sold1.0.csv", low_memory=False)
average_price_df = pd.read_csv("average_purhcase.csv")


@app.route(route="ping", methods=['GET'])
async def ping(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Ping request received.')
    return func.HttpResponse("Service is up", status_code=200)

@app.route(route="register-vehicle-lead", methods=["POST"])
async def register_vehicle_and_lead(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(f"Request received from {req.url}")

    try:
        response = await process_vehicle_and_lead(
            req=req, sold_df=sold_df, average_price_df=average_price_df
        )

        return response

    except Exception as e:
        logging.error(f"Error processing request: {str(e)}")
        return func.HttpResponse("Internal server error", status_code=500)


@app.route(route="vehicle-update", methods=["POST"])
async def vehicle_update_record(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(f"Request received from {req.url}")

    try:
        response = await update_vehicle(req=req)

        return response

    except Exception as e:
        logging.error(f"Error processing request: {str(e)}")

        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500)


@app.route(route="vehicle-activation", methods=["POST"])
async def vehicle_activation(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(f"Request received from {req.url}")

    try:
        response = await reactivate_vehicle(
            req=req, sold_df=sold_df, average_price_df=average_price_df
        )

        return response

    except Exception as e:
        logging.error(f"Error processing request: {str(e)}")
        return func.HttpResponse("Internal server error", status_code=500)
