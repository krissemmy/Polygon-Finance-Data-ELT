from typing import Any, Dict, Optional, Sequence, Union
from datetime import datetime, timedelta
import json
import pandas as pd
import requests
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PolygonToPGOperator(BaseOperator):
    """
    Custom Airflow Operator to extract financial data from the Polygon finance API
    and load it into a PostgreSQL table.

    :param key: The API key for authenticating with the Polygon finance API.
    :param type_of_data: The type of financial data to export (e.g., "stock", "forex", "crypto").
    :param yesterday: The date for which the financial data should be exported.
    :param table: The name of the PostgreSQL table to load the data into.
    :param time: The timestamp for the data extraction.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param **kwargs: Additional keyword arguments to be passed to the BaseOperator.

    """


    def  __init__(
		self,
        *,
		key: str,
        type_of_data: str,
        yesterday: datetime,
        table: str,
        time: datetime,
        **kwagrs,
	) -> None:
        super().__init__(**kwagrs)
        self.key=key
        self.table=table
        self.type_of_data = type_of_data
        self.yesterday = yesterday
        self.time = time
        self.endpoint = self._set_endpoint(self.type_of_data, self.yesterday)


    def execute(self, context: Dict[str, Any]) -> None:
        """
        Execute the operator. Extracts financial data from the Polygon finance API,
        transforms it, and loads it into a PostgreSQL table.

        :param context: Airflow context dictionary.

        """
        self.log.info(f"Executing export of file from {self.endpoint}")
        self.log.info(self.yesterday)

        # Set up API request parameters and headers
        params = { "adjusted" : "false" }
        headers = { "Authorization" : f"Bearer {self.key}" }

        # Make API request
        response = requests.get(self.endpoint, headers=headers, params=params)

        # Check for errors in the API response
        if response.status_code not in range(200,299):
            self.log.error("Error from request response")
        else:
            self.log.info("Request was successful with %s", response.status_code)


            req = response.json()

            df = pd.DataFrame(req['results'])
            df['date'] = self.yesterday
            df['request_id'] = req['request_id']
            df['adjusted'] = req['adjusted']
            df = df.rename(columns={'T': 'symbol', 'c': 'close_price', 'v': 'trading_volume', 't': 'timestamp_unix',
                         'vw': 'volume_weighted', 'o': 'open_price', 'n': 'number_of_transaction', 'h': 'highest_price',
                         'l': 'lowest_price'
                        })
            self.log.info(f"Data contains {df.size} columns")

            # Set up PostgreSQL connection
            postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
            post_con = postgres_hook.get_uri()
            print(post_con)

            # Connect to the PostgreSQL database and load data into the specified table
            engine = create_engine(post_con)
            engine.connect()
            df.to_sql(name=self.table, con=engine, index=False, if_exists="replace")
            self.log.info("Table successfully loaded")


    @staticmethod
    def _set_endpoint(type_of_data: str, yesterday: datetime) -> str:
        """
        Set the API endpoint based on the type of financial data.

        :param type_of_data: The type of financial data.
        :param yesterday: The date for which the data is requested.

        :return: The constructed API endpoint.

        """
 
        if type_of_data == "stock":
            endpoint = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{yesterday}"
        elif type_of_data == "forex":
            endpoint = f"https://api.polygon.io/v2/aggs/grouped/locale/global/market/fx/{yesterday}"
        elif type_of_data == "crypto":
            endpoint = f"https://api.polygon.io/v2/aggs/grouped/locale/global/market/crypto/{yesterday}" 
        return endpoint
