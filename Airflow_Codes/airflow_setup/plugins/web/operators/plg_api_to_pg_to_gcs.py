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
    Extract files from Polygon finance API

    :param destination_bucket: The bucket to upload to.
    :param destination_path: The destination name of the object in the
        destination Google Cloud Storage bucket. If destination_path is not
        provided, file/files will be placed in the main bucket path. If a
        wildcard is supplied in the destination_path argument, this is the
        prefix that will be prepended to the final destination objects' paths.
    :param type_of_data: The type of financial data to export (e.g., "stock",
        "forex", "crypto").
    :param yesterday: The date for which the financial data should be exported.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param gzip: Allows for the file to be compressed and uploaded as gzip.
    :param mime_type: The mime-type string.
    :param delegate_to: The account to impersonate using domain-wide delegation
        of authority, if any. For this to work, the service account making the
        request must have domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """


    def  __init__(
		self,
        *,
		key: str,
        type_of_data: str,
        yesterday: datetime,
        table: str,
        time: datetime,
		gcp_conn_id: str = "google_cloud_default",
        **kwagrs,
	) -> None:
        super().__init__(**kwagrs)
        self.key=key
        self.table=table
        self.type_of_data = type_of_data
        self.gcp_conn_id = gcp_conn_id
        self.yesterday = yesterday
        self.time = time
        self.endpoint = self._set_endpoint(self.type_of_data, self.yesterday)


    def execute(self, context: Dict[str, Any]) -> None:
        """Helper function to copy single files from spotify to GCS """
        self.log.info(f"Executing export of file from {self.endpoint}")
        self.log.info(self.yesterday)


        params = { "adjusted" : "false" }
        headers = { "Authorization" : f"Bearer {self.key}" }

        response = requests.get(self.endpoint, headers=headers, params=params)

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

            postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
            post_con = postgres_hook.get_uri()
            print(post_con)

            engine = create_engine(post_con)
            engine.connect()
            df.to_sql(name=self.table, con=engine, index=False, if_exists="replace")
            self.log.info("Table successfully loaded")


    @staticmethod
    def _set_endpoint(type_of_data: str, yesterday: datetime) -> str:
 
        if type_of_data == "stock":
            endpoint = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{yesterday}"
        elif type_of_data == "forex":
            endpoint = f"https://api.polygon.io/v2/aggs/grouped/locale/global/market/fx/{yesterday}"
        elif type_of_data == "crypto":
            endpoint = f"https://api.polygon.io/v2/aggs/grouped/locale/global/market/crypto/{yesterday}" 
        return endpoint
