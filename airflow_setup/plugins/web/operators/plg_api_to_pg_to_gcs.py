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
    template_fields: Sequence[str] = (
        "time"
    )


    def  __init__(
		self,
        *,
		key: str,
		#destination_path: Optional[str] = None,
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
        #self.destination_path = self._set_destination_path(destination_path)


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

            # file_name = self.destination_path
            # df.to_csv(file_name, index=False)

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

    # @staticmethod
    # def _set_destination_path(path: Union[str, None]) -> str:
    #     if path is not None:
    #         return path.lstrip("/") if path.startswith("/") else path
    #     return ""

    # @staticmethod
    # def _set_bucket_name(name: str) -> str:
    #     bucket = name if not name.startswith("gs://") else name[5:]
    #     return bucket.strip("/")


# class PolygonToPostgresOperators(BaseOperator):
#     def __init__(
#         self,
#         api_token: str,
#         order_by: str,
#         gcp_conn_id: str = "google_cloud_default",
#         api_url: str = "https://data.sfgov.org/resource/",
#         destination_bucket:  Optional[str] = None,
#         delegete_to: Optional[str] = None,
#         impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
#         file_name: str = "output_file",  # Define a default file name
#         endpoint: str = "5cei-gny5.json", #Default Endpoint
#         *args,
#         **kwargs
#     ) -> None:
#         super().__init__(*args, **kwargs)
#         self.api_url = api_url
#         self.api_token = api_token
#         self.order_by = order_by
#         self.gcp_conn_id = gcp_conn_id
#         self.destination_bucket = destination_bucket
#         self.file_name = file_name
#         self.delegete_to = delegete_to
#         self.impersonation_chain = impersonation_chain
#         self.endpoint = endpoint

#     def execute(self, context: Dict[str, Any]) -> None:
#         # download it using requests via into a tempfile a pandas df
#         with tempfile.TemporaryDirectory() as tmpdirname:
#             api_url = self.api_url + self.endpoint
#             self.log.info(f"API url is: {self.api_url}")

#             limit = 500000
#             offset = 0
#             params = {
#                 '$limit': limit,
#                 '$offset': offset,
#                 '$order': self.order_by,  # Specify the field to order by
#             }

#             # Define the headers with the X-App-Token header
#             headers = {
#                 'X-App-Token': self.api_token,
#             }
#             # Make a GET request to the API with the headers and parameters
#             response = requests.get(api_url, headers=headers, params=params)

#             df = pd.DataFrame()

#             while True:
#                 # Make a GET request to the API with the headers and parameters
#                 response = requests.get(api_url, headers=headers, params=params)

#                 # Check the response status code
#                 if response.status_code == 200:
#                     data = response.json()
#                     df1 = pd.DataFrame(data)
#                     df = pd.concat([df,df1],ignore_index=True)
#                     self.log.info(f"Dataframe currently has {df.shape[0]} columns")


#                     # If the number of results received is less than the limit, you've reached the end
#                     if len(data) < limit:
#                         break

#                     # Increment the offset for the next page
#                     offset += limit
#                 else:
#                     self.log.info(f"Request failed with status code: {response.status_code}")
#                     break
#             csv_file = self.file_name+".csv"
#             df.to_csv(f'{tmpdirname}/{csv_file}', index=False)
#             # upload it to gcs using GCS hooks
#             gcs_hook = GCSHook(
#                 gcp_conn_id = self.gcp_conn_id,
#                 delegete_to = self.delegete_to,
#                 impersonation_chain = self.impersonation_chain
#             )
#             #Upload as a CSV file
#             gcs_hook.upload(
#                     bucket_name=self.destination_bucket,
#                     object_name=csv_file,
#                     filename=f'{tmpdirname}/{csv_file}',
#                     mime_type="text/csv",
#                     gzip=False,
#             )
#             self.log.info(f"Loaded CSV: {csv_file}")

#             #Upload as a Parquet file
#             parquet_file = self.file_name+".parquet"
#             df['file_date'] = pd.to_datetime(df['file_date'])
#             df.to_parquet(f'{tmpdirname}/{parquet_file}', engine='pyarrow')
#             # upload it to gcs using GCS hooks
#             gcs_hook.upload(
#                     bucket_name=self.destination_bucket,
#                     object_name=parquet_file,
#                     filename=f'{tmpdirname}/{parquet_file}',
#                     mime_type="application/parquet",
#                     gzip=False,
#             )
#             self.log.info(f"Loaded Parquet: {parquet_file}")


# from typing import Any, Dict, Optional

# from airflow.exceptions import AirflowException
# from airflow.providers.http.hooks.http import HttpHook
# from requests_oauthlib import OAuth2Session

# class Spotifyhook(HttpHook):

#     """
#     Interact with Spotify APIs.

#     :param method: the API method to be called
#     :param spotify_conn_id: The Spotify connection id. The name or identifier for
#         establishing a connection to Spotify that has the base API url
#         and optional authentication credentials. Default params and headers
#         can also be specified in the Extra field in json format.
#     """

#     def __init__(
#             self,
#             method: str = "GET",
#             Polygon_API_conn_id: str =  "polygon_conn_id",
#             **kwargs,
#     ) -> None:
#         super().__init__(method,**kwargs)
#         self.polygon_api_conn_id = Polygon_API_conn_id
#         self.base_url: str = ""

#     # headers can be passed through directly or in the "extra" field in the connection

#     def get_conn(self):

#         """
#         Returns OAuth2 session for use with requests

#         :param headers: additional headers to be passed through as a dictionary
#         """
#         conn = self.get_connection(self.polygon_api_conn_id)
#         self.extras = conn.extra_dejson.copy()

#          # https://api.com/v2/read
#         if conn.host and "://" in conn.host:
#             self.base_url = conn.host
#         else:
#             # schema defaults to HTTP
#             schema = conn.schema if conn.schema else "https"
#             host = conn.host if conn.host else ""
#             self.base_url = schema + "://" + host

#         self.log.info(f"Base url is: {self.base_url}")


#         try:
#             api_key = self.extras["api_token"]
#             refresh_token = self.extras["refresh_token"]
#             grant_type = self.extras["grant_type"]
#         except KeyError as e:
#             self.log.error(
#                 "Connection to {} requires value for extra field {}.".format(
#                     conn.host, e.args[0]
#                 )
#             )
#             raise AirflowException(
#                 f"Invalid value for extra field {e.args[0]} in Connection object"
#             )

#        extra_headers = {"Authorization" : f"Bearer {api_key}"}  # client_credentials= client_id:client_secret

        # #"grant_type":"refresh_token"  https://api.polygon.io   /v2/aggs/grouped/locale/us/market/stocks/
        # body = {"grant_type":grant_type,
        #                 grant_type: refresh_token}

        # token = session.refresh_token(
        #     self.base_url,
        #     refresh_token=refresh_token,
        #     headers=extra_headers,
        #     data = body
        # )

        # session.headers["Authorization"] = "Bearer {}".format(token['access_token'])
        # #session["headers"] = {"Authorization" : f"Bearer  {token['access_token']}"}


        # if headers:
        #     session.headers.update(headers)

        # self.log.info(f"final headers used: {session.headers}")
        # return session

    def url_from_endpoint(self, endpoint: Optional[str]) -> str:
        """Overwrite parent `url_from_endpoint` method"""
        return endpoint