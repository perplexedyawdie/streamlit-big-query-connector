from streamlit.connections import ExperimentalBaseConnection
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import streamlit as st
import json

class BigQueryConnection(ExperimentalBaseConnection[bigquery.Client]):

    def _connect(self, **kwargs) -> bigquery.Client:
        # If the service account details are passed directly, use them
        if 'service_account_location' in kwargs:
            sa_loc = kwargs.pop('service_account_info')
            cred = service_account.Credentials.from_service_account_file(filename=sa_loc)
            return bigquery.Client(credentials=cred, project=cred.project_id)
        else:
            sa_dict = dict()
            sa_dict["type"] = self._secrets['type']
            sa_dict["project_id"] = self._secrets['project_id']
            sa_dict["private_key_id"] = self._secrets['private_key_id']
            sa_dict["private_key"] = self._secrets['private_key']
            sa_dict["client_email"] = self._secrets['client_email']
            sa_dict["client_id"] = self._secrets['client_id']
            sa_dict["auth_uri"] = self._secrets['auth_uri']
            sa_dict["token_uri"] = self._secrets['token_uri']
            sa_dict["auth_provider_x509_cert_url"] = self._secrets['auth_provider_x509_cert_url']
            sa_dict["client_x509_cert_url"] = self._secrets['client_x509_cert_url']
            sa_dict["universe_domain"] = self._secrets['universe_domain']

            # Create credentials from the service account info
            cred = service_account.Credentials.from_service_account_info(info=sa_dict)
            # Connect to BigQuery with the created credentials
            return bigquery.Client(credentials=cred, project=cred.project_id)

    def client(self) -> bigquery.Client:
        return self._instance

    @st.cache_data(ttl=3600)
    def query(_self, query: str) -> pd.DataFrame:
        return _self.client().query(query).to_dataframe()
    