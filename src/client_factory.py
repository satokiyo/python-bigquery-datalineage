# Standard Library
from enum import Enum

# Third Party Library
from google.cloud import bigquery, datacatalog_v1beta1
from google.cloud.datacatalog import lineage_v1


class ClientType(Enum):
    CATALOG = "catalog"
    LINEAGE = "lineage"
    BIGQUERY = "bigquery"


class ClientFactory:
    def __new__(cls, *args, **kargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super(ClientFactory, cls).__new__(cls)
        return cls._instance

    def get_client(
        self,
        client_type: ClientType,
        project=None,
    ):
        if client_type.value == ClientType.CATALOG.value:
            return datacatalog_v1beta1.DataCatalogClient()
        elif client_type.value == ClientType.LINEAGE.value:
            return lineage_v1.LineageClient()
        elif client_type.value == ClientType.BIGQUERY.value:
            if project is not None:
                return bigquery.Client(project)
            return bigquery.Client()
        else:
            raise Exception(f"ClientType {client_type} not found.")
