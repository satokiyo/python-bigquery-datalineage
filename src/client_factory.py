# Third Party Library
from google.cloud import datacatalog_v1beta1
from google.cloud.datacatalog import lineage_v1


class ClientFactory:
    def __new__(cls, *args, **kargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super(ClientFactory, cls).__new__(cls)
        return cls._instance

    __clients = {
        "catalog": datacatalog_v1beta1.DataCatalogClient(),
        "lineage": lineage_v1.LineageClient(),
    }

    def __init__(self):
        pass

    @classmethod
    def get_client(
        self,
        client_type,
    ):
        return self.__clients.get(client_type)
