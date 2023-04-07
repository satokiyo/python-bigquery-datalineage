# Third Party Library
from google.cloud import datacatalog_v1beta1
from google.cloud.datacatalog_v1beta1 import services as datacatalog_svc
from google.cloud.datacatalog_v1beta1 import types as datacatalog_types


def search_catalog(
    client: datacatalog_v1beta1.DataCatalogClient,
    include_project_id: str,
    include_gcp_public_datasets: bool,
    query: str,
) -> datacatalog_svc.data_catalog.pagers.SearchCatalogPager:
    """
    Search Catalog
    Args:
        include_project_id (str): Your Google Cloud project ID.
        include_gcp_public_datasets (bool): If true, include Google Cloud Platform (GCP) public
        datasets in the search results.
        query (str): Your query string.
        See: https://cloud.google.com/data-catalog/docs/how-to/search-reference
        Example: system=bigquery type=dataset
    """

    include_project_ids = [include_project_id]

    scope = {
        "include_project_ids": include_project_ids,
        "include_gcp_public_datasets": include_gcp_public_datasets,
    }

    # Iterate over all results
    return client.search_catalog(request={"scope": scope, "query": query})


def get_resource(client, resource_name: str) -> datacatalog_types.datacatalog.Entry:
    return (datacatalog_types.datacatalog.Entry)(
        client.lookup_entry(request={"linked_resource": resource_name})
    )
