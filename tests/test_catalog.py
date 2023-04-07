# Standard Library
import datetime

# Third Party Library
from google.cloud import datacatalog_v1beta1
import pytest

# First Party Library
from src.catalog_api import get_resource, search_catalog
from src.client_factory import ClientFactory

t_delta = datetime.timedelta(hours=9)
JST = datetime.timezone(t_delta, "JST")


@pytest.mark.parametrize(
    "project, location, type, name",
    [
        (
            "",
            "",
            "",
            "",
        ),
    ],
)
def test_search_dataset_in_catalog(project, location, type, name):
    # 検索条件
    query = f"system=bigquery type={type} name:({name}) location={location}"

    client = ClientFactory.get_client("catalog")

    try:
        results = search_catalog(
            client=client,
            include_project_id=project,
            include_gcp_public_datasets=False,
            query=query,
        )

        for item in results:
            assert (
                datacatalog_v1beta1.SearchResultType(item.search_result_type).name
                == "ENTRY"
            )
            assert type in item.search_result_subtype
            assert name in item.linked_resource

            print(
                f"Result type: {datacatalog_v1beta1.SearchResultType(item.search_result_type).name}"
            )
            print(f"Result subtype: {item.search_result_subtype}")
            print(f"Relative resource name: {item.relative_resource_name}")
            print(f"Linked resource: {item.linked_resource}")
    except Exception:
        pytest.fail("Unexpected Error..")


@pytest.mark.parametrize(
    "project, location, type, name",
    [
        (
            "",
            "",
            "",
            "",
        ),
    ],
)
def test_search_table_in_catalog(project, location, type, name):
    # 検索条件
    query = f"system=bigquery type={type} name:({name}) location={location}"

    client = ClientFactory.get_client("catalog")

    try:
        results = search_catalog(
            client=client,
            include_project_id=project,
            include_gcp_public_datasets=False,
            query=query,
        )

        for item in results:
            assert (
                datacatalog_v1beta1.SearchResultType(item.search_result_type).name
                == "ENTRY"
            )
            assert type in item.search_result_subtype
            assert name in item.linked_resource

            print(
                f"Result type: {datacatalog_v1beta1.SearchResultType(item.search_result_type).name}"
            )
            print(f"Result subtype: {item.search_result_subtype}")
            print(f"Relative resource name: {item.relative_resource_name}")
            print(f"Linked resource: {item.linked_resource}")
    except Exception:
        pytest.fail("Unexpected Error..")


@pytest.mark.parametrize(
    "project, location, type, name",
    [
        (
            "",
            "",
            "",
            "",
        ),
    ],
)
def test_search_catalog_and_get_resource(project, location, type, name):
    # 検索条件
    query = f"system=bigquery type={type} name:({name}) location={location}"

    client = ClientFactory.get_client("catalog")

    try:
        results = search_catalog(
            client=client,
            include_project_id=project,
            include_gcp_public_datasets=False,
            query=query,
        )
        for item in results:
            resource_name = item.linked_resource
            resource = get_resource(client, resource_name)
            # print(resource)
            print("\n")
            print(f"name: {resource.name}")
            print(f"linked_resource: {resource.linked_resource}")
            print(resource.type_)
            print(resource.integrated_system)
            print(
                f'create_time: {resource.source_system_timestamps.create_time.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")}'
            )
            print(
                f'update_time: {resource.source_system_timestamps.update_time.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")}'
            )
    except Exception:
        pytest.fail("Unexpected Error..")


@pytest.mark.parametrize(
    "project, dataset, table",
    [
        (
            "",
            "",
            "",
        ),
    ],
)
def test_get_resource_by_name(project, dataset, table):
    # 検索条件
    table_name = f"//bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}"

    client = ClientFactory.get_client("catalog")

    try:
        resource = get_resource(client, table_name)
        # print(resource)
        print("\n")
        print(f"name: {resource.name}")
        print(f"linked_resource: {resource.linked_resource}")
        print(resource.type_)
        print(resource.integrated_system)
        print(
            f'create_time: {resource.source_system_timestamps.create_time.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")}'
        )
        print(
            f'update_time: {resource.source_system_timestamps.update_time.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")}'
        )
    except Exception:
        pytest.fail("Unexpected Error..")
