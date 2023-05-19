# Standard Library
import datetime

# Third Party Library
from google.cloud import datacatalog_v1beta1
import pytest

# First Party Library
from src.catalog_api import CatalogAPIWrapper
from src.client_factory import ClientFactory

t_delta = datetime.timedelta(hours=9)
JST = datetime.timezone(t_delta, "JST")

factory = ClientFactory()
catalog_api = CatalogAPIWrapper(factory)


@pytest.mark.test_target
@pytest.mark.parametrize(
    "project, location, type, name",
    [
        (),
    ],
)
def test_search_dataset_in_catalog(project, location, type, name):
    query = f"system=bigquery type={type} name:({name}) location={location}"

    try:
        results = catalog_api.search_catalog(
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


# @pytest.mark.test_target
@pytest.mark.parametrize(
    "project, location, type, name",
    [
        (),
    ],
)
def test_search_table_in_catalog(project, location, type, name):
    query = f"system=bigquery type={type} name:({name}) location={location}"

    try:
        results = catalog_api.search_catalog(
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
            is_entry_contains_name = [
                n in item.linked_resource for n in name.split("|")
            ]
            assert any(is_entry_contains_name) is True

            print(
                f"Result type: {datacatalog_v1beta1.SearchResultType(item.search_result_type).name}"
            )
            print(f"Result subtype: {item.search_result_subtype}")
            print(f"Relative resource name: {item.relative_resource_name}")
            print(f"Linked resource: {item.linked_resource}")
    except Exception:
        pytest.fail("Unexpected Error..")


@pytest.mark.test_target
@pytest.mark.parametrize(
    "project, location, type, name",
    [
        (),
    ],
)
def test_search_catalog_and_get_resource(project, location, type, name):
    query = f"system=bigquery type={type} name:({name}) location={location}"

    try:
        results = catalog_api.search_catalog(
            include_project_id=project,
            include_gcp_public_datasets=False,
            query=query,
        )
        for item in results:
            resource_name = item.linked_resource
            resource = catalog_api.get_resource(resource_name)
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


@pytest.mark.test_target
@pytest.mark.parametrize(
    "project, dataset, table",
    [
        (),
    ],
)
def test_get_resource_by_name(project, dataset, table):
    table_name = f"//bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}"

    try:
        resource = catalog_api.get_resource(table_name)
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
