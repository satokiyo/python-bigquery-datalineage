# Standard Library
import argparse
from dataclasses import dataclass
import datetime
from typing import List, Optional, Tuple

# First Party Library
from bigquery_api import BigQueryAPIWrapper
from catalog_api import CatalogAPIWrapper
from client_factory import ClientFactory
from lineage_api import LineageAPIWrapper

t_delta = datetime.timedelta(hours=9)
JST = datetime.timezone(t_delta, "JST")

factory = ClientFactory()
catalog_api = CatalogAPIWrapper(factory)
lineage_api = LineageAPIWrapper(factory)
bigquery_api = BigQueryAPIWrapper(factory)


def display_timestamp_upstream_lineage(
    project, location, fully_qualified_name, n=1, counter=0
):
    """Recursively show upstream lineage

    Args:
        project: project id
        location: location of a table
        fully_qualified_name: fqn of a table
        n (int): display the lineage chain up to the nth degree (up to 100)
    """
    if n > 100:
        raise
    if n == counter:
        return
    counter += 1

    # upstream lineage
    upstream_links = lineage_api.search_upstream_links(
        project, location, fully_qualified_name
    )

    # Handle the response
    for link in upstream_links:
        source = link.source

        print("--------------------")
        print(f"upstream source: {source} (n={counter})")

        if not source.fully_qualified_name.startswith("bigquery:"):
            # ex: spreadsheet...etc.
            continue

        project, dataset, table = source.fully_qualified_name.split(":")[-1].split(".")

        table_name = f"//bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}"
        resource = catalog_api.get_resource(table_name)
        print(
            f'create_time: {resource.source_system_timestamps.create_time.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")}'
        )
        print(
            f'update_time: {resource.source_system_timestamps.update_time.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")}'
        )

        display_timestamp_upstream_lineage(
            project, location, source.fully_qualified_name, n, counter
        )


def display_timestamp_downstream_lineage(
    project, location, fully_qualified_name, n=1, counter=0
):
    """Recursively show upstream lineage

    Args:
        project: project id
        location: location of a table
        fully_qualified_name: fqn of a table
        n (int): display the lineage chain up to the nth degree (up to 100)
    """
    if n > 100:
        raise
    if n == counter:
        return
    counter += 1

    # upstream lineage
    response = lineage_api.search_downstream_links(
        project, location, fully_qualified_name
    )

    # Handle the response
    for link in response:
        target = link.target

        print("--------------------")
        print(f"downstream source: {target} (n={counter})")

        if not target.fully_qualified_name.startswith("bigquery:"):
            # ex: spreadsheet...etc.
            continue

        project, dataset, table = target.fully_qualified_name.split(":")[-1].split(".")

        table_name = f"//bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}"
        resource = catalog_api.get_resource(table_name)
        print(
            f'create_time: {resource.source_system_timestamps.create_time.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")}'
        )
        print(
            f'update_time: {resource.source_system_timestamps.update_time.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")}'
        )

        display_timestamp_downstream_lineage(
            project, location, target.fully_qualified_name, n, counter
        )


def display_last_updated_timestamp_for_lineage(
    project, location, fully_qualified_name, n=2
):
    print("start!!")
    print(f"fully_qualified_name: {fully_qualified_name}")

    display_timestamp_upstream_lineage(project, location, fully_qualified_name, n=n)
    display_timestamp_downstream_lineage(project, location, fully_qualified_name, n=n)

    print("finish!!")


@dataclass
class CreationJob:
    target_table: str = ""
    n: int = -1
    process: str = ""
    origin_name: str = ""
    origin_type: str = ""
    origin_project: str = ""
    bq_job_id: str = ""
    bq_job_type: str = ""
    bq_job_created: str = ""
    bq_job_ended: str = ""
    bq_job_query: str = ""
    bq_job_user_email: str = ""


@dataclass
class TableInfo:
    project: str = ""
    dataset: str = ""
    table: str = ""
    table_source_type: str = ""
    n: int = -1
    create_time: str = ""
    update_time: str = ""
    location: str = ""
    has_upstream: bool = False


def search_creation_job(
    project, location, fully_qualified_name, n
) -> Optional[CreationJob]:
    print("search creation job of {fully_qualified_name} start! n={n}")

    # upstream lineage
    upstream_links = lineage_api.search_upstream_links(
        project, location, fully_qualified_name
    )

    # Handle the response
    link_names = [link.name for link in upstream_links]

    if not link_names:
        return None

    processes = lineage_api.batch_search_link_processes(project, location, link_names)
    if not processes:
        raise Exception("No process found.")

    process_names = [process.process for process in processes]
    # Confirm only one process.
    process_name = process_names[0]

    process = lineage_api.get_process(process_name)

    job_id = process.attributes["job_id"]
    origin_type = (
        process.origin.source_type
    )  # https://cloud.google.com/python/docs/reference/lineage/latest/google.cloud.datacatalog.lineage_v1.types.Origin#google_cloud_datacatalog_lineage_v1_types_Origin_SourceType
    origin_project = process.origin.name.split(":")[0]

    creation_job = {
        "target_table": fully_qualified_name,
        "n": n,
        "process": process_name,
        "origin_name": process.origin.name,
        "origin_type": origin_type,
        "origin_project": origin_project,
    }

    if not origin_type == 2:  # <SourceType.BIGQUERY: 2>
        return CreationJob(**creation_job)

    bq_job = bigquery_api.get_job(location, job_id, origin_project)
    creation_job.update(
        {
            "bq_job_id": bq_job.job_id,
            "bq_job_type": bq_job.job_type,
            "bq_job_created": bq_job.created.astimezone(JST).isoformat(),
            "bq_job_ended": bq_job.ended.astimezone(JST).isoformat(),
            "bq_job_query": bq_job.query,
            "bq_job_user_email": bq_job.user_email,
        },
    )
    return CreationJob(**creation_job)


def search_upstream_tables(
    project, location, fully_qualified_name, n
) -> List[TableInfo]:
    # upstream lineage
    upstream_links = lineage_api.search_upstream_links(
        project, location, fully_qualified_name
    )
    upstream_tables = []
    for link in upstream_links:
        source = link.source

        if not source.fully_qualified_name.startswith("bigquery:"):
            # ex: spreadsheet...etc.
            table_info_attrs = {
                "table": source.fully_qualified_name,
                "n": n + 1,
            }
            upstream_tables.append(TableInfo(**table_info_attrs))
            continue

        (
            source_project,
            source_dataset,
            source_table,
        ) = source.fully_qualified_name.split(":")[-1].split(".")

        # target source table
        table_name = f"//bigquery.googleapis.com/projects/{source_project}/datasets/{source_dataset}/tables/{source_table}"

        resource = catalog_api.get_resource(table_name)
        # ex: resource.name 'projects/project_name/locations/asia-northeast1/entryGroups/@bigquery/entries/cHJvamVjdHMvZHRlY2hsYWItaW50L2RhdGFzZXRzL3NmYS90YWJsZXMvS19DT05UUkFDVF9ERVRBSUw'
        source_location = resource.name.split("/")[3]
        source_create_time = resource.source_system_timestamps.create_time.astimezone(
            JST
        ).strftime("%Y-%m-%d %H:%M:%S")
        source_update_time = resource.source_system_timestamps.update_time.astimezone(
            JST
        ).strftime("%Y-%m-%d %H:%M:%S")

        table_source_type = bigquery_api.get_table_type(
            source_project, source_dataset, source_table
        )

        # Check if the source table has further upstream lineage links.
        source_upstream_links = lineage_api.search_upstream_links(
            source_project, source_location, source.fully_qualified_name
        )
        has_upstream = any([link.name for link in source_upstream_links])

        table_info_attrs = {
            "project": source_project,
            "dataset": source_dataset,
            "table": source_table,
            "table_source_type": table_source_type,
            "n": n + 1,
            "create_time": source_create_time,
            "update_time": source_update_time,
            "location": source_location,
            "has_upstream": has_upstream,
        }
        upstream_tables.append(TableInfo(**table_info_attrs))

    return upstream_tables


def recursive_search_creation_job_and_upstream_tables(
    project,
    location,
    fully_qualified_name,
    outputs: List[Tuple[CreationJob, List[TableInfo]]] = None,
    n=0,
):
    print(f"recursive search start! target={fully_qualified_name} n={n}")
    if n > 5:  # up to 5
        return outputs

    outputs = outputs or []

    creation_job = search_creation_job(project, location, fully_qualified_name, n)
    upstream_tables = search_upstream_tables(project, location, fully_qualified_name, n)

    outputs.append((creation_job, upstream_tables))

    for source in upstream_tables:
        if (
            not source.table_source_type
        ):  # If the upstream source is not a table, but a spreadsheet ...etc.
            continue
        project_ = source.project
        dataset_ = source.dataset
        location_ = source.location
        table_ = source.table

        fully_qualified_name_ = f"bigquery:{project_}.{dataset_}.{table_}"

        recursive_search_creation_job_and_upstream_tables(
            project_, location_, fully_qualified_name_, outputs, n + 1
        )

    return outputs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("project", type=str, help="project no")
    parser.add_argument("location", type=str, help="location")
    parser.add_argument("fqn", type=str, help="fully qualified name")
    parser.add_argument(
        "--n",
        type=int,
        default=2,
        help="display the lineage chain up to the nth degree",
    )
    args = parser.parse_args()

    project = args.project
    location = args.location
    fully_qualified_name = args.fqn
    n = args.n

    display_last_updated_timestamp_for_lineage(
        project, location, fully_qualified_name, n
    )
