# Third Party Library
from google.cloud.datacatalog import lineage_v1


def search_downstream_links(
    client: lineage_v1.LineageClient,
    project: str,
    location: str,
    fully_qualified_name: str,
):
    """Retrieve a list of downstream links connected to a specific asset.
    fully_qualified_nameがsourceになるリンク(source-targetのペア)を一覧表示する。
    Links represent the data flow between source (upstream) and target (downstream) assets in transformation pipelines.
    Links are stored in the same project as the Lineage Events that create them.

    Args:
        client (lineage_v1.LineageClient): _description_
        project (str): _description_
        location (str): _description_
        fully_qualified_name (str): _description_
    """
    # Initialize request argument(s)
    source = lineage_v1.EntityReference()
    source.fully_qualified_name = fully_qualified_name
    parent = f"projects/{project}/locations/{location}"

    request = lineage_v1.SearchLinksRequest(
        source=source,
        parent=parent,
    )

    # Make the request
    return client.search_links(request=request)


def search_upstream_links(
    client: lineage_v1.LineageClient,
    project: str,
    location: str,
    fully_qualified_name: str,
):
    """Retrieve a list of upstream links connected to a specific asset.
    fully_qualified_nameがtargetになるリンク(source-targetのペア)を一覧表示する。
    Links represent the data flow between source (upstream) and target (downstream) assets in transformation pipelines.
    Links are stored in the same project as the Lineage Events that create them.

    Args:
        client (lineage_v1.LineageClient): _description_
        project (str): _description_
        location (str): _description_
        fully_qualified_name (str): _description_
    """
    # Initialize request argument(s)
    target = lineage_v1.EntityReference()
    target.fully_qualified_name = fully_qualified_name
    parent = f"projects/{project}/locations/{location}"

    request = lineage_v1.SearchLinksRequest(
        target=target,
        parent=parent,
    )

    # Make the request
    return client.search_links(request=request)


def batch_search_link_processes(
    client: lineage_v1.LineageClient, project, location, link_names
):
    """Retrieve information about LineageProcesses associated with specific links.
    LineageProcesses are transformation pipelines that result in data flowing from source to target assets.
    Links between assets represent this operation.

    Args:
        client (lineage_v1.LineageClient): _description_
        project (str):
        location (str):
        link_names (str):
    """
    # Initialize request argument(s)
    request = lineage_v1.BatchSearchLinkProcessesRequest(
        parent=f"projects/{project}/locations/{location}",
        links=[
            f"projects/{project}/locations/{location}/links/{link_name}"
            for link_name in link_names
        ],
    )

    # Make the request
    return client.batch_search_link_processes(request=request)


def list_lineage_events(
    client: lineage_v1.LineageClient,
    project: str,
    location: str,
    process_id: str,
    run_id: str,
):
    """Lists lineage events in the given project and location. The list order is not defined.

    Args:
        client (lineage_v1.LineageClient): _description_
        project (str):
        location (str):
        process_id (str):
        run_id (str): The id of the run that owns the collection of lineage events to get.
    """
    # Initialize request argument(s)
    run_name = (
        f"projects/{project}/locations/{location}/processes/{process_id}/runs/{run_id}"
    )
    request = lineage_v1.ListLineageEventsRequest(
        parent=run_name,
    )

    # Make the request
    return client.list_lineage_events(request=request)


def get_lineage_event(
    client: lineage_v1.LineageClient,
    project: str,
    location: str,
    process_id: str,
    run_id: str,
    lineage_event_id: str,
):
    """Gets details of a specified lineage event.

    Args:
        client (lineage_v1.LineageClient): _description_
        project (str):
        location (str):
        process_id (str):
        run_id (str):
        lineage_event_id: The id of the lineage event to get.
    """
    # Initialize request argument(s)

    lineage_event_name = f"projects/{project}/locations/{location}/processes/{process_id}/runs/{run_id}/lineageEvents/{lineage_event_id}"
    request = lineage_v1.GetLineageEventRequest(
        name=lineage_event_name,
    )

    # Make the request
    return client.get_lineage_event(request=request)


def get_process(client: lineage_v1.LineageClient, name: str):
    """Gets the details of the specified process.

    Args:
        client (lineage_v1.LineageClient): _description_
        name: The name of the process to get.
    """
    # Initialize request argument(s)
    request = lineage_v1.GetProcessRequest(
        name=name,
    )

    # Make the request
    response = client.get_process(request=request)

    # Handle the response
    print(response)


def get_run(client: lineage_v1.LineageClient, name: str):
    """Gets the details of the specified run.

    Args:
        client (lineage_v1.LineageClient): _description_
        name (str): The name of the run to get.
    """
    # Create a client
    client = lineage_v1.LineageClient()

    # Initialize request argument(s)
    request = lineage_v1.GetRunRequest(
        name=name,
    )

    # Make the request
    response = client.get_run(request=request)

    # Handle the response
    print(response)


def list_processes(client: lineage_v1.LineageClient, project: str, location: str):
    """List processes in the given project and location. List order is descending by insertion time.

    Args:
        client (lineage_v1.LineageClient): _description_
        project (str):
        location (str):
    """
    # The name of the project and its location that owns this collection of processes.
    parent = f"projects/{project}/locations/{location}"
    # Initialize request argument(s)
    request = lineage_v1.ListProcessesRequest(
        parent=parent,
    )

    # Make the request
    return client.list_processes(request=request)
