# Standard Library
import datetime

# Third Party Library
import pytest

# First Party Library
from src.client_factory import ClientFactory

t_delta = datetime.timedelta(hours=9)
JST = datetime.timezone(t_delta, "JST")


# First Party Library
from src.lineage_api import (
    batch_search_link_processes,
    get_lineage_event,
    list_lineage_events,
    list_processes,
    search_downstream_links,
    search_upstream_links,
)


@pytest.mark.parametrize(
    "project, location",
    [
        (
            "",
            "",
        )
    ],
)
def test_list_processes_in_project(project, location):
    # Create a client
    client = ClientFactory.get_client("lineage")

    response = list_processes(client, project, location)

    # Handle the response
    for item in response:
        print(item)


@pytest.mark.parametrize(
    "project, location, fully_qualified_name",
    [
        (
            "",
            "",
            "",
        ),
    ],
)
def test_search_downstream_links_of_specific_resource(
    project, location, fully_qualified_name
):
    # Create a client
    client = ClientFactory.get_client("lineage")

    # search downstream links of fully_qualified_name
    response = search_downstream_links(client, project, location, fully_qualified_name)
    # Handle the response
    for item in response:
        # print(item)
        print(f"name: {item.name}")
        print(f"source: {item.source}")
        print(f"target: {item.target}")  # downstream resource
        print(
            f"start_time: {item.start_time.astimezone(JST).strftime('%Y-%m-%d %H:%M:%S')}"
        )
        print(
            f"end_time: {item.end_time.astimezone(JST).strftime('%Y-%m-%d %H:%M:%S')}"
        )


@pytest.mark.parametrize(
    "project, location, fully_qualified_name",
    [
        (
            "",
            "",
            "",
        ),
    ],
)
def test_search_upstream_links_of_specific_resource(
    project, location, fully_qualified_name
):
    # Create a client
    client = ClientFactory.get_client("lineage")

    # search upstream links of fully_qualified_name
    response = search_upstream_links(client, project, location, fully_qualified_name)
    # Handle the response
    for item in response:
        # print(item)
        print(f"name: {item.name}")
        print(f"source: {item.source}")
        print(f"target: {item.target}")  # upstream resource
        print(
            f"start_time: {item.start_time.astimezone(JST).strftime('%Y-%m-%d %H:%M:%S')}"
        )
        print(
            f"end_time: {item.end_time.astimezone(JST).strftime('%Y-%m-%d %H:%M:%S')}"
        )


@pytest.mark.parametrize(
    "project, location, link_names",
    [
        ("", "", [""]),
    ],
)
def test_batch_search_link_processes_assosiated_with_specific_links(
    project, location, link_names
):
    # Create a client
    client = ClientFactory.get_client("lineage")

    response = batch_search_link_processes(client, project, location, link_names)

    # Handle the response
    for item in response:
        print(f"process: {item.process}")
        for link in item.links:
            print(f"link.link: {link.link}")
            print(
                f"link.start_time: {link.start_time.astimezone(JST).strftime('%Y-%m-%d %H:%M:%S')}"
            )
            print(
                f"link.end_time: {link.end_time.astimezone(JST).strftime('%Y-%m-%d %H:%M:%S')}"
            )


@pytest.mark.parametrize(
    "project, location, process_id, run_id",
    [
        (
            "",
            "",
            "",
            "",
        ),
    ],
)
def test_list_lineage_events_assosiated_with_specific_run(
    project, location, process_id, run_id
):
    # Create a client
    client = ClientFactory.get_client("lineage")
    response = list_lineage_events(client, project, location, process_id, run_id)
    # Handle the response
    for item in response:
        # print(item)
        print(f"lineageEvent name: {item.name}")
        for link in item.links:
            print(f"link.source: {link.source}")
            print(f"link.target: {link.target}")
        print(
            f"start_time: {item.start_time.astimezone(JST).strftime('%Y-%m-%d %H:%M:%S')}"
        )
        print(
            f"end_time: {item.end_time.astimezone(JST).strftime('%Y-%m-%d %H:%M:%S')}"
        )


@pytest.mark.parametrize(
    "project, location, process_id, run_id, lineage_event_id",
    [
        (
            "",
            "",
            "",
            "",
            "",
        ),
    ],
)
def test_get_lineage_event(project, location, process_id, run_id, lineage_event_id):
    # Create a client
    client = ClientFactory.get_client("lineage")

    response = get_lineage_event(
        client,
        project,
        location,
        process_id,
        run_id,
        lineage_event_id,
    )

    # Handle the response
    # print(response)
    print(f"lineageEvent name: {response.name}")
    for link in response.links:
        print(f"link.source: {link.source}")
        print(f"link.target: {link.target}")
    print(
        f"start_time: {response.start_time.astimezone(JST).strftime('%Y-%m-%d %H:%M:%S')}"
    )
    print(
        f"end_time: {response.end_time.astimezone(JST).strftime('%Y-%m-%d %H:%M:%S')}"
    )
