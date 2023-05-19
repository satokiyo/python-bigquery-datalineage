# Standard Library
import datetime

# Third Party Library
import pytest

# First Party Library
from src.client_factory import ClientFactory
from src.lineage_api import LineageAPIWrapper

t_delta = datetime.timedelta(hours=9)
JST = datetime.timezone(t_delta, "JST")

factory = ClientFactory()
lineage_api = LineageAPIWrapper(factory)


@pytest.mark.test_target
@pytest.mark.parametrize(
    "project, location",
    [()],
)
def test_list_processes_in_project(project, location):
    response = lineage_api.list_processes(project, location)

    # Handle the response
    for item in response:
        print(item)


@pytest.mark.test_target
@pytest.mark.parametrize(
    "project, location, fully_qualified_name",
    [
        (),
    ],
)
def test_search_downstream_links_of_specific_resource(
    project, location, fully_qualified_name
):
    # search downstream links of fully_qualified_name
    response = lineage_api.search_downstream_links(
        project, location, fully_qualified_name
    )
    # Handle the response
    for item in response:
        print(f"name: {item.name}")
        print(f"source: {item.source}")
        print(f"target: {item.target}")  # downstream resource
        print(
            f"start_time: {item.start_time.astimezone(JST).strftime('%Y-%m-%d %H:%M:%S')}"
        )
        print(
            f"end_time: {item.end_time.astimezone(JST).strftime('%Y-%m-%d %H:%M:%S')}"
        )


@pytest.mark.test_target
@pytest.mark.parametrize(
    "project, location, fully_qualified_name",
    [
        (),
    ],
)
def test_search_upstream_links_of_specific_resource(
    project, location, fully_qualified_name
):
    # search upstream links of fully_qualified_name
    response = lineage_api.search_upstream_links(
        project, location, fully_qualified_name
    )
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


@pytest.mark.test_target
@pytest.mark.parametrize(
    "project, location, link_names",
    [
        (),
    ],
)
def test_batch_search_link_processes_assosiated_with_specific_links(
    project, location, link_names
):
    response = lineage_api.batch_search_link_processes(project, location, link_names)

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


@pytest.mark.test_target
@pytest.mark.parametrize(
    "project, location, process_id, run_id",
    [
        (),
    ],
)
def test_list_lineage_events_assosiated_with_specific_run(
    project, location, process_id, run_id
):
    response = lineage_api.list_lineage_events(project, location, process_id, run_id)
    # Handle the response
    for item in response:
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


@pytest.mark.test_target
@pytest.mark.parametrize(
    "project, location, process_id, run_id, lineage_event_id",
    [
        (),
    ],
)
def test_get_lineage_event(project, location, process_id, run_id, lineage_event_id):
    response = lineage_api.get_lineage_event(
        project,
        location,
        process_id,
        run_id,
        lineage_event_id,
    )

    # Handle the response
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
