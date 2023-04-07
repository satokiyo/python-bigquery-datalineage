# Standard Library
import argparse
import datetime

# First Party Library
from catalog_api import get_resource
from client_factory import ClientFactory
from lineage_api import search_downstream_links, search_upstream_links

t_delta = datetime.timedelta(hours=9)
JST = datetime.timezone(t_delta, "JST")


def show_upstream_lineage(project, location, fully_qualified_name, n=1, counter=0):
    """再帰関数

    Args:
        client (_type_): _description_
        project (_type_): _description_
        location (_type_): _description_
        fully_qualified_name (_type_): _description_
        n (int): n次のつながりまで表示する (max 3)
    """
    if n > 3:
        raise
    if n == counter:
        return
    counter += 1

    # Create a client
    lineage_client = ClientFactory.get_client("lineage")

    # upstream lineage
    response = search_upstream_links(
        lineage_client, project, location, fully_qualified_name
    )

    # Handle the response
    for item in response:
        source = item.source

        print("--------------------")
        print(f"upstream source: {source} (n={counter})")

        if not source.fully_qualified_name.startswith("bigquery:"):
            # bigquery以外のsource(ex: spreadsheet...etc.)
            continue

        project, dataset, table = source.fully_qualified_name.split(":")[-1].split(".")
        catalog_client = ClientFactory.get_client("catalog")
        # 検索条件
        table_name = f"//bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}"
        resource = get_resource(catalog_client, table_name)
        print(
            f'create_time: {resource.source_system_timestamps.create_time.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")}'
        )
        print(
            f'update_time: {resource.source_system_timestamps.update_time.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")}'
        )

        show_upstream_lineage(
            project, location, source.fully_qualified_name, n, counter
        )


def show_downstream_lineage(project, location, fully_qualified_name, n=1, counter=0):
    """再帰関数

    Args:
        client (_type_): _description_
        project (_type_): _description_
        location (_type_): _description_
        fully_qualified_name (_type_): _description_
        n (int): n次のつながりまで表示する (max 3)
    """
    if n > 3:
        raise
    if n == counter:
        return
    counter += 1

    # Create a client
    lineage_client = ClientFactory.get_client("lineage")

    # upstream lineage
    response = search_downstream_links(
        lineage_client, project, location, fully_qualified_name
    )

    # Handle the response
    for item in response:
        target = item.target

        print("--------------------")
        print(f"downstream source: {target} (n={counter})")

        if not target.fully_qualified_name.startswith("bigquery:"):
            # bigquery以外のtarget(ex: spreadsheet...etc.)
            continue

        project, dataset, table = target.fully_qualified_name.split(":")[-1].split(".")
        catalog_client = ClientFactory.get_client("catalog")
        # 検索条件
        table_name = f"//bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}"
        resource = get_resource(catalog_client, table_name)
        print(
            f'create_time: {resource.source_system_timestamps.create_time.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")}'
        )
        print(
            f'update_time: {resource.source_system_timestamps.update_time.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")}'
        )

        show_downstream_lineage(
            project, location, target.fully_qualified_name, n, counter
        )


def main(project, location, fully_qualified_name):
    print("start!!")
    print(f"fully_qualified_name: {fully_qualified_name}")

    show_upstream_lineage(project, location, fully_qualified_name, n=2)
    show_downstream_lineage(project, location, fully_qualified_name, n=2)

    print("finish!!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("project", type=str, help="project no")
    parser.add_argument("location", type=str, help="location")
    parser.add_argument("fqn", type=str, help="fully qualified name")
    args = parser.parse_args()

    project = args.project
    location = args.location
    fully_qualified_name = args.fqn

    main(project, location, fully_qualified_name)
