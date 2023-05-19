# Standard Library
import datetime

# Third Party Library
import pandas as pd
import pytest

# First Party Library
from src.main import (
    display_last_updated_timestamp_for_lineage,
    recursive_search_creation_job_and_upstream_tables,
)

t_delta = datetime.timedelta(hours=9)
JST = datetime.timezone(t_delta, "JST")


@pytest.mark.test_target
@pytest.mark.parametrize(
    "project, location, fully_qualified_name",
    [
        (),
    ],
)
def test_display_last_updated_timestamp_for_lineage(
    project, location, fully_qualified_name
):
    display_last_updated_timestamp_for_lineage(project, location, fully_qualified_name)


@pytest.mark.test_target
@pytest.mark.parametrize(
    "project, location, fully_qualified_name",
    [
        (),
    ],
)
def test_recursive_search_creation_job_and_upstream_tables(
    project, location, fully_qualified_name
):
    results = recursive_search_creation_job_and_upstream_tables(
        project, location, fully_qualified_name
    )

    i = 0
    df_creation_jobs = pd.DataFrame()
    df_ups = pd.DataFrame()
    for result in results:
        creation_job, upstream_tables = result

        df_creation_jobs = pd.concat([df_creation_jobs, pd.DataFrame([creation_job])])
        df_ups = pd.concat([df_ups, pd.DataFrame(upstream_tables)])

        i += 1

    df_creation_jobs.to_csv(
        f"TEST_all_creation_jobs_{fully_qualified_name}.tsv", sep="\t", index=False
    )
    df_ups.to_csv(
        f"TEST_all_upstream_tables_{fully_qualified_name}.tsv", sep="\t", index=False
    )
