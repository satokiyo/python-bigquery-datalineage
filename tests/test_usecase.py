# Standard Library
import datetime

# Third Party Library
import pytest

# First Party Library
from src.main import main

t_delta = datetime.timedelta(hours=9)
JST = datetime.timezone(t_delta, "JST")


@pytest.mark.test_target
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
def test_main_usecase(project, location, fully_qualified_name):
    main(project, location, fully_qualified_name)
