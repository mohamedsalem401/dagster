import pytest
from dagster_polytomic.component import PolytomicComponent
from dagster_polytomic.workspace import PolytomicWorkspace


@pytest.fixture
def polytomic_workspace() -> PolytomicWorkspace:
    return PolytomicWorkspace(api_key="test-key")


@pytest.fixture
def component(polytomic_workspace: PolytomicWorkspace) -> PolytomicComponent:
    return PolytomicComponent(workspace=polytomic_workspace)
