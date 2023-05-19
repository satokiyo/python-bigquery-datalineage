# First Party Library
from client_factory import ClientFactory, ClientType


class BigQueryAPIWrapper:
    def __init__(self, factory: ClientFactory) -> None:
        self.factory = factory

    def client(self, project=None):
        if project is not None:
            return self.factory.get_client(ClientType("bigquery"), project)
        return self.factory.get_client(ClientType("bigquery"))

    def get_job(
        self,
        location: str,
        job_id: str,
        project: str = None,
    ) -> None:
        if project is not None:
            return self.client(project).get_job(job_id, location=location)
        return self.client.get_job(job_id, location=location)

    def get_table_type(
        self,
        project,
        dataset,
        table,
    ) -> str:
        table_ref = self.client(project).dataset(dataset, project=project).table(table)
        table = self.client(project).get_table(table_ref)
        return table.table_type
