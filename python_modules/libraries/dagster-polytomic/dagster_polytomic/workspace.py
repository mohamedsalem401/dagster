from functools import cached_property

import dagster as dg
from polytomic import Polytomic
from pydantic import Field

from dagster_polytomic.objects import (
    PolytomicBulkSync,
    PolytomicBulkSyncSchema,
    PolytomicConnection,
    PolytomicWorkspaceData,
)

POLYTOMIC_CLIENT_VERSION = "2024-02-08"


class PolytomicWorkspace(dg.Resolvable, dg.Model):
    """Handles all interactions with the Polytomic API to fetch and manage state."""

    api_key: str = Field(
        description="The API key to your Polytomic organization.",
        examples=['"{{ env.POLYTOMIC_API_KEY }}"'],
        repr=False,
    )

    @cached_property
    def client(self) -> Polytomic:
        return Polytomic(
            version=POLYTOMIC_CLIENT_VERSION,
            token=self.api_key,
        )

    def _fetch_connections(self) -> list[PolytomicConnection]:
        """Fetch all connections."""
        response = self.client.connections.list().data or []
        return [PolytomicConnection.from_api_response(conn) for conn in response]

    def _fetch_bulk_syncs(self) -> list[PolytomicBulkSync]:
        """Fetch all bulk syncs."""
        response = self.client.bulk_sync.list().data or []
        return [PolytomicBulkSync.from_api_response(sync) for sync in response]

    def _fetch_bulk_sync_schemas(self, bulk_sync_id: str) -> list[PolytomicBulkSyncSchema]:
        """Fetch all schemas for a specific bulk sync."""
        response = self.client.bulk_sync.schemas.list(id=bulk_sync_id).data or []
        return [PolytomicBulkSyncSchema.from_api_response(schema) for schema in response]

    async def fetch_polytomic_state(self) -> PolytomicWorkspaceData:
        """Fetch all connections, bulks syncs and schemas from the Polytomic API.

        This is the main public method for getting complete Polytomic state.
        """
        connections = self._fetch_connections()
        bulk_syncs = self._fetch_bulk_syncs()
        schemas = {}
        for bulk_sync in bulk_syncs:
            schemas[bulk_sync.id] = self._fetch_bulk_sync_schemas(bulk_sync_id=bulk_sync.id)

        return PolytomicWorkspaceData(
            connections=connections,
            bulk_syncs=bulk_syncs,
            schemas_by_bulk_sync_id=schemas,
        )
