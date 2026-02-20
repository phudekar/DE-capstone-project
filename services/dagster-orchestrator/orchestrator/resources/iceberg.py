"""Iceberg resource for Dagster — wraps PyIceberg catalog."""

from __future__ import annotations

import logging

from dagster import ConfigurableResource

logger = logging.getLogger(__name__)


class IcebergResource(ConfigurableResource):
    """Dagster resource wrapping PyIceberg REST catalog."""

    catalog_uri: str = "http://iceberg-rest:8181"
    catalog_name: str = "lakehouse"
    s3_endpoint: str = "http://minio:9000"
    s3_access_key: str = "minio"
    s3_secret_key: str = "minio123"
    s3_region: str = "us-east-1"

    def get_catalog(self):
        """Return a PyIceberg catalog instance."""
        from pyiceberg.catalog import load_catalog

        return load_catalog(
            self.catalog_name,
            **{
                "type": "rest",
                "uri": self.catalog_uri,
                "s3.endpoint": self.s3_endpoint,
                "s3.access-key-id": self.s3_access_key,
                "s3.secret-access-key": self.s3_secret_key,
                "s3.region": self.s3_region,
            },
        )

    def load_table(self, full_name: str):
        """Load an Iceberg table by full name (e.g. 'bronze.raw_trades')."""
        catalog = self.get_catalog()
        return catalog.load_table(full_name)
