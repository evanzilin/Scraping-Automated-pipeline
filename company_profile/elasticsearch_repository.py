"""Elasticsearch: search by domain only (local search layer before Postgres)."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class ElasticsearchCompanyRepository:
    """
    Search the company index by normalized ``company_domain``.

    Documents are expected to include at least ``company_domain`` and ``id``
    (Postgres primary key / row identifier for ``company_dataset``).
    """

    def __init__(
        self,
        hosts: str | list[str],
        index: str,
        *,
        api_key: Optional[str] = None,
        verify_certs: bool = True,
    ) -> None:
        from elasticsearch import Elasticsearch

        kwargs: Dict[str, Any] = {}
        if api_key:
            kwargs["api_key"] = api_key
        self._client = Elasticsearch(hosts, verify_certs=verify_certs, **kwargs)
        self._index = index

    @classmethod
    def from_env(cls) -> ElasticsearchCompanyRepository:
        url = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
        index = os.environ.get("ELASTICSEARCH_INDEX_COMPANY", "company_dataset")
        api_key = os.environ.get("ELASTICSEARCH_API_KEY") or None
        return cls(url, index, api_key=api_key)

    def find_company_id_by_domain(self, company_domain: str) -> Optional[str]:
        """
        Return the document / company ``id`` linked to Postgres, or None.

        Tries ``term`` on ``company_domain.keyword`` then a ``match`` on ``company_domain``.
        """
        domain = (company_domain or "").strip().lower()
        if not domain:
            return None

        body = {
            "size": 1,
            "query": {
                "bool": {
                    "should": [
                        {"term": {"company_domain.keyword": domain}},
                        {"term": {"company_domain": domain}},
                        {"match": {"company_domain": {"query": domain, "operator": "and"}}},
                    ],
                    "minimum_should_match": 1,
                }
            },
            "_source": ["id", "company_domain"],
        }
        try:
            try:
                resp = self._client.search(
                    index=self._index,
                    size=1,
                    query=body["query"],
                    source_includes=["id", "company_domain"],
                )
            except TypeError:
                resp = self._client.search(index=self._index, body=body)
        except Exception as e:
            logger.exception("Elasticsearch search failed: %s", e)
            return None

        hits = (resp.get("hits") or {}).get("hits") or []
        if not hits:
            logger.info("Elasticsearch: no hit for domain=%s", domain)
            return None
        src = hits[0].get("_source") or {}
        cid = src.get("id") or hits[0].get("_id")
        if cid is not None:
            cid = str(cid).strip()
        logger.info("Elasticsearch: hit domain=%s id=%s", domain, cid)
        return cid or None

    def index_company(self, document: Dict[str, Any], *, doc_id: str) -> None:
        """Index or replace a company document (used after Postgres upsert)."""
        try:
            self._client.index(index=self._index, id=doc_id, document=document)
            self._client.indices.refresh(index=self._index)
            logger.info("Elasticsearch: indexed id=%s", doc_id)
        except Exception as e:
            logger.exception("Elasticsearch index failed id=%s: %s", doc_id, e)
            raise
