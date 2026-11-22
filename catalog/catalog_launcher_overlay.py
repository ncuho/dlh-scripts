"""OpenMetadata MCP launcher (streamable-http).

Obtains a fresh OpenMetadata JWT at startup (via login when username/password are
provided) and keeps it fresh with a background refresher, so the MCP server does not
stop working after the ~1h token expiry.
"""

import base64
import json
import logging
import os
import threading
import time
import urllib.request

from src.openmetadata.openmetadata_client import initialize_client, get_client
from src.server import app
from src.main import APITYPE_TO_FUNCTIONS, APIType

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("openmetadata-mcp")

HOST = (os.getenv("OPENMETADATA_HOST", "") or "").rstrip("/")
JWT = os.getenv("OPENMETADATA_JWT_TOKEN")
USERNAME = os.getenv("OPENMETADATA_USERNAME")
PASSWORD = os.getenv("OPENMETADATA_PASSWORD")


def _login():
    """Exchange username/password for an OpenMetadata JWT (password is base64-encoded)."""
    pwd_b64 = base64.b64encode(PASSWORD.encode()).decode()
    req = urllib.request.Request(
        HOST + "/api/v1/users/login",
        data=json.dumps({"email": USERNAME, "password": pwd_b64}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _resolve_token():
    if JWT:
        return JWT, False  # static token, no refresh
    if USERNAME and PASSWORD:
        return _login()["accessToken"], True
    raise ValueError(
        "Either OPENMETADATA_JWT_TOKEN or OPENMETADATA_USERNAME+PASSWORD must be set"
    )


api_token, can_refresh = _resolve_token()
initialize_client(host=HOST, api_token=api_token, username=USERNAME, password=PASSWORD)
client = get_client()


def _refresher():
    while True:
        time.sleep(1800)  # 30 min; OM user tokens live ~1h
        try:
            new_token = _login()["accessToken"]
            client.session.headers["Authorization"] = "Bearer " + new_token
            log.info("OpenMetadata JWT refreshed")
        except Exception as exc:  # noqa: BLE001
            log.warning("OpenMetadata JWT refresh failed: %s", exc)


if can_refresh:
    threading.Thread(target=_refresher, daemon=True).start()

apis = [api.value for api in APIType]
registered_count = 0
for api in apis:
    get_function = APITYPE_TO_FUNCTIONS[APIType(api)]
    try:
        functions = get_function()
    except NotImplementedError:
        log.warning("API type '%s' not implemented", api)
        continue
    for func, name, description, *_ in functions:
        app.add_tool(func, name=name, description=description)
        registered_count += 1

log.info("Total registered tools: %d", registered_count)

# ---- Ontology tools (glossary terms = ontology concepts) ----
# OpenMetadata 2.x models the ontology on the GlossaryTerm entity: fields `iri`,
# `conceptType`, `conceptMappings`, `attributes`, `relatedTerms` (typed relations),
# `references`, `realizedIn`. These wrap /api/v1/glossaryTerms so the agent can both
# form (create/update) and parse (read) the ontology.


def _om(method, endpoint, params=None, json_data=None):
    headers = (
        {"Content-Type": "application/json-patch+json"} if method == "PATCH" else None
    )
    return client._make_request(
        method, endpoint, params=params, json_data=json_data, headers=headers
    )


def _jd(obj):
    return json.dumps(obj, ensure_ascii=False, default=str)


def create_glossary_term(
    glossary_fqn: str,
    name: str,
    description: str = "",
    display_name: str = "",
    parent_fqn: str = "",
    synonyms: list = None,
    related_terms: list = None,
    references: list = None,
    concept_mappings: list = None,
) -> str:
    """Create a glossary term (an ontology concept) inside a glossary.

    glossary_fqn: FQN of the target glossary (e.g. "Business Glossary").
    name: unique term name within the glossary.
    related_terms: typed relations to other terms (relationType: broader/narrower/synonym/relatedTo).
    references: links to external glossary terms.
    synonyms: business synonyms of the term.
    concept_mappings: mappings to external concepts (SKOS alignments).
    """
    p = {"glossary": glossary_fqn, "name": name, "description": description}
    if display_name:
        p["displayName"] = display_name
    if parent_fqn:
        p["parent"] = parent_fqn
    if synonyms:
        p["synonyms"] = synonyms
    if related_terms:
        p["relatedTerms"] = related_terms
    if references:
        p["references"] = references
    if concept_mappings:
        p["conceptMappings"] = concept_mappings
    return _jd(_om("POST", "glossaryTerms", json_data=p))


def get_glossary_term_ontology(fqn: str) -> str:
    """Read a glossary term by FQN with its semantic relations (relatedTerms, references, synonyms, conceptMappings, owners)."""
    fields = "relatedTerms,references,synonyms,conceptMappings,owners,reviewers"
    return _jd(_om("GET", "glossaryTerms/name/" + fqn, params={"fields": fields}))


def list_glossary_terms_ontology(glossary_fqn: str, limit: int = 100) -> str:
    """List glossary terms of a glossary with their semantic relations."""
    fields = "relatedTerms,references,synonyms,conceptMappings,owners"
    return _jd(
        _om(
            "GET",
            "glossaryTerms",
            params={"glossary": glossary_fqn, "fields": fields, "limit": limit},
        )
    )


def _resolve_term_id(ref: str):
    """Resolve a glossary term FQN to its UUID."""
    if not ref:
        return None
    d = _om("GET", "glossaryTerms/name/" + ref, params={"fields": "id"})
    return d.get("id") if isinstance(d, dict) else None


def set_term_relations(fqn: str, related_terms: list) -> str:
    """Set typed relations between a term and other terms.

    related_terms: [{"name": "<other term FQN>", "relationType": "broader|narrower|synonym|relatedTo"} or {"id": "<uuid>", "relationType": "..."}].
    """
    value = []
    for rt in related_terms or []:
        if not isinstance(rt, dict):
            continue
        rel_type = rt.get("relationType", "relatedTo")
        if isinstance(rt.get("term"), dict):
            value.append(rt)
        elif rt.get("name"):
            tid = _resolve_term_id(rt["name"])
            if tid:
                value.append(
                    {
                        "relationType": rel_type,
                        "term": {"id": tid, "type": "glossaryTerm"},
                    }
                )
        elif rt.get("id"):
            value.append(
                {
                    "relationType": rel_type,
                    "term": {"id": rt["id"], "type": "glossaryTerm"},
                }
            )
    return _jd(
        _om(
            "PATCH",
            "glossaryTerms/name/" + fqn,
            json_data=[{"op": "add", "path": "/relatedTerms", "value": value}],
        )
    )


def update_glossary_term(
    fqn: str,
    description: str = "",
    concept_mappings: list = None,
    references: list = None,
    synonyms: list = None,
) -> str:
    """Update semantic fields of a glossary term by FQN (JSON Patch)."""
    patch = []
    if description:
        patch.append({"op": "add", "path": "/description", "value": description})
    if concept_mappings is not None:
        patch.append(
            {"op": "add", "path": "/conceptMappings", "value": concept_mappings}
        )
    if references is not None:
        patch.append({"op": "add", "path": "/references", "value": references})
    if synonyms is not None:
        patch.append({"op": "add", "path": "/synonyms", "value": synonyms})
    return _jd(_om("PATCH", "glossaryTerms/name/" + fqn, json_data=patch))


app.add_tool(
    create_glossary_term,
    name="create_glossary_term",
    description="Create a glossary term (ontology concept) with relations/references/synonyms/conceptMappings.",
)
app.add_tool(
    get_glossary_term_ontology,
    name="get_glossary_term_ontology",
    description="Read a glossary term by FQN with semantic relations (relatedTerms, references, synonyms, conceptMappings).",
)
app.add_tool(
    list_glossary_terms_ontology,
    name="list_glossary_terms_ontology",
    description="List glossary terms of a glossary with semantic relations.",
)
app.add_tool(
    set_term_relations,
    name="set_term_relations",
    description="Set typed relations (broader/narrower/synonym/relatedTo) between glossary terms.",
)
app.add_tool(
    update_glossary_term,
    name="update_glossary_term",
    description="Update semantic fields (description/conceptMappings/references/synonyms) of a glossary term by FQN.",
)

# Bind 0.0.0.0 so the pod is reachable; host/port configurable via env.
# stateless_http + json_response: match the Trino MCP server (avoids 421 session errors
# for the AI Space skill client, which uses simple request/response without SSE sessions).
app.settings.stateless_http = True
app.settings.json_response = True
# Allow any Host header: we sit behind HAProxy (which does IP filtering), and the
# MCP SDK's default DNS-rebinding protection only whitelists localhost — that would
# 421 every request coming via the kube-vip VIP / HAProxy hostname.
app.settings.transport_security.enable_dns_rebinding_protection = False
host = os.getenv("MCP_HOST", "0.0.0.0")
port = int(os.getenv("MCP_PORT", "8000"))
app.settings.host = host
app.settings.port = port
# Scoped extension: bounded typed lineage deletion using native OM endpoint.
from uuid import UUID


def delete_lineage_by_id(from_type: str, from_id: str, to_type: str, to_id: str):
    allowed = {"table", "chart", "dashboard", "pipeline", "topic"}
    if from_type not in allowed or to_type not in allowed:
        raise ValueError("unsupported lineage entity type")
    source = str(UUID(from_id))
    target = str(UUID(to_id))
    if from_type != "table" or to_type != "chart":
        raise ValueError("only table to chart removal is supported")
    graph = _om(
        "GET",
        "lineage/" + from_type + "/" + source,
        params={"upstreamDepth": 0, "downstreamDepth": 1},
    )
    if not any(
        e.get("fromEntity") == source and e.get("toEntity") == target
        for e in graph.get("downstreamEdges", [])
    ):
        return _jd({"status": "absent"})
    _om("DELETE", "lineage/" + from_type + "/" + source + "/" + to_type + "/" + target)
    graph = _om(
        "GET",
        "lineage/" + from_type + "/" + source,
        params={"upstreamDepth": 0, "downstreamDepth": 1},
    )
    if any(
        e.get("fromEntity") == source and e.get("toEntity") == target
        for e in graph.get("downstreamEdges", [])
    ):
        raise RuntimeError("lineage delete verification failed")
    return _jd({"status": "deleted"})


app.add_tool(
    delete_lineage_by_id,
    name="delete_lineage_by_id",
    description="Remove one verified table-to-chart lineage edge by UUID; idempotent.",
)


# Scoped status description update for the performance dashboard and its charts.
def set_demo_bi_description(entity_type: str, entity_id: str, description: str):
    allowed = {
        "dashboard": {"webinar_superset.dashboard_6"},
        "chart": {"webinar_superset.chart_" + str(n) for n in range(19, 23)},
    }
    ident = str(UUID(entity_id))
    if entity_type not in allowed:
        raise ValueError("unsupported BI entity type")
    endpoint = ("dashboards/" if entity_type == "dashboard" else "charts/") + ident
    current = _om("GET", endpoint)
    if current.get("fullyQualifiedName") not in allowed[entity_type]:
        raise ValueError("unsupported BI entity")
    if not isinstance(description, str) or not 1 <= len(description) <= 1000:
        raise ValueError("invalid description")
    endpoint = ("dashboards/" if entity_type == "dashboard" else "charts/") + ident
    _om(
        "PATCH",
        endpoint,
        json_data=[{"op": "add", "path": "/description", "value": description}],
    )
    result = _om("GET", endpoint)
    if result.get("description") != description:
        raise RuntimeError("description readback failed")
    return _jd({"status": "updated"})


app.add_tool(
    set_demo_bi_description,
    name="set_demo_bi_description",
    description="Update verified performance dashboard/chart description by scoped UUID and FQN.",
)

app.run(transport="streamable-http")
