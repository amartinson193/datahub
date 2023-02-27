import json
import os 
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    GenericAspectClass,
    MetadataChangeProposalClass,
)

dq_aspect = {
    "distinct_key": [
        {
            "column": "test_col1",
            "distinctValues": str(["hi","bye","check","hello"])
        },
    ]
}

datahub_token = os.getenv("DATAHUB_TOKEN")
emitter: DatahubRestEmitter = DatahubRestEmitter(gms_server="http://localhost:8080", token=datahub_token)

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)"
mcp_raw: MetadataChangeProposalClass = MetadataChangeProposalClass(
    entityType="dataset",
    entityUrn=dataset_urn,
    changeType=ChangeTypeClass.UPSERT,
    aspectName="distinctValues",
    aspect=GenericAspectClass(
        contentType="application/json",
        value=json.dumps(dq_aspect).encode("utf-8"),
    ),
)

try:
    emitter.emit(mcp_raw)
    print("Successfully wrote to DataHub")
except Exception as e:
    print("Failed to write to DataHub")
    raise e
