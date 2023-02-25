import json

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
emitter: DatahubRestEmitter = DatahubRestEmitter(gms_server="http://localhost:8080", token='eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMiIsImp0aSI6ImQ1NmFjNjM0LTk4ZDktNDMxOC04MjAyLTlhODAzMmE1OWJlMiIsInN1YiI6ImRhdGFodWIiLCJpc3MiOiJkYXRhaHViLW1ldGFkYXRhLXNlcnZpY2UifQ.SJNyRlMJR3Nkch-ZGsXWGYFkolTM1sCDhUibOxsdZ44')

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
