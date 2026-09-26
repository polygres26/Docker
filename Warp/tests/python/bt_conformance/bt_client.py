"""Raw-gRPC Bigtable client for the conformance harness (the Google client libraries are not installed): stubs generated from the
vendored protos live in bt_stubs/ (a namespace-package root, no __init__.py, so it merges with the installed google.protobuf)."""
import os
import sys

STUBS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bt_stubs")
if STUBS not in sys.path:
    sys.path.insert(0, STUBS)

import grpc  # noqa: E402
from google.bigtable.admin.v2 import bigtable_table_admin_pb2 as adm, bigtable_table_admin_pb2_grpc as admg  # noqa: E402
from google.bigtable.v2 import bigtable_pb2 as bt, bigtable_pb2_grpc as btg, data_pb2 as dt  # noqa: E402
from google.protobuf import json_format  # noqa: E402

DATA = {"ReadRows": bt.ReadRowsRequest, "SampleRowKeys": bt.SampleRowKeysRequest, "MutateRow": bt.MutateRowRequest,
        "MutateRows": bt.MutateRowsRequest, "CheckAndMutateRow": bt.CheckAndMutateRowRequest,
        "ReadModifyWriteRow": bt.ReadModifyWriteRowRequest, "PingAndWarm": bt.PingAndWarmRequest}
STREAMING = {"ReadRows", "SampleRowKeys", "MutateRows"}
ADMIN = {"CreateTable": adm.CreateTableRequest, "GetTable": adm.GetTableRequest, "ListTables": adm.ListTablesRequest,
         "DeleteTable": adm.DeleteTableRequest, "ModifyColumnFamilies": adm.ModifyColumnFamiliesRequest,
         "DropRowRange": adm.DropRowRangeRequest, "GenerateConsistencyToken": adm.GenerateConsistencyTokenRequest,
         "CheckConsistency": adm.CheckConsistencyRequest, "UpdateTable": adm.UpdateTableRequest}


def dictify(m):
    return json_format.MessageToDict(m, preserving_proto_field_name=True)


class BtClient:
    def __init__(self, target):
        self.target = target
        self.channel = grpc.insecure_channel(target, options=[("grpc.max_receive_message_length", 64 * 1024 * 1024)])
        self.data = btg.BigtableStub(self.channel)
        self.admin = admg.BigtableTableAdminStub(self.channel)

    def close(self):
        self.channel.close()

    def call(self, rpc, req, timeout=30):
        """req: dict in proto-JSON form (bytes fields base64) or a message. Returns (code_name, details, body). Streaming RPCs
        return body = list of response dicts (chunks of ReadRows, entries of MutateRows, samples of SampleRowKeys)."""
        cls = DATA.get(rpc) or ADMIN[rpc]
        msg = req if not isinstance(req, dict) else json_format.ParseDict(req, cls())
        stub = self.data if rpc in DATA else self.admin
        f = getattr(stub, rpc)
        try:
            if rpc in STREAMING:
                return "OK", "", [dictify(r) for r in f(msg, timeout=timeout)]
            return "OK", "", dictify(f(msg, timeout=timeout))
        except grpc.RpcError as e:
            return e.code().name, e.details() or "", None
