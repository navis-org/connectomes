from ..utils.catmaid import CatmaidClient
from ..utils.misc import DataFrameBuilder
from .base import ConnectivitySource
import pandas as pd


class CatmaidConnectivitySource(ConnectivitySource):
    def __init__(self, client: CatmaidClient, project_id: int) -> None:
        self.client = client
        self.project_id = project_id

    def get_edges(self, source_skeleton_ids: list[int], target_skeleton_ids: list[int]) -> pd.DataFrame:
        data = {
            "rows": source_skeleton_ids,
            "columns": target_skeleton_ids,
        }
        url = f"{self.project_id}/skeleton/connectivity_matrix"
        response = self.client.request("POST", url, data=data)
        builder = DataFrameBuilder(
            ["source_skeleton_id", "target_skeleton_id", "count"],
            ["uint64", "uint64", "uint32"],
        )
        for source, tgt_dict in response.json().items():
            for target, count in tgt_dict.items():
                builder.append_row([int(source), int(target), int(count)])

        return builder.build()

    def get_partners(self, skeleton_ids: list[int]):
        builder = DataFrameBuilder(
            ["skeleton_id", "partner_id", "count", "is_outgoing"],
            ["uint64", "uint64", "uint32", "bool"],
        )
        data = {
            "source_skeleton_ids": skeleton_ids,
            "boolean_op": "OR",
        }
        url = f"{self.project_id}/skeletons/connectivity"
        response = self.client.request("POST", url, data=data).json()
        for src_skid_str, d in response["outgoing"].items():
            src_skid = int(src_skid_str)
            for partner_skid_str, hist in d["skids"].items():
                partner_skid = int(partner_skid_str)
                total = sum(hist)
                builder.append_row([src_skid, partner_skid, total, True])

        for src_skid_str, d in response["incoming"].items():
            src_skid = int(src_skid_str)
            for partner_skid_str, hist in d["skids"].items():
                partner_skid = int(partner_skid_str)
                total = sum(hist)
                builder.append_row([src_skid, partner_skid, total, False])

        return builder.build()

    def get_synapses(self, skeleton_ids: list[int]) -> pd.DataFrame:
        builder = DataFrameBuilder(
            ["skeleton_id", "x", "y", "z", "is_outgoing"],
            ["uint64", "float64", "float64", "float64", "bool"],
        )
        url = f"{self.project_id}/connectors/links/"
        # {"links": [[skeleton_id, connector_id, x, y, z, confidence, user_id, treenode_id, creation_time, edition_time], ...], "tags": {}}
        data = {"skeleton_ids": skeleton_ids}
        url_data = [
            (url, {"relation_type": "presynaptic_to", **data}),
            (url, {"relation_type": "postsynaptic_to", **data}),
        ]
        is_outgoing = True
        for response in self.client.post_many(url_data):
            for skid, _, x, y, z, *_ in response.json()["links"]:
                builder.append_row([skid, x, y, z, is_outgoing])
            is_outgoing = False
        return builder.build()
