from ..utils.catmaid import CatmaidClient
from ..utils.misc import chunk
from cloudvolume import Skeleton
from .base import SkeletonSource
import numpy as np
import typing as tp


BATCH = 50

class CatmaidSkeleton(Skeleton):
    def __init__(self, vertices=None, edges=None, radii=None, vertex_types=None, segid=None, transform=None, space='voxel', extra_attributes=None, skeleton_id=None, vertex_ids=None,):
        super().__init__(vertices, edges, radii, vertex_types, segid, transform, space, extra_attributes)
        self.skeleton_id = skeleton_id
        self.vertex_ids = None
        if vertex_ids is not None:
            self.vertex_ids = np.asarray(vertex_ids, "uint64")

    @classmethod
    def from_detail(cls, detail_response, skeleton_id=None):
        builder = CatmaidSkeletonBuilder(skeleton_id)
        for row in detail_response[0]:
            builder.append(row)
        return builder.build()


class CatmaidSkeletonBuilder:
    # todo: vertex types - soma from root? could also use tag
    # todo: vertex types - in/out connectors?
    def __init__(self, skeleton_id=None) -> None:
        self.tnid_to_vertex_idx: dict[int, int] = dict()
        self.edges: list[tuple[int, int]] = list()
        self.vertices: list[tuple[int, int, int]] = list()
        self.radii: list[int] = list()
        self.vertex_ids: list[int] = list()
        self.skeleton_id = None
        if skeleton_id is not None:
            self.skeleton_id = np.uint64(skeleton_id)

    def append(self, detail_row):
        """tnid, parent_id, user_id, x, y, z, r, confidence"""
        tnid, parent_id, _, x, y, z, r, _ = detail_row
        self.tnid_to_vertex_idx[tnid] = len(self.vertices)
        self.vertex_ids.append(tnid)
        self.vertices.append((x, y, z))
        self.radii.append(r)
        if parent_id and parent_id >= 1:
            self.edges.append((tnid, parent_id))

    def build(self) -> Skeleton:
        return CatmaidSkeleton(
            np.asarray(self.vertices, "uint32"),
            np.asarray([
                [self.tnid_to_vertex_idx[tn] for tn in e]
                for e in self.edges
            ], "uint32"),
            np.asarray(self.radii, "float32"),
            space="physical",
            skeleton_id=self.skeleton_id,
            vertex_ids=np.asarray(self.vertex_ids, "uint64"),
        )


class CatmaidSkeletonSource(SkeletonSource):
    def __init__(self, client: CatmaidClient, project_id: int) -> None:
        self.client = client
        self.project_id = project_id

    def get(self, ids: list[int]) -> tp.Iterable[Skeleton]:
        url = f"{self.project_id}/skeletons/compact-detail"
        batch = min(BATCH, max(len(ids) // self.client.max_workers, 1))
        # TODO: msgpack
        chunked = [
            (url, {"skeleton_ids": c})
            # (url, {"skeleton_ids": c, "request_format": "msgpack"})
            for c in chunk(ids, batch)
        ]
        for response in self.client.post_many(chunked):
            for _, details in response.json()["skeletons"].items():
                yield CatmaidSkeleton.from_detail(details)
