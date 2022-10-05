#    A collection of tools to interface with various connectome backends.
#
#    Copyright (C) 2021 Philipp Schlegel
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.

from .base import MeshSource
from ..utils.catmaid import CatmaidClient
from cloudvolume import Mesh
import typing as tp
import meshio
from io import BytesIO


class CatmaidMeshSource(MeshSource):
    def __init__(self, client: CatmaidClient, project_id: int):
        self.client = client
        self.project_id = project_id

    def get(self, ids: list[int]) -> tp.Iterable[Mesh]:
        """Fetch meshes.

        Parameters
        ----------
        ids:
            CATMAID volume IDs to fetch.
        """
        fmt = "stl"
        url_params = [
            (f"{self.project_id}/volumes/{vid}/export.{fmt}", None)
            for vid in ids
        ]
        for response in self.client.get_many(url_params, headers={"Accept": "model/stl"}):
            buf = BytesIO(response.content)
            mesh = meshio.stl._stl._read_ascii(buf)
            yield Mesh(
                mesh.points.astype("float32"),
                mesh.cells_dict["triangle"].astype("uint32")
            )
