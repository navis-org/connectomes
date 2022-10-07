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

import numpy as np

from concurrent.futures import ThreadPoolExecutor, as_completed

from .base import MeshSource
from ..utils.flywire import parse_volume, is_iterable

class FlywireMeshSource(MeshSource):
    def __init__(self, dataset='production'):
        self.dataset = dataset

    def get(self, x, threads=2, omit_failures=False, **kwargs):
        """Fetch meshes for given neuron id.

        Parameters
        ----------
        x :             int | list
                        Defines which meshes to fetch. Can be:
                          - a body ID (integers)
                          - lists of body IDs
        threads :       bool | int, optional
                        Whether to use threads to fetch meshes in parallel.
        omit_failures : bool, optional
                        Determine behaviour when mesh download
                        fails.
        """
        vol = parse_volume(self.dataset)
        if is_iterable(x):
            x = np.asarray(x, dtype=np.int64)
            if not threads or threads == 1:
                return [self.get(id_, **kwargs) for id_ in x]
            else:
                if not isinstance(threads, int):
                    raise TypeError(f'`threads` must be int or `None`, got "{type(threads)}".')
            with ThreadPoolExecutor(max_workers=threads) as executor:
                futures = {executor.submit(self.get, n,
                                           omit_failures=omit_failures,
                                           threads=None): n for n in x}

                results = []
                for f in as_completed(futures):
                    results.append(f.result())
            return results
        x = np.int64(x)
        mesh = vol.mesh.get(x, remove_duplicate_vertices=True)[x]
        return mesh
