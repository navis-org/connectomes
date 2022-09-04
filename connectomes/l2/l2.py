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

"""Functions to interact with the L2 graphs and L2 cache.

Heavily borrows from code from Casey Schneider-Mizell's "pcg_skel"
(https://github.com/AllenInstitute/pcg_skel).

"""

import navis
import fastremap

import networkx as nx
import numpy as np
import pandas as pd
import skeletor as sk
import trimesh as tm

from cloudvolume import CloudVolume
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from urllib.parse import urlparse

from .utils import (get_cave_client, retry_on_fail, chunks_to_nm,
                    get_chunkedgraph_secret)


class L2Cache:
    """Function to interact with/use the L2 cache for ChunkedGraph backends."""

    def __init__(self, url, datastack):
        self.datastack = datastack
        self.url = url

        self._api_token = None
        self._cave_client = None
        self._volume = None

    @property
    def api_token(self):
        """The CloudVolume API Token."""
        return get_chunkedgraph_secret(self.domain)

    @property
    def cave_client(self):
        """The CAVE client (lazily loaded)."""
        if not self._cave_client:
            self._cache_client = get_cave_client(self.datastack,
                                                 token=self.api_token)
        return self._cache_client

    @property
    def cloudvolume(self):
        """The cloudvolume (lazily loaded)."""
        # Lazy initialization
        if not self._volume:
            self._volume = CloudVolume('graphene://' + self.url, use_https=True)
        return self._volume

    @property
    def domain(self):
        return urlparse(self.url).netloc

    def get_info(self, root_ids, progress=True, max_threads=4):
        """Fetch basic info for given neuron(s) using the L2 cache.

        Parameters
        ----------
        root_ids  :         int | list of ints
                            FlyWire root ID(s) for which to fetch L2 infos.
        progress :          bool
                            Whether to show a progress bar.
        max_threads :       int
                            Number of parallel requests to make.

        Returns
        -------
        pandas.DataFrame
                            DataFrame with basic info (also see Examples):
                              - `length_um` is the sum of the max diameter across
                                all L2 chunks
                              - `bounds_nm` is a very rough bounding box based on the
                                representative coordinates of the L2 chunks

        """
        if navis.utils.is_iterable(root_ids):
            root_ids = np.unique(root_ids)
            info = []
            with ThreadPoolExecutor(max_workers=max_threads) as pool:
                func = retry_on_fail(partial(self.get_info))
                futures = pool.map(func, root_ids)
                info = [f for f in navis.config.tqdm(futures,
                                                     desc='Fetching L2 info',
                                                     total=len(root_ids),
                                                     disable=not progress or len(root_ids) == 1,
                                                     leave=False)]
            return pd.concat(info, axis=0)

        # Get/Initialize the CAVE client
        client = self.cave_client

        l2_ids = client.chunkedgraph.get_leaves(root_ids, stop_layer=2)

        attributes = ['area_nm2', 'size_nm3', 'max_dt_nm', 'rep_coord_nm']
        info = client.l2cache.get_l2data(l2_ids.tolist(), attributes=attributes)
        n_miss = len([v for v in info.values() if not v])

        row = [root_ids, len(l2_ids), n_miss]
        info_df = pd.DataFrame([row],
                               columns=['root_id', 'l2_chunks', 'chunks_missing'])

        # Collect L2 attributes
        for at in attributes:
            if at in ('rep_coord_nm'):
                continue

            summed = sum([v.get(at, 0) for v in info.values()])
            if at.endswith('3'):
                summed /= 1000**3
            elif at.endswith('2'):
                summed /= 1000**2
            else:
                summed /= 1000

            info_df[at.replace('_nm', '_um')] = [summed]

        # Check bounding box
        pts = np.array([v['rep_coord_nm'] for v in info.values() if v])

        if len(pts) > 1:
            bounds = [v for l in zip(pts.min(axis=0), pts.max(axis=0)) for v in l]
        elif len(pts) == 1:
            pt = pts[0]
            rad = [v['max_dt_nm'] for v in info.values() if v][0] / 2
            bounds = [pt[0] - rad, pt[0] + rad,
                      pt[1] - rad, pt[1] + rad,
                      pt[2] - rad, pt[2] + rad]
            bounds = [int(co) for co in bounds]
        else:
            bounds = None
        info_df['bounds_nm'] = [bounds]

        info_df.rename({'max_dt_um': 'length_um'},
                       axis=1, inplace=True)

        return info_df

    def get_graph(self, root_ids, progress=True):
        """Fetch L2 graph(s).

        Parameters
        ----------
        root_ids  :         int | list of ints
                            FlyWire root ID(s) for which to fetch the L2 graphs.
        progress :          bool
                            Whether to show a progress bar.

        Returns
        -------
        networkx.Graph
                            The L2 graph.

        """
        if navis.utils.is_iterable(root_ids):
            graphs = []
            for id in navis.config.tqdm(root_ids, desc='Fetching',
                                        disable=not progress, leave=False):
                n = self.get_graph(id)
                graphs.append(n)
            return graphs

        # Get/Initialize the CAVE client
        client = self.cave_client

        # Load the L2 graph for given root ID
        # This is a (N,2) array of edges
        l2_eg = np.array(client.chunkedgraph.level2_chunk_graph(root_ids))

        # Drop duplicate edges
        l2_eg = np.unique(np.sort(l2_eg, axis=1), axis=0)

        G = nx.Graph()
        G.add_edges_from(l2_eg)

        return G

    def get_skeleton(self, root_id, refine=True, drop_missing=True,
                     progress=True, **kwargs):
        """Generate skeleton from L2 graph.

        Parameters
        ----------
        root_id  :          int | list of ints
                            Root ID(s) of the FlyWire neuron(s) you want to
                            skeletonize.
        refine :            bool
                            If True, will refine skeleton nodes by moving them in
                            the center of their corresponding chunk meshes.
        drop_missing :      bool
                            Only relevant if ``refine=True``: if True, will drop
                            nodes that don't have a corresponding chunk mesh. These
                            are typically chunks that are either very small or very
                            new.
        progress :          bool
                            Whether to show a progress bar.
        **kwargs
                            Keyword arguments are passed through to Dotprops
                            initialization. Use to e.g. set extra properties.

        Returns
        -------
        skeleton :          navis.TreeNeuron
                            The extracted skeleton.

        """
        # TODO:
        # - drop duplicate nodes in unrefined skeleton
        # - use L2 graph to find soma: highest degree is typically the soma

        if navis.utils.is_iterable(root_id):
            nl = []
            for id in navis.config.tqdm(root_id, desc='Skeletonizing',
                                        disable=not progress, leave=False):
                n = self.get_skeleton(id, refine=refine, drop_missing=drop_missing,
                                      progress=progress, **kwargs)
                nl.append(n)
            return navis.NeuronList(nl)

        # Get the cloudvolume
        vol = self.cloudvolume

        # Get/Initialize the CAVE client
        client = self.cave_client

        # Load the L2 graph for given root ID
        # This is a (N,2) array of edges
        l2_eg = np.array(client.chunkedgraph.level2_chunk_graph(root_id))

        # Drop duplicate edges
        l2_eg = np.unique(np.sort(l2_eg, axis=1), axis=0)

        # Unique L2 IDs
        l2_ids = np.unique(l2_eg)

        # ID to index
        l2dict = {l2: ii for ii, l2 in enumerate(l2_ids)}

        # Remap edge graph to indices
        eg_arr_rm = fastremap.remap(l2_eg, l2dict)

        coords = [np.array(vol.mesh.meta.meta.decode_chunk_position(l)) for l in l2_ids]
        coords = np.vstack(coords)

        # This turns the graph into a hierarchal tree by removing cycles and
        # ensuring all edges point towards a root
        if sk.__version_vector__[0] < 1:
            G = sk.skeletonizers.edges_to_graph(eg_arr_rm)
            swc = sk.skeletonizers.make_swc(G, coords=coords)
        else:
            G = sk.skeletonize.utils.edges_to_graph(eg_arr_rm)
            swc = sk.skeletonize.utils.make_swc(G, coords=coords, reindex=False)

        # Set radius to 0
        swc['radius'] = 0

        # Convert to Euclidian space
        # Dimension of a single chunk
        ch_dims = chunks_to_nm([1, 1, 1], vol) - chunks_to_nm([0, 0, 0], vol)
        ch_dims = np.squeeze(ch_dims)

        xyz = swc[['x', 'y', 'z']].values
        swc[['x', 'y', 'z']] = chunks_to_nm(xyz, vol) + ch_dims / 2

        if refine:
            # Get the L2 representative coordinates
            l2_info = client.l2cache.get_l2data(l2_ids.tolist(), attributes=['rep_coord_nm'])
            # Missing L2 chunks will be {'id': {}}
            new_co = {l2dict[int(k)]: v['rep_coord_nm'] for k, v in l2_info.items() if v}

            # Map refined coordinates onto the SWC
            has_new = swc.node_id.isin(new_co)
            swc.loc[has_new, 'x'] = swc.loc[has_new, 'node_id'].map(lambda x: new_co[x][0])
            swc.loc[has_new, 'y'] = swc.loc[has_new, 'node_id'].map(lambda x: new_co[x][1])
            swc.loc[has_new, 'z'] = swc.loc[has_new, 'node_id'].map(lambda x: new_co[x][2])

            # Turn into a proper neuron
            tn = navis.TreeNeuron(swc, id=root_id, units='1 nm', **kwargs)

            # Drop nodes that are still at their unrefined chunk position
            if drop_missing:
                tn = navis.remove_nodes(tn, swc.loc[~has_new, 'node_id'].values)
        else:
            tn = navis.TreeNeuron(swc, id=root_id, units='1 nm', **kwargs)

        return tn

    def dotprops(self, root_ids, min_size=None, progress=True, max_threads=10,
                 **kwargs):
        """Generate dotprops from L2 chunks.

        Parameters
        ----------
        root_ids  :         int | list of ints
                            Root ID(s) of the FlyWire neuron(s) you want to
                            dotprops for.
        min_size :          int, optional
                            Minimum size (in nm^3) for the L2 chunks. Smaller chunks
                            will be ignored. This is useful to de-emphasise the
                            finer terminal neurites which typically break into more,
                            smaller chunks and are hence overrepresented. A good
                            value appears to be around 1,000,000.
        progress :          bool
                            Whether to show a progress bar.
        max_threads :       int
                            Number of parallel requests to make when fetching the
                            L2 IDs (but not the L2 info).
        **kwargs
                            Keyword arguments are passed through to Dotprops
                            initialization. Use to e.g. set extra properties.

        Returns
        -------
        dps :               navis.NeuronList
                            List of Dotprops.

        """
        if not navis.utils.is_iterable(root_ids):
            root_ids = [root_ids]

        # Get/Initialize the CAVE client
        client = self.cave_client

        # Load the L2 IDs
        # Note that we are using the L2 graph endpoint as I have not yet found a
        # faster way to query the IDs.
        with ThreadPoolExecutor(max_workers=max_threads) as pool:
            futures = pool.map(client.chunkedgraph.level2_chunk_graph, root_ids)
            l2_eg = [f for f in navis.config.tqdm(futures,
                                                  desc='Fetching L2 IDs',
                                                  total=len(root_ids),
                                                  disable=not progress or len(root_ids) == 1,
                                                  leave=False)]

        # Unique L2 IDs per root ID
        l2_ids = [np.unique(g).astype(str) for g in l2_eg]

        # Flatten into a list of all L2 IDs
        l2_ids_all = np.unique([i for l in l2_ids for i in l])

        # Get the L2 representative coordinates, vectors and (if required) volume
        chunk_size = 2000  # no. of L2 IDs per query (doesn't seem have big impact)
        attributes = ['rep_coord_nm', 'pca']
        if min_size:
            attributes.append('size_nm3')

        l2_info = {}
        with navis.config.tqdm(desc='Fetching vectors',
                               disable=not progress,
                               total=len(l2_ids_all),
                               leave=False) as pbar:
            func = retry_on_fail(client.l2cache.get_l2data)
            for chunk_ix in np.arange(0, len(l2_ids_all), chunk_size):
                chunk = l2_ids_all[chunk_ix: chunk_ix + chunk_size]
                l2_info.update(func(chunk.tolist(), attributes=attributes))
                pbar.update(len(chunk))

        # L2 chunks without info will show as empty dictionaries
        # Let's drop them to make our life easier
        l2_info_ids = [k for k, v in l2_info.items() if v]

        # Generate dotprops
        dps = []
        for root, ids in zip(root_ids, l2_ids):
            # Find out for which IDs we have info
            ids = ids[np.isin(ids, l2_info_ids)]

            # Get xyz points and the first component of the PCA as vector
            pts = np.vstack([l2_info[i]['rep_coord_nm'] for i in ids])
            vec = np.vstack([l2_info[i]['pca'][0] for i in ids])

            # Apply min size filter if requested
            if min_size:
                sizes = np.array([l2_info[i]['size_nm3'] for i in ids])
                pts = pts[sizes >= min_size]
                vec = vec[sizes >= min_size]

            # Generate the actual dotprops
            dps.append(navis.Dotprops(points=pts, vect=vec, id=root, k=None,
                                      units='1 nm', **kwargs))

        return navis.NeuronList(dps)

    def _get_centroids(self, l2_ids, vol, threads=10, progress=True):
        """Fetch L2 meshes and compute centroid."""
        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures = [pool.submit(vol.mesh.get, i,
                                   allow_missing=True,
                                   deduplicate_chunk_boundaries=False) for i in l2_ids]

            res = [f.result() for f in navis.config.tqdm(futures,
                                                         disable=not progress,
                                                         leave=False,
                                                         desc='Loading meshes')]

        # Unpack results
        meshes = {k: v for d in res for k, v in d.items()}

        # For each mesh find the center of mass and move the corresponding point
        centroids = {}
        for k, m in meshes.items():
            m = tm.Trimesh(m.vertices, m.faces)
            # Do NOT use center_mass here -> garbage if not non-watertight
            centroids[k] = m.centroid

        return centroids
