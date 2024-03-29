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

import navis
import requests
import textwrap

import datetime as dt
import numpy as np
import pandas as pd

from cloudvolume import CloudVolume
from concurrent import futures
from diskcache import Cache
from requests_futures.sessions import FuturesSession
from scipy import ndimage
from urllib.parse import urlparse

from .. import utils

from .base import SegmentationSource



class ChunkedGraphDataSet(SegmentationSource):
    """A dataset with a chunkedgraph backend.

    Parameters
    ----------
    url :       str
                URL to the data set.

    """
    def __init__(self, url):
        if url.startswith('graphene://'):
            url = url[11:]

        while url.endswith('/'):
            url = url[:-1]

        self.url = url

        self._volume = None

    @property
    def api_token(self):
        """The CloudVolume API Token."""
        return utils.get_chunkedgraph_secret(self.domain)

    @property
    def cloudvolume(self):
        # Lazy initialization
        if not self._volume:
            self._volume = CloudVolume('graphene://' + self.url, use_https=True)
        return self._volume

    @property
    def domain(self):
        return urlparse(self.url).netloc

    def get_edit_history(self, x, progress=True, max_threads=4):
        """Fetch edit history for given neuron(s).

        Parameters
        ----------
        x :             int | list of int
                        Segmentation (root) ID(s).
        progress :      bool
                        If True, show progress bar.
        max_threads :   int
                        Max number of parallel requests to server.

        Returns
        -------
        pandas.DataFrame

        """
        if not isinstance(x, (list, set, np.ndarray)):
            x = [x]

        session = requests.Session()
        future_session = FuturesSession(session=session, max_workers=max_threads)

        token = utils.get_chunkedgraph_secret()
        session.headers['Authorization'] = f"Bearer {token}"

        futures = []
        for id in x:
            url = self.url + f'/root/{id}/tabular_change_log'
            f = future_session.get(url, params=None)
            futures.append(f)

        # Get the responses
        resp = [f.result() for f in navis.config.tqdm(futures,
                                                      desc='Fetching',
                                                      disable=not progress or len(futures) == 1,
                                                      leave=False)]

        df = []
        for r, i in zip(resp, x):
            r.raise_for_status()
            this_df = pd.DataFrame(r.json())
            this_df['segment'] = i
            df.append(this_df)

        # Concat if any edits at all
        if any([not f.empty for f in df]):
            # Drop neurons without edits
            df = [f for f in df if not f.empty]
            df = pd.concat(df, axis=0, sort=True)
            df['timestamp'] = pd.to_datetime(df.timestamp, unit='ms')
        else:
            # Return the first empty data frame
            df = df[0]

        return df

    def roots_to_supervoxels(self, x, use_cache=True, progress=True):
        """Get supervoxels making up given neurons.

        Parameters
        ----------
        x :             int | list of int
                        Segmentation (root) ID(s).
        use_cache :     bool
                        Whether to use disk cache to avoid repeated queries for the
                        same root. Cache is stored in `~/.fafbseg/`.
        progress :      bool
                        If True, show progress bar.

        Returns
        -------
        dict
                        ``{root_id: [svoxel_id1, svoxelid2, ...], ...}``

        """
        # Make sure we are working with an array of integers
        x = navis.utils.make_iterable(x).astype(int, copy=False)

        if len(x) <= 1:
            progress = False

        svoxels = {}
        # See what ewe can get from cache
        if use_cache:
            # Cache for root -> supervoxels
            # Grows to max 1Gb by default and persists across sessions
            with Cache(directory='~/.fafbseg/svoxel_cache/') as sv_cache:
                # See if we have any of these roots cached
                with sv_cache.transact():
                    is_cached = np.isin(x, sv_cache)

                # Add supervoxels from cache if we have any
                if np.any(is_cached):
                    # Get values from cache
                    with sv_cache.transact():
                        svoxels.update({i: sv_cache[i] for i in x[is_cached]})

        # Get the supervoxels for the roots that are still missing
        # We need to convert keys to integer array because otherwise there is a
        # mismatch in types (int vs np.int?) which causes all root IDs to be in miss
        # -> I think that's because of the way disk cache works
        vol = self.cloudvolume
        miss = x[~np.isin(x, np.array(list(svoxels.keys())).astype(int))]
        svoxels.update({i: vol.get_leaves(i,
                                          bbox=vol.meta.bounds(0),
                                          mip=0) for i in navis.config.tqdm(miss,
                                                               desc='Querying',
                                                               disable=not progress,
                                                               leave=False)})

        # Update cache
        if use_cache:
            with sv_cache.transact():
                for i in miss:
                    sv_cache[i] = svoxels[i]

        return svoxels

    def supervoxels_to_roots(self, x, use_cache=False):
        """Get root(s) for given supervoxel(s).

        Parameters
        ----------
        x :             int | list of int
                        Supervoxel ID(s) to find the root(s) for.
        use_cache :     bool
                        Whether to use disk cache to avoid repeated queries for the
                        same supervoxel. The implementation for this is optimized
                        for a small memory footprint - so it's slow! At this point
                        only use it if you want to minimize your impact on the
                        backend and you have time on your hands. The cache
                        is stored in `~/.fafbseg/`.
        progress :      bool
                        If True, show progress bar.

        Returns
        -------
        dict
                        ``{root_id: [svoxel_id1, svoxelid2, ...], ...}``

        """
        # Make sure we are working with an array of integers
        x = navis.utils.make_iterable(x).astype(np.int64, copy=False)

        # Prepare results array
        roots = np.zeros(x.shape, dtype=np.int64)

        # We can't query supervoxel ID 0
        not_zero = x != 0

        vol = self.cloudvolume
        if use_cache:
            # Cache for supervoxel -> root map
            # Grows to max 1Gb by default and persists across sessions
            with Cache(directory='~/.fafbseg/roots_cache/') as roots_cache:
                # See if we have any of these supervoxels cached
                with roots_cache.transact():
                    is_cached = np.isin(x, roots_cache)

                # For cached roots, check if they are latest
                if np.any(is_cached):
                    # Get values from cache
                    with roots_cache.transact():
                        cached = np.array([roots_cache[i] for i in x[is_cached]])
                    # Check if cached roots are latest
                    is_latest = self.is_latest_root(cached)
                    # Set roots that are still up-to-date
                    is_cached[is_cached] = is_latest
                    roots[is_cached] = cached[is_latest]

                # To fetch are those supervoxels that are != 0 and are not cached
                to_fetch = ~is_cached & not_zero

                # Fill in the blanks
                if np.any(to_fetch):
                    roots[to_fetch] = vol.get_roots(x[to_fetch])

                    # Update cache
                    with roots_cache.transact():
                        for k, v in zip(x[to_fetch], roots[to_fetch]):
                            roots_cache[k] = v

        else:
            # get_roots() doesn't like to be asked for zeros - causes server error
            roots[not_zero] = vol.get_roots(x[not_zero])

        return roots

    def locs_to_supervoxels(self, locs, mip=2, coordinates='voxel'):
        """Retrieve supervoxel IDs at given location(s).

        Parameters
        ----------
        locs :          list-like | pandas.DataFrame
                        Array of x/y/z coordinates. If DataFrame must contain
                        'x', 'y', 'z' or 'fw.x', 'fw.y', 'fw.z' columns. If both
                        present, 'fw.' columns take precedence!
        mip :           int [2-8]
                        Scale to query. Lower mip = more precise but slower;
                        higher mip = faster but less precise (small supervoxels
                        might not show at all).
        coordinates :   "voxel" | "nm"
                        Units in which your coordinates are in. "voxel" is assumed
                        to be 4x4x40 (x/y/z) nanometers.

        Returns
        -------
        numpy.array
                    List of segmentation IDs in the same order as ``locs``. Invalid
                    locations will be returned with ID 0.

        """
        if isinstance(locs, pd.DataFrame):
            if np.all(np.isin(['fw.x', 'fw.y', 'fw.z'], locs.columns)):
                locs = locs[['fw.x', 'fw.y', 'fw.z']].values
            elif np.all(np.isin(['x', 'y', 'z'], locs.columns)):
                locs = locs[['x', 'y', 'z']].values
            else:
                raise ValueError('`locs` as pandas.DataFrame must have either [fw.x'
                                 ', fw.y, fw.z] or [x, y, z] columns.')

            # Make sure we are working with numbers
            if not np.issubdtype(locs.dtype, np.number):
                locs = locs.astype(np.float64)

        # Create point loader
        loader = utils.PointLoader(self.cloudvolume)
        loader.add(locs)

        pts, svids = loader.load_all()

        return svids

    def neuron_to_segments(self, x, coordinates='voxel'):
        """Get root IDs overlapping with a given neuron.

        Parameters
        ----------
        x :                 Neuron/List
                            Neurons for which to return root IDs. Neurons must be
                            in flywire (FAFB14.1) space.
        coordinates :       "voxel" | "nm"
                            Units the neuron(s) are in. "voxel" is assumed to be
                            4x4x40 (x/y/z) nanometers.

        Returns
        -------
        overlap_matrix :    pandas.DataFrame
                            DataFrame of root IDs (rows) and IDs
                            (columns) with overlap in nodes as values::

                                     id     id1   id2
                                root_id
                                10336680915   5     0
                                10336682132   0     1

        """
        if isinstance(x, navis.TreeNeuron):
            x = navis.NeuronList(x)

        assert isinstance(x, navis.NeuronList)

        # We must not perform this on x.nodes as this is a temporary property
        nodes = x.nodes

        # Get segmentation IDs
        nodes['root_id'] = self.locs_to_segments(nodes[['x', 'y', 'z']].values,
                                                 coordinates=coordinates,
                                                 root_ids=True)

        # Count segment IDs
        seg_counts = nodes.groupby(['neuron', 'root_id'], as_index=False).node_id.count()
        seg_counts.columns = ['id', 'root_id', 'counts']

        # Remove seg IDs 0
        seg_counts = seg_counts[seg_counts.root_id != 0]

        # Turn into matrix where columns are skeleton IDs, segment IDs are rows
        # and values are the overlap counts
        matrix = seg_counts.pivot(index='root_id', columns='id', values='counts')

        return matrix

    def locs_to_segments(self, locs, root_ids=True, coordinates='voxel'):
        """Retrieve root IDs at given location(s).

        Parameters
        ----------
        locs :          list-like | pandas.DataFrame
                        Array of x/y/z coordinates. If DataFrame must contain
                        'x', 'y', 'z' or 'fw.x', 'fw.y', 'fw.z' columns. If both
                        present, 'fw.' columns take precedence)!
        root_ids :      bool
                        If True, will return root IDs. If False, will return supervoxel
                        IDs.
        coordinates :   "voxel" | "nm"
                        Units in which your coordinates are in. "voxel" is assumed
                        to be 4x4x40 (x/y/z) nanometers.

        Returns
        -------
        numpy.array
                    List of segmentation IDs in the same order as ``locs``.

        """
        svoxels = self.locs_to_supervoxels(locs, coordinates=coordinates)

        if not root_ids:
            return svoxels

        return self.supervoxels_to_roots(svoxels)

    def is_latest_root(self, id, **kwargs):
        """Check if root is the current one.

        Parameters
        ----------
        id :            int | list-like
                        Single ID or list of root IDs.

        Returns
        -------
        numpy array
                        Array of booleans

        See Also
        --------
        :func:`~fafbseg.flywire.update_ids`
                        If you want the new ID.

        """
        id = navis.utils.make_iterable(id).astype(str)

        session = requests.Session()
        token = utils.get_chunkedgraph_secret()
        session.headers['Authorization'] = f"Bearer {token}"

        url = self.url + '/is_latest_roots?int64_as_str=1'
        post = {'node_ids': id.tolist()}
        r = session.post(url, json=post)

        r.raise_for_status()

        return np.array(r.json()['is_latest'])

    def update_ids(self, id, sample=0.1, progress=True, **kwargs):
        """Retrieve the most recent version of given root ID(s).

        This function works by:
            1. Checking if ID is outdated (see also `is_latest_root`)
            2. Fetching all supervoxels for outdated IDs
            3. Picking a random sample of ``sample`` of these supervoxels
            4. Fetching the most recent root IDs for the sample supervoxels
            5. Returning the root ID that was hit the most.

        Parameters
        ----------
        id :            int | list-like
                        Single ID or list of root IDs.
        sample :        int | float
                        Number (>= 1) or fraction (< 1) of super voxels to sample
                        to guess the most recent version.
        progress :      bool
                        If True, shows progress bar.

        Returns
        -------
        pandas.DataFrame
                        Mapping of old -> new root IDs with confidence::

                          old_id   new_id   confidence   changed
                        0
                        1

        See Also
        --------
        :func:`~fafbseg.flywire.is_latest_root`
                        If all you want is to know whether a (root) ID is up-to-date.

        """
        assert sample > 0, '`sample` must be > 0'

        # See if we already check if this was the latest root
        is_latest = kwargs.pop('is_latest', None)

        if isinstance(id, (list, set, np.ndarray)):
            is_latest = self.is_latest_root(id)
            res = [self.update_ids(x, is_latest=il, sample=sample)
                                for x, il in navis.config.tqdm(zip(id, is_latest),
                                                               desc='Updating',
                                                               leave=False,
                                                               total=len(id),
                                                               disable=not progress or len(id) == 1)]
            return pd.concat(res, axis=0, sort=False, ignore_index=True)

        # Check if outdated
        if isinstance(is_latest, type(None)):
            is_latest = self.is_latest_root(id)[0]

        if not is_latest:
            # Get supervoxel ids - we need to use mip=0 because otherwise small neurons
            # might not have any (visible) supervoxels
            vol = self.cloudvolume
            svoxels = vol.get_leaves(id, bbox=vol.meta.bounds(0), mip=0)

            # Shuffle voxels
            np.random.shuffle(svoxels)

            # Generate sample
            if sample >= 1:
                smpl = svoxels[: sample]
            else:
                smpl = svoxels[: int(len(svoxels) * sample)]

            # Fetch up-to-date root IDs for the sampled supervoxels
            roots = self.supervoxels_to_roots(smpl)

            # Find unique Ids and count them
            unique, counts = np.unique(roots, return_counts=True)

            # Get sorted indices
            sort_ix = np.argsort(counts)

            # New Id is the most frequent ID
            new_id = unique[sort_ix[-1]]

            # Confidence is the difference between the top and the 2nd most frequent ID
            if len(unique) > 1:
                conf = round((counts[sort_ix[-1]] - counts[sort_ix[-2]]) / sum(counts),
                             2)
            else:
                conf = 1
        else:
            new_id = id
            conf = 1

        return pd.DataFrame([[id, new_id, conf, id != new_id]],
                            columns=['old_id', 'new_id', 'confidence', 'changed']
                            ).astype({'old_id': int, 'new_id': int})

    def snap_to_id(self, locs, id, snap_zero=False,  search_radius=160,
                   coordinates='nm', max_workers=4, verbose=True):
        """Snap locations to the correct segmentation ID.

        Works by:
         1. Fetch segmentation ID for each location and for those with the wrong ID:
         2. Fetch cube around each loc and snap to the closest voxel with correct ID

        Parameters
        ----------
        locs :          (N, 3) array
                        Array of x/y/z coordinates.
        id :            int
                        Expected ID at each location.
        snap_zero :     bool
                        If False (default), we will not snap locations that map to
                        segment ID 0 (i.e. no segmentation).
        search_radius : int
                        Radius [nm] around a location to search for a position with
                        the correct ID. Lower values will be faster.
        coordinates :   "voxel" | "nm"
                        Coordinate system of `locs`. If "voxel" it is assumed to be
                        4 x 4 x 40 nm.
        max_workers :   int
        verbose :       bool
                        If True will plot summary at then end.

        Returns
        -------
        (N, 3) array
                    x/y/z locations that are guaranteed to map to the correct ID.

        """
        assert coordinates in ['nm', 'nanometer', 'nanometers', 'voxel', 'voxels']

        if isinstance(locs, navis.TreeNeuron):
            locs = locs.nodes[['x', 'y', 'z']].values

        # This also makes sure we work on a copy
        locs = np.array(locs, copy=True)
        assert locs.ndim == 2 and locs.shape[1] == 3

        # From hereon out we are working with nanometers
        if coordinates in ('voxel', 'voxels'):
            locs *= [4, 4, 40]

        root_ids = self.locs_to_segments(locs, coordinates='nm')

        id_wrong = root_ids != id
        not_zero = root_ids != 0

        to_fix = id_wrong

        if not snap_zero:
            to_fix = to_fix & not_zero

        # Use parallel processes to go over the to-fix nodes
        with navis.config.tqdm(desc='Snapping', total=to_fix.sum(), leave=False) as pbar:
            with futures.ProcessPoolExecutor(max_workers=max_workers) as ex:
                loc_futures = [ex.submit(self._process_cutout,
                                         id=id,
                                         loc=locs[ix],
                                         radius=search_radius) for ix in np.where(to_fix)[0]]
                for f in futures.as_completed(loc_futures):
                    pbar.update(1)

        # Get results
        results = [f.result() for f in loc_futures]

        # Stack locations
        new_locs = np.vstack(results)

        # If no new location found, array will be [0, 0, 0]
        not_snapped = new_locs.max(axis=1) == 0

        # Update location
        to_update = np.where(to_fix)[0][~not_snapped]
        locs[to_update, :] = new_locs[~not_snapped]

        if verbose:
            msg = f"""\
            {to_fix.sum()} of {to_fix.shape[0]} locations needed to be snapped.
            Of these {not_snapped.sum()} locations could not be snapped - consider
            increasing `search_radius`.
            """
            print(textwrap.dedent(msg))

        return locs

    def _process_cutout(self, loc, id, radius=160):
        """Process single cutout for snap_to_id."""
        # Get this location
        loc = loc.round()

        # Generating bounding box around this location
        mn = loc - radius
        mx = loc + radius
        # Make sure it's a multiple of 4 and 40
        mn = mn - mn % [4, 4, 40]
        mx = mx - mx % [4, 4, 40]

        # Generate bounding box
        bbox = np.vstack((mn, mx))

        # Get the cutout, the resolution and offset
        cutout, res, offset_nm = self.get_segmentation_cutout(bbox,
                                                              root_ids=True,
                                                              coordinates='nm')

        # Generate a mask
        mask = (cutout == id).astype(int, copy=False)

        # Erode so we move our point slightly more inside the segmentation
        mask = ndimage.binary_erosion(mask).astype(mask.dtype)

        # Find positions the ID we are looking for
        our_id = np.vstack(np.where(mask)).T

        # Return [0, 0, 0] if unable to snap (i.e. if id not within radius)
        if not our_id.size:
            return np.array([0, 0, 0])

        # Get the closest on to the center of the cutout
        center = np.divide(cutout.shape, 2).round()
        dist = np.abs(our_id - center).sum(axis=1)
        closest = our_id[np.argmin(dist)]

        # Convert the cutout offset to absolute 4/4/40 voxel coordinates
        snapped = closest * res + offset_nm

        return snapped

    def get_segmentation_cutout(self, bbox, root_ids=True, coordinates='voxel'):
        """Fetch cutout of segmentation.

        Parameters
        ----------
        bbox :          array-like
                        Bounding box for the cutout::

                            [[xmin, xmax], [ymin, ymax], [zmin, zmax]]

        root_ids :      bool
                        If True, will return root IDs. If False, will return
                        supervoxel IDs.
        coordinates :   "voxel" | "nm"
                        Units in which your coordinates are in. "voxel" is assumed
                        to be 4x4x40 (x/y/z) nanometers.

        Returns
        -------
        cutout :        np.ndarry
                        (N, M) array of segmentation (root or supervoxel) IDs.
        resolution :    (3, ) numpy array
                        [x, y, z] resolution of voxel in cutout.
        nm_offset :     (3, ) numpy array
                        [x, y, z] offset in nanometers of the cutout with respect
                        to the absolute coordinates.

        """
        assert coordinates in ['nm', 'nanometer', 'nanometers', 'voxel', 'voxels']

        bbox = np.asarray(bbox)
        assert bbox.ndim == 2

        if bbox.shape == (2, 3):
            pass
        elif bbox.shape == (3, 2):
            bbox = bbox.T
        else:
            raise ValueError(f'`bbox` must have shape (2, 3) or (3, 2), got {bbox.shape}')

        # First convert to nanometers
        if coordinates in ('voxel', 'voxels'):
            bbox = bbox * [4, 4, 40]

        # Now convert (back to) to [16, 16, 40] voxel
        vol = self.cloudvolume
        bbox = (bbox / vol.scale['resolution']).round().astype(int)

        offset_nm = bbox[0] * vol.scale['resolution']

        # Get cutout
        cutout = vol[bbox[0][0]:bbox[1][0],
                     bbox[0][1]:bbox[1][1],
                     bbox[0][2]:bbox[1][2]]

        if root_ids:
            svoxels = np.unique(cutout.flatten())
            roots = self.supervoxels_to_roots(svoxels)

            sv2r = dict(zip(svoxels[svoxels != 0], roots[svoxels != 0]))

            for k, v in sv2r.items():
                cutout[cutout == k] = v

        return cutout[:, :, :, 0], np.asarray(vol.scale['resolution']), offset_nm


class FlyWireDataSet(ChunkedGraphDataSet):
    """FlyWire(-like) dataset."""

    def __init__(self, url, itanna_url='https://spine.janelia.org/app/transform-service'):
        super().__init__(url)
        self.seg_service = utils.SegmentationService(itanna_url)

    def get_leaderboard(self, days=7, by_day=False, progress=True, max_threads=4):
        """Fetch leader board (# of edits).

        Parameters
        ----------
        day :           int
                        Number of days to go back.
        by_day :        bool
                        If True, will provide a day-by-day breakdown of # edits.
        progress :      bool
                        If True, show progress bar.
        max_threads :   int
                        Max number of parallel requests to server.

        Returns
        -------
        pandas.DataFrame

        """
        assert isinstance(days, (int, np.int))
        assert days >= 0

        session = requests.Session()
        if not by_day:
            url = f'https://pyrdev.eyewire.org/flywire-leaderboard?days={days-1}'
            resp = session.get(url, params=None)
            resp.raise_for_status()
            return pd.DataFrame.from_records(resp.json()['entries']).set_index('name')

        future_session = FuturesSession(session=session, max_workers=max_threads)
        futures = []
        for i in range(0, days):
            url = f'https://pyrdev.eyewire.org/flywire-leaderboard?days={i}'
            futures.append(future_session.get(url, params=None))

        # Get the responses
        resp = [f.result() for f in navis.config.tqdm(futures,
                                                      desc='Fetching',
                                                      disable=not progress or len(futures) == 1,
                                                      leave=False)]

        df = None
        for i, r in enumerate(resp):
            date = dt.date.today() - dt.timedelta(days=i)
            r.raise_for_status()
            this_df = pd.DataFrame.from_records(r.json()['entries']).set_index('name')
            this_df.columns = [date]
            if isinstance(df, type(None)):
                df = this_df
            else:
                df = pd.merge(df, this_df, how='outer', left_index=True, right_index=True)

        # Make sure we don't have NAs
        df = df.fillna(0).astype(int)

        # This breaks it down into days
        if df.shape[1] > 1:
            df.iloc[:, 1:] = df.iloc[:, 1:].values - df.iloc[:, :-1].values

        # Reverse such that the right-most entry is the current date
        df = df.iloc[:, ::-1]
        return df.loc[df.sum(axis=1).sort_values(ascending=False).index]

    def locs_to_supervoxels(self, locs, mip=2, coordinates='voxel'):
        """Retrieve supervoxel IDs at given location(s).

        Uses a Service kindly hosted by Davi Bock and Eric Perlman.

        Parameters
        ----------
        locs :          list-like | pandas.DataFrame
                        Array of x/y/z coordinates. If DataFrame must contain
                        'x', 'y', 'z' or 'fw.x', 'fw.y', 'fw.z' columns. If both
                        present, 'fw.' columns take precedence!
        mip :           int [2-8]
                        Scale to query. Lower mip = more precise but slower;
                        higher mip = faster but less precise (small supervoxels
                        might not show at all).
        coordinates :   "voxel" | "nm"
                        Units in which your coordinates are in. "voxel" is assumed
                        to be 4x4x40 (x/y/z) nanometers.

        Returns
        -------
        numpy.array
                    List of segmentation IDs in the same order as ``locs``. Invalid
                    locations will be returned with ID 0.

        """
        if isinstance(locs, pd.DataFrame):
            if np.all(np.isin(['fw.x', 'fw.y', 'fw.z'], locs.columns)):
                locs = locs[['fw.x', 'fw.y', 'fw.z']].values
            elif np.all(np.isin(['x', 'y', 'z'], locs.columns)):
                locs = locs[['x', 'y', 'z']].values
            else:
                raise ValueError('`locs` as pandas.DataFrame must have either [fw.x'
                                 ', fw.y, fw.z] or [x, y, z] columns.')

            # Make sure we are working with numbers
            if not np.issubdtype(locs.dtype, np.number):
                locs = locs.astype(np.float64)

        return self.seg_service.get_segids(locs, segmentation='flywire_190410',
                                           coordinates=coordinates, mip=-1)
