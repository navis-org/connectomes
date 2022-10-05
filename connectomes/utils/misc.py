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

import typing as tp
import json
import navis
import os
import time

from caveclient import CAVEclient
from pathlib import Path
from importlib import reload

import cloudvolume as cv
import numpy as np
import pandas as pd


__all__ = ['set_chunkedgraph_secret', 'get_chunkedgraph_secret',
           'get_cave_client', 'retry_on_fail', 'chunks_to_nm']

FLYWIRE_DATASETS = {'production': 'fly_v31',
                    'sandbox': 'fly_v26'}

CAVE_DATASETS = {'production': 'flywire_fafb_production',
                 'sandbox': 'flywire_fafb_sandbox'}

# Initialize without a volume
fw_vol = None
cave_clients = {}


def get_cave_client(datastack, token=None, force_new=False):
    """Get CAVE client.

    Parameters
    ----------
    dataset :   str
                Data set to create client for.
    token :     str, optional
                Your chunked graph secret (i.e. "CAVE secret").

    Returns
    -------
    CAVEclient

    """
    if datastack not in cave_clients or force_new:
        cave_clients[datastack] = CAVEclient(datastack, auth_token=token)

    return cave_clients[datastack]


def get_chunkedgraph_secret(domain):
    """Get chunked graph secret.

    Parameters
    ----------
    domain :    str
                Domain to get the secret for. Only relevant for
                ``cloudvolume>=3.11.0``.

    Returns
    -------
    token :     str

    """
    if hasattr(cv.secrets, 'cave_credentials'):
        token = cv.secrets.cave_credentials(domain).get('token', None)
        if not token:
            raise ValueError(f'No chunkedgraph secret for domain {domain} '
                             'found. Please see '
                             'fafbseg.flywire.set_chunkedgraph_secret to set '
                             'your secret.')
    else:
        try:
            token = cv.secrets.chunkedgraph_credentials['token']
        except BaseException:
            raise ValueError('No chunkedgraph secret found. Please see '
                             'fafbseg.flywire.set_chunkedgraph_secret to set your '
                             'secret.')
    return token


def set_chunkedgraph_secret(token, filepath=None,
                            domain='prod.flywire-daf.com'):
    """Set chunked graph secret (called "cave credentials" now).

    Parameters
    ----------
    token :     str
                Get your token from
                https://globalv1.flywire-daf.com/auth/api/v1/refresh_token
    filepath :  str filepath
                Path to secret file. If not provided will store in default path:
                ``~/.cloudvolume/secrets/{domain}-cave-secret.json``
    domain :    str
                The domain (incl subdomain) this secret is for.

    """
    assert isinstance(token, str), f'Token must be string, got "{type(token)}"'

    if not filepath:
        filepath = f'~/.cloudvolume/secrets/{domain}-cave-secret.json'
    elif not filepath.endswith('/chunkedgraph-secret.json'):
        filepath = os.path.join(filepath, f'{domain}-cave-secret.json')
    elif not filepath.endswith('.json'):
        filepath = f'{filepath}.json'

    filepath = Path(filepath).expanduser()

    # Make sure this file (and the path!) actually exist
    if not filepath.exists():
        if not filepath.parent.exists():
            filepath.parent.mkdir(parents=True)
        filepath.touch()

    with open(filepath, 'w+') as f:
        json.dump({'token': token}, f)

    # We need to reload cloudvolume for changes to take effect
    reload(cv.secrets)
    reload(cv)

    # Should also reset the volume after setting the secret
    global fw_vol
    fw_vol = None

    print("Token succesfully stored in ", filepath)


def parse_root_ids(x):
    """Parse root IDs.

    Always returns an array of integers.
    """
    if isinstance(x, navis.BaseNeuron):
        ids = [x.id]
    elif isinstance(x, navis.NeuronList):
        ids = x.id
    elif isinstance(x, (int, np.int)):
        ids = [x]
    else:
        ids = navis.utils.make_iterable(x)

    # Make sure we are working with proper numerical IDs
    try:
        return np.asarray(ids).astype(int)
    except ValueError:
        raise ValueError(f'Unable to convert given root IDs to integer: {ids}')
    except BaseException:
        raise


def retry_on_fail(func, cooldown=2, n_retries=3):
    """Wrap function to retry call on fail.

    Parameters
    ----------
    cooldown :  int | float
                Cooldown period in seconds between attempts.
    n_retries : int
                Number of retries before we give up.

    """
    def wrapper(*args, **kwargs):
        i = 0
        while True:
            i += 1
            try:
                res = func(*args, **kwargs)
                break
            except BaseException:
                if i > n_retries:
                    raise
            time.sleep(cooldown)
        return res
    return wrapper


def chunks_to_nm(xyz_ch, vol, voxel_resolution=[4, 4, 40]):
    """Map an L2 chunk location to Euclidean space.

    Parameters
    ----------
    xyz_ch :            array-like
                        (N, 3) array of chunk indices.
    vol :               cloudvolume.CloudVolume
                        CloudVolume object associated with the chunked space.
    voxel_resolution :  list, optional
                        Voxel resolution.

    Returns
    -------
    np.array
                        (N, 3) array of spatial points.

    """
    mip_scaling = vol.mip_resolution(0) // np.array(voxel_resolution, dtype=int)

    x_vox = np.atleast_2d(xyz_ch) * vol.mesh.meta.meta.graph_chunk_size
    return (
        (x_vox + np.array(vol.mesh.meta.meta.voxel_offset(0)))
        * voxel_resolution
        * mip_scaling
    )


def chunk(it: tp.Iterable, chunks: int) -> tp.Iterable[list]:
    out = []
    for item in it:
        out.append(item)
        if len(out) >= chunks:
            yield out
            out = []
    if out:
        yield out


class DataFrameBuilder:
    def __init__(self, columns, dtypes=None):
        self.columns = {c: [] for c in columns}
        self.dtypes = dtypes
        if dtypes is not None and len(dtypes) != len(self.columns):
            raise ValueError()

    def append_row(self, row: list):
        if len(row) != len(self.columns):
            raise ValueError()
        for item, col in zip(row, self.columns.values()):
            col.append(item)

    def append_dict(self, row: dict[str, tp.Any]):
        if len(row) != len(self.columns):
            raise ValueError()
        for k, v in row.items():
            self.columns[k].append(v)

    def build(self) -> pd.DataFrame:
        cols = dict()
        for idx, (k, v) in enumerate(self.columns.items()):
            if self.dtypes:
                v2 = np.asarray(v, self.dtypes[idx])
            else:
                v2 = np.asarray(v)
            cols[k] = v2
        return pd.DataFrame.from_dict(cols)
