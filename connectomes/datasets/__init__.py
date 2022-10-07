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

import functools
import fuzzywuzzy
import neuprint as neu
import os

from .base import BaseDataSet
from ..meshes.neu import NeuPrintMeshSource
from ..skeletons.neu import NeuPrintSkeletonSource
from ..segmentation.cloudvol import CloudVolSegmentationSource
from ..connectivity.neu import NeuPrintConnectivitySource
from ..annotations.neu import NeuPrintAnnotationSource
from .catmaid import (
    FafbVfb,
    FancVfb,
    FancJrc2018Vfb,
    L1emVfb,
    L3vncVfb,
    Abd15Vfb,
    IavRoboVfb,
    IavTntVfb,
)


@functools.lru_cache
def get(dataset, *args, **kwargs):
    """Get/initialize given dataset.

    Parameters
    ----------
    dataset :   str
                Name of the dataset to initialize.
    *args/**kwargs
                Additional arguments passed to the initialization of the
                dataset.
    """
    if dataset not in DATASETS:
        match = process.extractOne(dataset, list(DATASETS))[0]
        raise ValueError(f'Did not find any dataset matching "{dataset}". Did '
                         f'you perhaps mean "{match}" instead?')

    return DATASETS[dataset](*args, **kwargs)


class HemiBrain(BaseDataSet):
    """Interface with the Janelia 'Hemibrain' dataset.

    Parameters
    ----------
    version :   str
                Version to use. Defaults to the currently lates (1.2.1).
    server :    str
                The server to use. Defaults to the public service.

    References
    ----------
    Louis K Scheffer et al. (2020) A connectome and analysis of the adult
    Drosophila central brain eLife 9:e57443. https://doi.org/10.7554/eLife.57443

    """
    def __init__(self, version='1.2.1', server='https://neuprint.janelia.org', token=None):
        # Check if credentials are set
        self.check_token()

        self.client = neu.Client(server, dataset=f'hemibrain:v{version}', token=token)
        self.version = version

        # Extract segmentation source (this probably needs some checks and balances)
        segs = [s for s in self.client.meta['neuroglancerMeta'] if s.get('dataType') == 'segmentation']
        seg_source = segs[0]['source']

        super().__init__(
            mesh=NeuPrintMeshSource(self.client),
            skeleton=NeuPrintSkeletonSource(self.client),
            segmentation=CloudVolSegmentationSource(seg_source),
            connectivity=NeuPrintConnectivitySource(self.client),
            annotations=NeuPrintAnnotationSource(self.client),
            doi_url="https://doi.org/10.7554/eLife.57443",
        )

        self.reference = 'Scheffer et al., eLife (2020)'

    def __str__(self):
        return f'Janelia "hemibrain" dataset (v{self.version})'

    def check_token(self):
        if 'NEUPRINT_APPLICATION_CREDENTIALS' not in os.environ:
            msg = ('In order to programmatically query the Janelia hemibrain '
                   'dataset you have to get and set an API token:\n',
                   ' 1. Visit https://neuprint.janelia.org and sign up.\n'
                   ' 2. On the website click on your profile icon in the top '
                   'right and select "Account".\n'
                   ' 3. Take the auth token and set it as "NEUPRINT_APPLICATION_CREDENTIALS" '
                   'environment variable. The exact way to set such variables '
                   'depends on your OS/terminal - I recommend googling it. '
                   'Alternatively, you can also provide the token directly via '
                   '`connectomes.get("hemibrain", token="eyJhGc...")`.')
            raise ValueError(msg)


# Add more datasets here
DATASETS = {
    'hemibrain': HemiBrain,
    'fafb_vfb': FafbVfb,
    'fanc_vfb': FancVfb,
    'fanc_jrc2018_female_vfb': FancJrc2018Vfb,
    'l1em_vfb': L1emVfb,
    'l3vnc_vfb': L3vncVfb,
    'abd1.5_vfb': Abd15Vfb,
    'iav_robo_vfb': IavRoboVfb,
    'iav_tnt_vfb': IavTntVfb,
}
