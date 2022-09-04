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

import cloudvolume as cv
import numpy as np

from .base import SegmentationSource


class CloudVolSegmentationSource(SegmentationSource):
    def __init__(self, url):
        self.url = url
        self._cloudvol = None  # Lazily loaded

    @property
    def cloudvol(self):
        if not self._cloudvol:
            self._cloudvol = cv.CloudVolume(self.url, use_https=True, progress=False)
        return self._cloudvol

    def __getitem__(self, slices):
        return np.asarray(self.cloudvol[slices])
