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

from .base import SkeletonSource

import navis.interfaces.neuprint as neu


class NeuPrintSkeletonSource(SkeletonSource):
    def __init__(self, client):
        self.client = client

    def get(self, x):
        """Fetch skeletons for given neurons.

        Parameters
        ----------
        x :     int | list | str | neuprint.NeuronCriteria
                Defines which meshes to fetch. Can be:
                 - body IDs (integers) or lists thereof
                 - strings that define search criteria (see examples below)
                 - a ``NeuronCriteria`` defining the search criteria

        """
        if isinstance(x, str):
            criteria = {}
            for c in str.split(','):
                k, v = c.split('=')
                criteria[k.strip()] = v.strip().replace("'", '').replace('"', '')
            x = neu.NeuronCriteria(**criteria)

        return neu.fetch_skeletons(x, client=self.client)
