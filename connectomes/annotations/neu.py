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

from .base import AnnotationSource

import neuprint as neu


class NeuPrintAnnotationSource(AnnotationSource):
    def __init__(self, client):
        self.client = client

    def find(self, **criteria):
        """Find neurons matching given criteria.

        Parameters
        ----------
        **criteria
                    Criteria to search for. See examples!

        Examples
        --------
        >>> import connectomes
        >>> hb = connectomes.get('hemibrain')
        >>> meta = hb.annotations.find(type='DA1_lPN')
        >>>

        """
        x = neu.NeuronCriteria(**criteria)
        meta = neu.fetch_neurons(x, client=self.client)[0]

        meta.rename({'bodyId': 'id'}, axis=1, inplace=True)

        return meta
