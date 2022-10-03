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

from .base import ConnectivitySource

import neuprint as neu

NoneType = type(None)

class NeuPrintConnectivitySource(ConnectivitySource):
    def __init__(self, client):
        self.client = client

    def get_edges(self, sources, targets, by_roi=False):
        """Fetch edges between sources and targets.

        Parameters
        ----------
        sources :   int | list of int | neu.NeuronCriteria
                    Body ID(s) of sources. For more complicated queries use
                    neuprint-python's ``NeuronCriteria``. You can use ``None``
                    to fetch all incoming edges of ``targets``.
        targets :   int | list of int |
                    Body ID(s) of targets. For more complicated queries use
                    neuprint-python's ``NeuronCriteria``. You can use ``None``
                    to fetch all outgoing edges of ``sources``.
        by_roi :    bool
                    Whether to separate edges into regions of interests.

        Returns
        -------
        edges :     pandas.DataFrame

        """
        if not isinstance(sources, (neu.NeuronCriteria, NoneType)):
            sources = neu.NeuronCriteria(bodyId=sources)
        if not isinstance(targets, (neu.NeuronCriteria, NoneType)):
            targets = neu.NeuronCriteria(bodyId=targets)

        meta, edges = neu.fetch_adjacencies(sources=sources,
                                            targets=targets,
                                            client=self.client)

        edges.columns = ['source', 'target', 'roi', 'weight']

        if not by_roi:
            edges = edges.groupby(['source', 'target'],
                                  as_index=False).weight.sum()

        return edges

    def get_synapses(self, x):
        """Retrieve synapse for given neurons.

        Parameters
        ----------
        x :         int | list of int | neu.NeuronCriteria
                    Body ID(s) to fetch synapses for. For more complicated
                    queries use neuprint-python's ``NeuronCriteria``. You can
                    use ``None`` to fetch all incoming edges of ``targets``.

        """
        if not isinstance(x, (neu.NeuronCriteria, NoneType)):
            x = neu.NeuronCriteria(bodyId=x)

        syn = neu.fetch_synapses(x, client=self.client)

        syn.rename({'bodyId': 'id'}, axis=1, inplace=True)

        return syn
