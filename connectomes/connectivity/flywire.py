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


from functools import partial

import numpy as np
import pandas as pd

from .base import ConnectivitySource
from ..utils.flywire import get_cave_client, retry

NoneType = type(None)

class FlywireConnectivitySource(ConnectivitySource):
    def __init__(self, dataset = 'production'):
        self.dataset = dataset
        # Get the cave client
        self.client = get_cave_client(dataset=self.dataset)

    def get_edges(self, sources, targets=None, min_score=10, batch_size=20):
        """Fetch edges between sources and targets.

        Parameters
        ----------
        sources :   int | list of int |
                    Body ID(s) of sources.
        targets :   int | list of int | None
                    Body ID(s) of targets.

        Returns
        -------
        edges :     pandas.DataFrame

        """
        if targets is None:
            targets = sources

        mat = self.client.materialize.version
        columns = ['pre_pt_root_id', 'post_pt_root_id', 'cleft_score']
        func = partial(retry(self.client.materialize.query_table),
                       table=self.client.materialize.synapse_table,
                       materialization_version=mat,
                       select_columns=columns)

        edges, syn = [], []
        for i in range(0, len(sources), batch_size):
            source_batch = sources[i:i+batch_size]
            for k in range(0, len(targets), batch_size):
                target_batch = targets[k:k+batch_size]

                this = func(filter_in_dict=dict(post_pt_root_id=target_batch,
                                                pre_pt_root_id=source_batch))

                # We need to drop the .attrs (which contain meta data from queries)
                # Otherwise we run into issues when concatenating
                this.attrs = {}

                if not this.empty:
                    syn.append(this)

        # Combine results from batches
        if len(syn):
            syn = pd.concat(syn, axis=0, ignore_index=True)
        else:
            edges = pd.DataFrame(np.zeros((len(sources), len(targets))),
                            index=sources, columns=targets)
            edges.index.name = 'source'
            edges.columns.name = 'target'
            return edges


        # Depending on how queries were batched, we need to drop duplicate synapses
        syn.drop_duplicates('id', inplace=True)

        # Rename some of those columns
        syn.rename({'post_pt_root_id': 'post', 'pre_pt_root_id': 'pre'},
                axis=1, inplace=True)

        # Next we need to run some clean-up:
        # Drop below threshold connections
        if min_score:
            syn = syn[syn.cleft_score >= min_score]

        # Aggregate
        cn = syn.groupby(['pre', 'post'], as_index=False).size()
        cn.columns = ['source', 'target', 'weight']

        # Pivot
        edges = cn.pivot(index='source', columns='target', values='weight').fillna(0)

        # Index to match order and add any missing neurons
        edges = edges.reindex(index=sources, columns=targets).fillna(0)
        return edges

    def get_synapses(self, x, transmitters=False, batch_size=20):
        """Retrieve synapse for given neurons.

        Parameters
        ----------
        x :         int | list of int | neu.NeuronCriteria
                    Body ID(s) to fetch synapses for. For more complicated
                    queries use neuprint-python's ``NeuronCriteria``. You can
                    use ``None`` to fetch all incoming edges of ``targets``.

        """
        mat = self.client.materialize.version
        
        columns = ['pre_pt_root_id', 'post_pt_root_id', 'cleft_score',
                'pre_pt_position', 'post_pt_position', 'id']

        if transmitters:
            columns += ['gaba', 'ach', 'glut', 'oct', 'ser', 'da']

        func = partial(retry(self.client.materialize.query_table),
                       table=self.client.materialize.synapse_table,
                       split_positions=True,
                       materialization_version=mat,
                       select_columns=columns)

        syn = []
        for i in range(0, len(x), batch_size):
            batch = x[i:i+batch_size]
            syn.append(func(filter_in_dict=dict(post_pt_root_id=batch)))
            syn.append(func(filter_in_dict=dict(pre_pt_root_id=batch)))

        # Drop attrs to avoid issues when concatenating
        for df in syn:
            df.attrs = {}

        # Combine results from batches
        syn = pd.concat(syn, axis=0, ignore_index=True)

        # Depending on how queries were batched, we need to drop duplicate synapses
        syn.drop_duplicates('id', inplace=True)

        # Rename some of those columns
        syn.rename({'post_pt_root_id': 'post',
                    'pre_pt_root_id': 'pre',
                    'post_pt_position_x': 'post_x',
                    'post_pt_position_y': 'post_y',
                    'post_pt_position_z': 'post_z',
                    'pre_pt_position_x': 'pre_x',
                    'pre_pt_position_y': 'pre_y',
                    'pre_pt_position_z': 'pre_z',
                    },
                axis=1, inplace=True)
        return syn
