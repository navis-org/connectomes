# connectomes [WIP]
A unified interface to pull data from various connectomic datasets.

## Rationale
Over the past few years various connectomics datasets (for example the
[hemibrain](https://neuprint.janelia.org) or the
[MICrONS cortical mm^3](https://www.microns-explorer.org/cortical-mm3)) have
become available and many more are in the making. At the same time, there are a
number of very different platforms/backends (CATMAID, neuPrint, DVID, ChunkedGraph, etc.)
used to disseminate these data. This can be somewhat confusing and
cumbersome as users have to learn and switch between various libraries as they
explore datasets.

Inspired by the excellent [cloud-volume](https://github.com/seung-lab/cloud-volume),
`connectomes` seeks to provide an interface that has the same "feel" no matter
the backend used by the respective dataset. It does so by wrapping the various
tools (`cloud-volume`, `neuprint-python`, `dvid`, `fafbseg`, etc.) to provide uniform
interfaces across these backends and to allow quickly start interacting with new
datasets via simple mix & match and subclassing.

## Usage
`connectomes` provides "pre-configured" datasets which implement the same
core methods to allow you to fetch data:

```Python
>>> # Import one of the datasets
>>> import connectomes
>>> hb = connectomes.get('hemibrain')

>>> # Fetch a 10x10x10 segmentation cutout
>>> cutout = hb.segmentation[22630:22640, 33570:33580, 13980:13990]
>>> type(cutout)
numpy.ndarray
>>> cutout.shape
(10, 10, 10, 1)

>>> # Fetch a single skeleton
>>> sk = hb.skeleton.get(546217818)
>>> type(sk)
navis.core.neuronlist.NeuronList

>>> # Fetch the mesh for the same neuron
>>> m = hb.mesh.get(546217818)
navis.core.neuronlist.NeuronList
```

See the documentation for a full list of functions. Also note that methods might
accept slightly different parameters due to the idiosyncrasies of the various
datasets.

Available datasets:
- [x] Janelia hemibrain dataset: `connectomes.get('hemibrain')`

- [ ] FlyWire FAFB: `connectomes.get('flywire')`

- [ ] Female Adult Nerve Cord (FANC): `connectomes.get('fanc')`

- [ ] MICrONs cortical mm^3: `connectomes.get('microns_mm3')`
- [ ] MICrONs L2/3: `connectomes.get('microns_l23')`

- [ ] Virtual Fly Brains' FAFB (CATMAID): `connectomes.get('vfb_fafb')`
- [ ] Virtual Fly Brains' L1  (CATMAID): `connectomes.get('vfb_l1')`
- [ ] Virtual Fly Brains' VNC  (CATMAID): `connectomes.get('vfb_vnc')`
