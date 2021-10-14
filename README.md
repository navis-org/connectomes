# connectomes [WIP]
A unified interface to pull data from various connectomic datasets.

## Rationale
Over the past few years various connectomics datasets (for example the
[hemibrain](https://neuprint.janelia.org) or the
[MICrONS cortical mm^3](https://www.microns-explorer.org/cortical-mm3) ) have
become available and many more are in the making. At the same time, the number of
backends (CATMAID, neuPrint, DVID, ChunkedGraph, etc.) used to disseminate/host
those datasets have increased too. As a consequence, users have to learn and
switch between various libraries as they explore datasets.

Inspired by the excellent [cloud-volume](https://github.com/seung-lab/cloud-volume),
`connectomes` seeks to provide an interface that has the same "feel" no matter
the backend used by the dataset. It does so by wrapping the various tools
(cloud-volume, neuprint-python, dvid, etc.) to provide uniform interfaces
across these backends and to allow quickly start interacting with new
datasets via simple mix & match and subclassing.
