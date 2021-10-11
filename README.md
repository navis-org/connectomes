# connectome backends
[WIP] Unified interface to various backends used for hosting connectomes in the cloud.

The basic idea here is to provide thin wrappers around the various tools (cloud-volume,
neuprint-python, dvid, etc.) to provide similar-ish interfaces across these backends 
and to allow quickly creating interfaces with new datasets via simple mix & match and 
subclassing.
