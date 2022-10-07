from abc import ABC
import typing as tp
import webbrowser
import logging
from urllib.parse import urljoin

if tp.TYPE_CHECKING:
    from ..annotations.base import AnnotationSource
    from ..connectivity.base import ConnectivitySource
    from ..meshes.base import MeshSource
    from ..segmentation.base import SegmentationSource
    from ..skeletons.base import SkeletonSource


logger = logging.getLogger(__name__)
DOI_ORG = "https://doi.org/"


class BaseDataSet(ABC):
    def __init__(
        self,
        annotations: tp.Optional["AnnotationSource"] = None,
        connectivity: tp.Optional["ConnectivitySource"] = None,
        mesh: tp.Optional["MeshSource"] = None,
        segmentation: tp.Optional["SegmentationSource"] = None,
        skeleton: tp.Optional["SkeletonSource"] = None,
        doi: tp.Optional[str] = None,
    ) -> None:
        self.annotations = annotations
        self.connectivity = connectivity
        self.mesh = mesh
        self.segmentation = segmentation
        self.skeleton = skeleton
        self.doi = doi

    def view_reference(self):
        if self.doi is None:
            logger.warning("No DOI exists for this dataset")
        else:
            doi_url = urljoin(DOI_ORG, self.doi)
            webbrowser.open(doi_url)

    def __repr__(self):
        return self.__str__()
