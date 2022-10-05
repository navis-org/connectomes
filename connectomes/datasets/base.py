from abc import ABC
import typing as tp
import webbrowser
import logging

if tp.TYPE_CHECKING:
    from ..annotations.base import AnnotationSource
    from ..connectivity.base import ConnectivitySource
    from ..meshes.base import MeshSource
    from ..segmentation.base import SegmentationSource
    from ..skeletons.base import SkeletonSource


logger = logging.getLogger(__name__)


class BaseDataSet(ABC):
    def __init__(
        self,
        annotations: tp.Optional[AnnotationSource] = None,
        connectivity: tp.Optional[ConnectivitySource] = None,
        mesh: tp.Optional[MeshSource] = None,
        segmentation: tp.Optional[SegmentationSource] = None,
        skeleton: tp.Optional[SkeletonSource] = None,
        doi_url: tp.Optional[str] = None,
    ) -> None:
        self.annotations = annotations
        self.connectivity = connectivity
        self.mesh = mesh
        self.segmentation = segmentation
        self.skeleton = skeleton
        self.doi_url = doi_url

    def view_reference(self):
        if self.doi_url is None:
            logger.warning("No DOI exists for this dataset")
        else:
            webbrowser.open(self.doi_url)

    def __repr__(self):
        return self.__str__()
