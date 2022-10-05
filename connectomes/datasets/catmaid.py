from connectomes.annotations.catmaid import CatmaidAnnotationSource
from .base import BaseDataSet
import typing as tp
from ..utils.catmaid import CatmaidClient
from ..connectivity.catmaid import CatmaidConnectivitySource
from ..meshes.catmaid import CatmaidMeshSource
from ..skeletons.catmaid import CatmaidSkeletonSource


class CatmaidDataSet(BaseDataSet):
    def __init__(self, client: CatmaidClient, project_id: int, doi_url=None):
        super().__init__(
            annotations=CatmaidAnnotationSource(client, project_id),
            connectivity=CatmaidConnectivitySource(client, project_id),
            mesh=CatmaidMeshSource(client, project_id),
            segmentation=None,
            skeleton=CatmaidSkeletonSource(client, project_id),
            doi_url=doi_url
        )
        self.client = client
        self.project_id = project_id


class VfbDataSet(CatmaidDataSet):
    slug: str
    project_id: int = 1
    doi_url: tp.Optional[str] = None

    def __init__(
        self, url="https://{slug}.catmaid.virtualflybrain.org", project_id=None,
        doi_url=None
    ):
        super().__init__(
            CatmaidClient(url.format(slug=self.slug)),
            project_id or self.project_id,
            doi_url or self.doi_url,
        )


# TODO: DOIs

class FafbVfb(VfbDataSet):
    slug = "fafb"
    doi_url = ""


class FancVfb(VfbDataSet):
    slug = "fanc"
    doi_url = ""


class FancJrc2018Vfb(VfbDataSet):
    slug = "fanc"
    project_id = 2
    doi_url = ""


class L1emVfb(VfbDataSet):
    slug = "l1em"
    doi_url = ""


class L3vncVfb(VfbDataSet):
    slug = "l3vnc"
    doi_url = ""


class Abd15Vfb(VfbDataSet):
    slug = "abd1.5"
    doi_url = ""


class IavRoboVfb(VfbDataSet):
    slug = "iav-robo"
    doi_url = ""


class IavTntVfb(VfbDataSet):
    slug = "iav-tnt"
    doi_url = ""
