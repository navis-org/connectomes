import webbrowser
from connectomes.annotations.catmaid import CatmaidAnnotationSource
from .base import BaseDataSet
import typing as tp
from ..utils.catmaid import CatmaidClient
from ..connectivity.catmaid import CatmaidConnectivitySource
from ..meshes.catmaid import CatmaidMeshSource
from ..skeletons.catmaid import CatmaidSkeletonSource
from ..segmentation.catmaid import CatmaidImageSourceSelector


class CatmaidDataSet(BaseDataSet):
    def __init__(self, client: CatmaidClient, project_id: int, doi=None, stack_mirror_ids: tp.Optional[tuple[int, int]]=None):
        self.url = client.server
        if stack_mirror_ids:
            selector = CatmaidImageSourceSelector(client, project_id)
            img = selector.get_image_source(*stack_mirror_ids)
        else:
            img = None
        super().__init__(
            annotations=CatmaidAnnotationSource(client, project_id),
            connectivity=CatmaidConnectivitySource(client, project_id),
            mesh=CatmaidMeshSource(client, project_id),
            segmentation=img,
            skeleton=CatmaidSkeletonSource(client, project_id),
            doi=doi,
        )
        self.client = client
        self.project_id = project_id

    def view_catmaid(self):
        webbrowser.open(self.url)


class VfbDataSet(CatmaidDataSet):
    slug: str
    project_id: int = 1
    doi_url: tp.Optional[str] = None
    stack_mirror_ids: tp.Optional[tuple[int, int]] = (1, 1)

    def __init__(
        self, url="https://{slug}.catmaid.virtualflybrain.org",
        project_id=None,
        doi_url=None
    ):
        client = CatmaidClient(url.format(slug=self.slug))
        pid = project_id or self.project_id
        super().__init__(
            client,
            pid,
            doi_url or self.doi_url,
            self.stack_mirror_ids,
        )


# TODO: check stacks/mirrors

class FafbVfb(VfbDataSet):
    slug = "fafb"
    doi = "10.1016/j.cell.2018.06.019"


class FancVfb(VfbDataSet):
    slug = "fanc"
    doi = "10.1016/j.cell.2020.12.013"


class FancJrc2018Vfb(VfbDataSet):
    slug = "fanc"
    project_id = 2
    doi = "10.1016/j.cell.2020.12.013"


class L1emVfb(VfbDataSet):
    slug = "l1em"
    doi = "10.1038/nature14297"


class L3vncVfb(VfbDataSet):
    slug = "l3vnc"
    doi = "10.7554/eLife.29089"


class Abd15Vfb(VfbDataSet):
    slug = "abd1.5"
    doi = "10.1038/nature14297"


class IavRoboVfb(VfbDataSet):
    slug = "iav-robo"
    doi = "10.1016/j.neuron.2020.10.004"


class IavTntVfb(VfbDataSet):
    slug = "iav-tnt"
    doi = "10.1016/j.neuron.2020.10.004"
