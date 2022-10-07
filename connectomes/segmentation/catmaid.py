from urllib.parse import urljoin
from .base import SegmentationSource
from ..utils.catmaid import CatmaidClient
import cloudvolume as cv
import typing as tp
import zarr
from zarr.storage import Store
import json
from PIL import Image
import numpy as np
import logging
import sys
from requests import Session

logger = logging.getLogger(__name__)
DIMS = "zyx"
ORIENTATIONS = ("XY", "XZ", "ZY")
orientation_to_idx = {k: v for v, k in enumerate(ORIENTATIONS)}

zarr_meta = {
    "zarr_format": 2,
    # "shape": None,  # to fill
    # "chunks": None,  # to fill
    "dtype": "|u1",
    "compressor": None,
    # "fill_value": None,  # to fill
    "order": "C",
    "filters": None,
    "dimension_separator": ".",
}


class JpegZarrStore(Store):
    is_listable = False
    is_writable = False
    is_erasable = False

    tile_fmt: str

    def __init__(
        self,
        url_base: str,
        shape: tp.Tuple[int, int, int],
        im_height_width: tp.Tuple[int, int],
        scale: int = 0,
        ext: str = ".jpg",
        fill_value: int = 0,
        attrs: tp.Optional[dict] = None,
        session: tp.Optional[Session] = None,
    ):
        self.url_base = url_base
        meta = zarr_meta.copy()
        meta.update({
            "shape": shape,
            "chunks": (1, *im_height_width),
            "fill_value": fill_value,
        })
        self.meta_bytes = json.dumps(meta).encode()
        self.scale = scale
        self.ext = ext
        self.attrs_bytes = json.dumps(attrs or dict()).encode()
        if session is None:
            session = Session()
        self.session = session

    def __getitem__(self, key: str) -> bytes:
        if "/" in key:
            raise ValueError(f"{type(self)} does not support groups")
        if key == ".zattrs":
            return self.attrs_bytes
        if key == ".zarray":
            return self.meta_bytes
        z, y, x = key.split(".")
        tail = self.tile_fmt.format(scale=self.scale, z=z, y=y, x=x, ext=self.ext)
        url = urljoin(self.url_base, tail)
        response = self.session.get(url, stream=True)
        if response.status_code == 404:
            raise KeyError()
        response.raise_for_status()
        img = Image.open(response.raw)
        return np.asarray(img).tobytes()

    def __delitem__(self, _):
        raise NotImplementedError("Store is not erasable")

    def __iter__(self):
        raise NotImplementedError("Store is not listable")

    def __len__(self):
        raise NotImplementedError("Store is not listable")

    def __setitem__(self):
        raise NotImplementedError("Store is not writable")


class JpegZarrStore1(JpegZarrStore):
    tile_fmt = "{z}/{y}_{x}_{scale}.{ext}"


class JpegZarrStore4(JpegZarrStore):
    tile_fmt = "{z}/{scale}/{y}_{x}.{ext}"


class JpegZarrStore5(JpegZarrStore):
    tile_fmt = "{scale}/{z}/{y}/{x}.{ext}"


JPEG_STORE_CLASSES = {
    1: JpegZarrStore1,
    4: JpegZarrStore4,
    5: JpegZarrStore5,
}


def cli_select(options: dict[int, str], prompt: str = ""):
    def p(*args, **kwargs):
        print(*args, **kwargs, file=sys.stderr)

    p(prompt)
    p("Type number and press enter. Use Ctrl+C to quit.")
    for k, v in sorted(options.items()):
        p(f"{k}. {v}")

    opt_str = ", ".join(str(o for o in sorted(options)))
    while True:
        try:
            selection = int(input("Selection: ").strip())
        except (ValueError, KeyError):
            p(f"Selection must be an integer.")
            continue
        if selection not in options:
            p(f"Selection must be one of {opt_str}")
            continue
        return selection


class CatmaidImageSourceSelector:
    def __init__(self, client: CatmaidClient, project_id: int) -> None:
        self.client = client
        self.project_id = project_id

    def get_stacks(self):
        return self.client.get(f"{self.project_id}/stacks").json()

    def get_stack_infos(self, stack_ids=None, filter_usable=True, filter_accessible=False):
        if stack_ids is None:
            stack_ids = [d["id"] for d in self.get_stacks()]

        urls = [
            (f"{self.project_id}/stack/{stack_id}/info", None)
            for stack_id in stack_ids
        ]
        for r in self.client.get_many(urls):
            d = r.json()
            if not filter_usable:
                yield d
                continue

            if d["orientation"] != orientation_to_idx["XY"]:
                logger.debug("Filtered stack %s for unsupported orientation %s", d["sid"], ORIENTATIONS[d["orientation"]])
                continue

            d["mirrors"] = [
                mirror
                for mirror in d["mirrors"]
                if mirror["tile_source_type"] in CatmaidImageSource.implemented_sources
            ]
            if not d["mirrors"]:
                logger.debug("Filtered stack %s for no usable mirrors", d["sid"])
            elif filter_accessible:
                yield self._filter_accessible(d)
            else:
                yield d

    def _filter_accessible(self, stack_info: dict[str, tp.Any]):
        logger.info("Filtering accessible stack mirrors, may take a few seconds")
        new_mirrors = []
        loc = stack_info["canary_location"]
        canary_slice = tuple(slice(loc[d], loc[d]+1) for d in DIMS)
        for mirror_info in stack_info["mirrors"]:

            try:
                arr = CatmaidImageSource.from_stack_info(stack_info, mirror_info)
            except NotImplementedError:
                continue

            try:
                arr[canary_slice]
            except Exception:
                logger.exception("Inaccessible mirror %s for stack %s", mirror_info["id"], stack_info["sid"])
                continue

            new_mirrors.append(mirror_info)

        sinfo = stack_info.copy()
        sinfo["mirrors"] = new_mirrors
        return sinfo

    def get_image_source(self, stack_id: int, mirror_id: tp.Optional[int] = None):
        sinfo = next(self.get_stack_infos([stack_id]))
        mirrors_by_id = {
            m["id"]: m for m in sinfo["mirrors"]
        }
        if mirror_id is None:
            mirror_id = min(mirrors_by_id)

        mirror = mirrors_by_id[mirror_id]
        return CatmaidImageSource.from_stack_info(sinfo, mirror)

    def select_stack_mirror(self):
        stacks = list(self.get_stack_infos())
        if not stacks:
            logger.warning("No stacks found")
        stack_options = {
            s["sid"]: ": ".join([s["stitle"], s["description"]]) for s in stacks
        }
        stack_id = int(cli_select(stack_options, "Select stack by ID."))
        stack = next(s for s in stacks if s["sid"] == stack_id)
        mirror_options = {
            m["id"]: f"{m['title']} ({m['image_base']})" for m in stack["mirrors"]
        }
        mirror_id = int(cli_select(mirror_options, "Select mirror by ID."))
        mirror = next(m for m in stack["mirrors"] if m["id"] == mirror_id)

        return CatmaidImageSource.from_stack_info(stack, mirror)


class CatmaidImageSource(SegmentationSource):
    implemented_sources = {*JPEG_STORE_CLASSES, 11, 13}

    def __init__(
        self,
        tile_source_type: int,
        source_base_url: str,
        dimension: tuple[int, int, int],
        resolution: tuple[float, float, float],
        num_zoom_levels: int,
        file_extension: str,
        tile_width: int,
        tile_height: int,
        orientation: tp.Literal["XY", "XZ", "ZY"] = "XY",
    ):
        self.source_base_url = source_base_url
        self.dimension = dimension
        self.resolution = resolution
        self.num_zoom_levels = num_zoom_levels
        self.file_extension = file_extension
        self.tile_width = tile_width
        self.tile_height = tile_height
        supported_orientation = DIMS[:-3:-1].upper()
        if orientation != supported_orientation:
            raise NotImplementedError(
                f"Only {supported_orientation} orientation is currently supported"
            )
        self.orientation = orientation
        self.tile_source_type = tile_source_type

        self.array = self._instantiate_array()

    def _instantiate_array(self):
        if self.tile_source_type in JPEG_STORE_CLASSES:
            return self._instantiate_jpeg_stack()
        elif self.tile_source_type == 11:
            return self._instantiate_n5_blocks()
        elif self.tile_source_type == 13:
            return cv.CloudVolume(self.source_base_url, use_https=True, progress=False)
        else:
            raise NotImplementedError()

    def _instantiate_jpeg_stack(self):
        store = JPEG_STORE_CLASSES[self.tile_source_type](
            self.source_base_url,
            self.dimension,
            (self.tile_height, self.tile_width),
            ext=self.file_extension,
        )
        return zarr.Array(store)

    def _instantiate_n5_blocks(self):
        url = self.source_base_url.replace("%SCALE_DATASET%", "s0")
        split = url.split("/")
        tail = split[-1]
        if tail != "2_1_0":
            raise NotImplementedError("Only N5 volumes sliced in dimensions 2, 1, 0 are supported")
        root_components = []
        # todo: look up for attributes.json
        components = iter(split[:-1])
        for component in components:
            root_components.append(component)
            if component.lower().endswith(".n5"):
                break
        ds_path = "/".join(components)
        root_path = "/".join(root_components)
        store = zarr.N5FSStore(root_path)
        g = zarr.Group(store)
        ds = g[ds_path]
        return ds

    def __getitem__(self, slices):
        return self.array.__getitem__(slices)

    @classmethod
    def from_stack_info(cls, stack_info: dict[str, tp.Any], mirror_info: dict[str, tp.Any]):
        return cls(
            mirror_info["tile_source_type"],
            mirror_info["image_base"],
            tuple(stack_info["dimension"][d] for d in DIMS),
            tuple(stack_info["resolution"][d] for d in DIMS),
            stack_info["num_zoom_levels"],
            mirror_info["file_extension"],
            mirror_info["tile_width"],
            mirror_info["tile_height"],
            ORIENTATIONS[stack_info["orientation"]],
        )
