
from .base import AnnotationSource
from ..utils.catmaid import CatmaidClient
from dataclasses import dataclass, asdict
import typing as tp
import datetime as dt


def copy_if_exists(src_dict: dict, tgt_dict: dict, key):
    val = src_dict.get(key)
    if val is not None:
        tgt_dict[key] = val


def opt_chain(*iterables):
    for it in iterables:
        if it is not None:
            yield from it


def is_str_only(iterable):
    has_str = False
    has_nonstr = False
    for item in iterable:
        if isinstance(item, str):
            has_str = True
            if has_nonstr:
                return None
        else:
            has_nonstr = True
            if has_str:
                return None

    return has_str


@dataclass
class AnnotationCriteria:
    name: tp.Optional[str] = None
    name_exact: tp.Optional[bool] = None
    name_case_sensitive: tp.Optional[bool] = None
    annotation_date_start: tp.Optional[dt.date] = None
    annotation_date_end: tp.Optional[dt.date] = None
    annotated_with: tp.Optional[list[int]] = None
    not_annotated_with: tp.Optional[list[int]] = None
    expand_sub_annotations: tp.Optional[list[int]] = None
    return_annotations: bool = False

    def to_data(self):
        self_dict = asdict(self)
        d = dict()
        copy_if_exists(self_dict, d, "name_exact")
        copy_if_exists(self_dict, d, "name")
        copy_if_exists(self_dict, d, "name_case_sensitive")
        copy_if_exists(self_dict, d, "annotated_with")
        copy_if_exists(self_dict, d, "not_annotated_with")
        copy_if_exists(self_dict, d, "expand_sub_annotations")
        d["types"] = ["annotation" if self.return_annotations else "neuron"]

        ann_ref_is_str = is_str_only(opt_chain(
            self.annotated_with,
            self.not_annotated_with,
            self.expand_sub_annotations,
        ))
        if ann_ref_is_str is None:
            raise ValueError("Annotation references must be all str or all int")
        elif ann_ref_is_str:
            d["annotation_reference"] = "name"
        else:
            d["annotation_reference"] = "id"

        if self.annotation_date_start is not None:
            d["annotation_date_start"] = self.annotation_date_start.isoformat()
        if self.annotation_date_end is not None:
            d["annotation_date_end"] = self.annotation_date_end.isoformat()
        return d


class CatmaidAnnotationSource(AnnotationSource):
    def __init__(self, client: CatmaidClient, project_id: int):
        self.client = client
        self.project_id = project_id

    def _find_entities(self, criteria: AnnotationCriteria):
        d = criteria.to_data()
        url = f"{self.project_id}/annotations/query-targets"
        return self.client.post(url, d).json()["entities"]

    def find_annotations(
        self,
        name: tp.Optional[str] = None,
        name_exact: tp.Optional[bool] = None,
        name_case_sensitive: tp.Optional[bool] = None,
        annotation_date_start: tp.Optional[dt.date] = None,
        annotation_date_end: tp.Optional[dt.date] = None,
        annotated_with: tp.Optional[list[int]] = None,
        not_annotated_with: tp.Optional[list[int]] = None,
        expand_sub_annotations: tp.Optional[list[int]] = None,
    ) -> tp.Iterable[tuple[str, int]]:
        """Find annotations based on some criteria.

        Yields pairs of (name, annotation_id).
        """
        criteria = AnnotationCriteria(
            name, name_exact, name_case_sensitive,
            annotation_date_start, annotation_date_end,
            annotated_with,
            not_annotated_with,
            expand_sub_annotations,
            True,
        )
        entities = self._find_entities(criteria)
        for row in entities:
            yield row["name"], int(row["id"])

    def find(
        self,
        name: tp.Optional[str] = None,
        name_exact: tp.Optional[bool] = None,
        name_case_sensitive: tp.Optional[bool] = None,
        annotation_date_start: tp.Optional[dt.date] = None,
        annotation_date_end: tp.Optional[dt.date] = None,
        annotated_with: tp.Optional[list[int]] = None,
        not_annotated_with: tp.Optional[list[int]] = None,
        expand_sub_annotations: tp.Optional[list[int]] = None,
    ) -> tp.Iterable[tuple[str, int]]:
        """Find neurons based on some annotation criteria.

        Yields pairs of (neuron_name, skeleton_id).
        """
        criteria = AnnotationCriteria(
            name, name_exact, name_case_sensitive,
            annotation_date_start, annotation_date_end,
            annotated_with,
            not_annotated_with,
            expand_sub_annotations,
            False,
        )
        entities = self._find_entities(criteria)
        for row in entities:
            name = row["name"]
            for skid in row["skeleton_ids"]:
                yield name, int(skid)
