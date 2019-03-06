from pymongo.collation import Collation
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

class _WriteOp(object):
    def __init__(
        self,
        filter: Optional[Mapping[str, Any]] = ...,
        doc: Optional[Mapping[str, Any]] = ...,
        upsert: Optional[bool] = ...) -> None: ...
    def __eq__(self, other: Any) -> bool: ...
    def __ne__(self, other: Any) -> bool: ...

class InsertOne(_WriteOp):
    def __init__(self, document: Mapping[str, Any]) -> None: ...
    def __repr__(self) -> str: ...

class DeleteOne(_WriteOp):
    def __init__(self, filter: Mapping[str, Any], collation: Optional[Collation] = ...) -> None: ...
    def __repr__(self) -> str: ...

class DeleteMany(_WriteOp):
    def __init__(self, filter: Mapping[str, Any], collation: Optional[Collation] = ...) -> None: ...
    def __repr__(self) -> str: ...

class ReplaceOne(_WriteOp):
    def __init__(
        self,
        filter: Mapping[str, Any],
        replacement: Mapping[str, Any],
        upsert: bool = ...,
        collation: Optional[Collation] = ...) -> None: ...
    def __repr__(self) -> str: ...

class UpdateOne(_WriteOp):
    def __init__(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        upsert: bool = ...,
        collation: Optional[Collation] = ...,
        array_filters: Optional[List[Mapping[str, Any]]] = ...) -> None: ...
    def __repr__(self) -> str: ...

class UpdateMany(_WriteOp):
    def __init__(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        upsert: bool = ...,
        collation: Optional[Collation] = ...,
        array_filters: Optional[List[Mapping[str, Any]]] = ...) -> None: ...
    def __repr__(self) -> str: ...

class IndexModel(object):
    def __init__(self, keys: Union[str, Sequence[Tuple[str, Union[int, str]]]], **kwargs: Any) -> None: ...
    @property
    def document(self) -> Dict[str, Any]: ...
