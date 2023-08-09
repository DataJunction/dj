"""
JSON encoder for AST objects
"""
from json import JSONEncoder

from sqlmodel import select

from datajunction_server.models import Node
from datajunction_server.sql.parsing import ast


def remove_circular_refs(obj, _seen: set = None):
    """
    Short-circuits circular references in AST nodes
    """
    if _seen is None:
        _seen = set()
    if id(obj) in _seen:
        return None
    _seen.add(id(obj))
    if issubclass(obj.__class__, ast.Node):
        serializable_keys = [
            key for key in obj.__dict__.keys() if key not in obj.json_ignore_keys
        ]
        for key in serializable_keys:
            setattr(obj, key, remove_circular_refs(getattr(obj, key), _seen))
    _seen.remove(id(obj))
    return obj


class ASTEncoder(JSONEncoder):
    """
    JSON encoder for AST objects. Disables the original circular check in favor
    of our own version with _processed so that we can catch and handle circular
    traversals.
    """

    def __init__(self, *args, **kwargs):
        kwargs["check_circular"] = False
        self.markers = set()
        super().__init__(*args, **kwargs)

    def default(self, o):
        o = remove_circular_refs(o)
        json_dict = {
            "__class__": o.__class__.__name__,
        }
        if hasattr(o, "__json_encode__"):
            json_dict = {**json_dict, **o.__json_encode__()}
        return json_dict


def ast_decoder(session, json_dict):
    """Decodes json dict"""
    class_name = json_dict["__class__"]
    if not class_name or not hasattr(ast, class_name):
        return None
    clazz = getattr(ast, class_name)
    if class_name == "NodeRevision":
        instance = (
            session.exec(select(Node).where(Node.name == json_dict["name"]))
            .one()
            .current
        )
    else:
        instance = clazz(
            **{
                k: v
                for k, v in json_dict.items()
                if k not in {"__class__", "_type", "laterals", "_is_compiled"}
            },
        )
    for key, value in json_dict.items():
        if key not in {"__class__", "_is_compiled"}:
            try:
                setattr(instance, key, value)
            except AttributeError:
                pass

    if class_name == "Table":
        instance._columns = [  # pylint: disable=protected-access
            ast.Column(ast.Name(col.name), _table=instance, _type=col.type)
            for col in instance._dj_node.columns  # pylint: disable=protected-access
        ]
    return instance
