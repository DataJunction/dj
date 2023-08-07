"""
JSON encoder for AST objects
"""
from json import JSONEncoder


class ASTEncoder(JSONEncoder):
    """
    JSON encoder for AST objects. Disables the original circular check in favor
    of our own version with _processed so that we can catch and handle circular
    traversals.
    """

    def __init__(self, *args, **kwargs):
        kwargs["check_circular"] = False  # no need to check anymore
        super().__init__(*args, **kwargs)
        self._processed = set()

    def default(self, o):
        if id(o) in self._processed:
            return None
        self._processed.add(id(o))

        if o.__class__.__name__ == "NodeRevision":
            return {
                "__class__": o.__class__.__name__,
                "name": o.name,
                "type": o.type,
            }

        json_dict = {
            k: o.__dict__[k]
            for k in o.__dict__
            if hasattr(o, "json_ignore_keys") and k not in o.json_ignore_keys
        }
        json_dict["__class__"] = o.__class__.__name__
        return json_dict
