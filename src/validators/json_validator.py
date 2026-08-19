import json


def is_str_valid_json(text: str) -> bool:
    # checks if a string can be parsed as json (e.g. reading from a file/response)
    try:
        json.loads(text)
        return True
    except Exception:
        return False


def is_obj_valid_json(obj) -> bool:
    # checks if a python object can be serialized to json (e.g. before writing it out)
    try:
        json.dumps(obj)
        return True
    except Exception:
        return False
