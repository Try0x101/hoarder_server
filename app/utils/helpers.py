def safe_string(value):
    if value is None or value == "":
        return None
    return str(value)
