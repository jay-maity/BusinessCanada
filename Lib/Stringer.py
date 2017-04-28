"""Modifies string functions"""

def is_number(s):
    """
    Check if a string is a number or not
    :return:
    """
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False

def remove_quote(s):
    """
    removes quotes and replace with blank
    :param s:
    :return:
    """
    return s.replace("\"", " ")
