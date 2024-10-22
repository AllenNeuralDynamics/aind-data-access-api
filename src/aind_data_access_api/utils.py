"""Utility functions."""


def is_dict_corrupt(input_dict: dict) -> bool:
    """
    Checks that all the keys, included nested keys, don't contain '$' or '.'
    Parameters
    ----------
    input_dict : dict

    Returns
    -------
    bool
      True if input_dict is not a dict, or if nested dictionary keys contain
      forbidden characters. False otherwise.

    """
    if not isinstance(input_dict, dict):
        return True
    for key, value in input_dict.items():
        if "$" in key or "." in key:
            return True
        elif isinstance(value, dict):
            if is_dict_corrupt(value):
                return True
    return False
