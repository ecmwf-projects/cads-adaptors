import os
from tempfile import mkstemp

from . import STACK_DOWNLOAD_DIR, STACK_TEMP_DIR


def create_file(stage, suffix, info):
    """Return the path of a new result file or temp file, depending on whether
    the specified processing stage is the last in the series or not.
    """
    # If this isn't the last stage in the process then make a temp file.
    # Otherwise make a result file
    assert stage in info["stages"]
    temp = stage != info["stages"][-1]

    if temp:
        path = temp_file(info["config"], suffix=suffix)

    else:
        # Check a final result file isn't requested more than once
        if "result_file" in info:
            raise Exception("Adaptor trying to create >1 result file??")

        os.makedirs(STACK_DOWNLOAD_DIR, exist_ok=True)
        info["result_file"] = (
            f"{STACK_DOWNLOAD_DIR}/" + f"{info['config']['request_uid']}{suffix}"
        )
        path = info["result_file"]

    return path


def temp_file(config, suffix=""):
    """Make and return the path of a temporary file."""
    os.makedirs(STACK_TEMP_DIR, exist_ok=True)
    fd, target = mkstemp(
        prefix=config["request_uid"] + "_", suffix=suffix, dir=STACK_TEMP_DIR
    )
    os.close(fd)
    return target
