
def create_file(stage, suffix, info, context, temp_path=None):
    """Return the path of a new result file or temp file, depending on whether
       the specified processing stage is the last in the series or not."""

    assert stage in info['stages']
    temp = (stage != info['stages'][-1])

    if temp:
        if temp_path is None:
            path = context.create_temp_file(suffix)
        else:
            path = temp_path

    else:
        # Check a final result file isn't requested more than once
        if 'result_file' in info:
            raise Exception('Adaptor trying to create >1 result file??')
        #info['result_file'] = context.create_result_file(suffix)

        #path = info['result_file'].path
        path = 'alabala.txt'

    return path
