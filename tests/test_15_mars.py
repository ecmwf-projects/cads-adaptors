import pytest

from cads_adaptors.adaptors import mars


@pytest.mark.parametrize(
    "exit_code,error_msg",
    (
        (1, "MARS has crashed."),
        (0, "MARS returned no data."),
    ),
)
def test_execute_mars_errors(tmp_path, monkeypatch, exit_code, error_msg):
    monkeypatch.chdir(tmp_path)  # execute_mars generates files in the working dir
    open("data.grib", "w")  # fake target
    context = mars.Context()
    with pytest.raises(RuntimeError, match=error_msg):
        mars.execute_mars(
            {},
            context=context,
            mars_cmd=("bash", "-c", f"echo output; echo error 1>&2; exit {exit_code}"),
        )
    assert context.stdout == "output\n"
    assert context.stderr == "error\n"
    assert context.user_visible_log == "output\n"
