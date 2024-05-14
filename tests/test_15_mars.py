# import pytest

# from cads_adaptors.adaptors import mars


# @pytest.mark.parametrize(
#     "cmd,error_msg",
#     (
#         ("cat r; echo error 1>&2; exit 1", "MARS has crashed."),
#         ("cat r; touch data.grib; echo error 1>&2", "MARS returned no data."),
#     ),
# )
# def test_execute_mars_errors(tmp_path, monkeypatch, cmd, error_msg):
#     monkeypatch.chdir(tmp_path)  # execute_mars generates files in the working dir
#     context = mars.Context()
#     with pytest.raises(RuntimeError, match=error_msg):
#         mars.execute_mars(
#             {},
#             context=context,
#             mars_cmd=("bash", "-c", cmd),
#         )
