import pytest
import bulum.io as bio
from bulum import utils


@pytest.mark.parametrize("path,expected_len", [
    ("./src/bulum/io/tests/res_csv_files/simple_model.res.csv", 49155),
    ("./src/bulum/io/tests/test_data.csv", 10),
    ("./src/bulum/io/tests/da_file/BUR_FLWX.IDX", 41819),
    ("./src/bulum/io/tests/M_L1#030.01d", None),
])
def test_read_dispatch(path, expected_len):
    df = bio.read(path)
    assert isinstance(df, utils.TimeseriesDataframe)
    assert len(df) > 0
    if expected_len is not None:
        assert len(df) == expected_len


def test_read_unknown_extension():
    with pytest.raises(ValueError):
        bio.read("./src/bulum/io/tests/test_data.unknown")
