"""Tests for circular imports."""

import unittest
import traceback


class Tests(unittest.TestCase):

    def test_circular_imports(self):
        try:
            from . import clim
            from . import io
            from . import plots
            from . import stats
            from . import stoch
            from . import trans
            from . import utils
        except ModuleNotFoundError as e:
            self.fail(e.msg)
        except AttributeError as _:
            # if "circular import" in e.msg:
            self.fail(f"Circular import: {traceback.format_exc()}")


if __name__ == "__main__":
    unittest.main()
