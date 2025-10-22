import unittest
import warnings
import numpy as np
from bulum.utils import interpolation


class TestInterpolation(unittest.TestCase):
    """Test cases for the interpolation module, especially deprecation warnings."""

    def test_interp_deprecation_warning(self):
        """Test that interp function shows deprecation warning."""
        x = [1, 2, 3, 4]
        y = [10, 20, 30, 40]
        xi = [1.5, 2.5, 3.5]

        # Test that deprecation warning is raised
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")  # Ensure all warnings are triggered
            result = interpolation.interp(xi, x, y)

            # Check that a warning was issued
            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("bulum.utils.interp() is deprecated", str(w[0].message))
            self.assertIn("Use numpy.interp() directly instead", str(w[0].message))

        # Verify the function still works correctly
        expected = np.interp(xi, x, y)
        np.testing.assert_array_equal(result, expected)

    def test_interp_with_kwargs(self):
        """Test that interp function passes through kwargs correctly."""
        x = [1, 2, 3, 4]
        y = [10, 20, 30, 40]
        xi = [0.5, 4.5]  # Outside bounds to test left/right parameters

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # Ignore deprecation warning for this test

            # Test with left and right parameters
            result = interpolation.interp(xi, x, y, left=5, right=50)
            expected = np.interp(xi, x, y, left=5, right=50)
            np.testing.assert_array_equal(result, expected)

    def test_interp_equivalent_to_numpy(self):
        """Test that interp results are identical to numpy.interp."""
        # Test with various input types
        test_cases = [
            # Simple linear interpolation
            ([1.5, 2.5], [1, 2, 3], [10, 20, 30]),
            # Single point interpolation
            ([2.0], [1, 2, 3], [10, 20, 30]),
            # Edge case interpolation
            ([1, 3], [1, 2, 3], [10, 20, 30]),
        ]

        for xi, x, y in test_cases:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")  # Ignore deprecation warnings

                result = interpolation.interp(xi, x, y)
                expected = np.interp(xi, x, y)

                np.testing.assert_array_equal(result, expected,
                    err_msg=f"Failed for xi={xi}, x={x}, y={y}")

    def test_interp_return_type(self):
        """Test that interp returns numpy array like numpy.interp."""
        x = [1, 2, 3]
        y = [10, 20, 30]
        xi = [1.5, 2.5]

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = interpolation.interp(xi, x, y)

        self.assertIsInstance(result, np.ndarray)
        self.assertEqual(result.dtype, np.float64)

    def test_module_docstring_deprecation(self):
        """Test that module docstring indicates deprecation."""
        module_doc = interpolation.__doc__
        self.assertIn("deprecated", module_doc.lower())
        self.assertIn("numpy.interp", module_doc)


if __name__ == '__main__':
    unittest.main()