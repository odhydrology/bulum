import unittest

import bulum.io as bio


class Tests(unittest.TestCase):

    def test_iqqm_out_reader(self):
        reader = bio.iqqm_out_reader("./src/bulum/io/tests/iqqm_results/O02l.IQN")
        reader.require(node="030", output="01")
        reader.require(node="065", output="01")
        df = reader.read()
        self.assertAlmostEqual(len(df.columns), 2)
        self.assertAlmostEqual(len(df), 43464)
        self.assertAlmostEqual(df["030_01.d"].sum(), 30598077.0495)

    def test_iqqm_out_reader2(self):
        reader = bio.iqqm_out_reader("./src/bulum/io/tests/iqqm_results/O02l.IQN")
        df = reader.read(read_all_availabe=True)
        self.assertAlmostEqual(df["030_01.d"].sum(), 30598077.0495)

    def test_iqqm_out_reader3(self):
        for engine in ["iqqmgui", "python"]:
            with self.subTest(engine=engine):
                reader = bio.IqqmOutReader("./src/bulum/io/tests/iqqm_results/O02l.IQN")
                reader.require(type=3.1, output=2)
                reader.require(supertype=8, output=2)
                reader.require(node=30, output=1)
                df = reader.read(engine=engine)
                self.assertEqual(len(df.columns), 14)
                self.assertEqual(len(df), 43464)
                self.assertAlmostEqual(df["020_02.d"].sum(), 561119.5652, delta=5)
                self.assertAlmostEqual(df["030_01.d"].sum(), 30598077.0495, delta=30)

    def test_num_to_name(self):
        reader = bio.IqqmOutReader("./src/bulum/io/tests/iqqm_results/O02l.IQN")
        reader.require(node="030", output="01")
        required_map = reader.num_to_name(which="required")
        self.assertIn("030", required_map)
        self.assertIsInstance(required_map["030"], str)
        available_map = reader.num_to_name(which="available")
        self.assertGreater(len(available_map), len(required_map))
        with self.assertRaises(ValueError):
            reader.num_to_name(which="invalid")


if __name__ == "__main__":
    unittest.main()
