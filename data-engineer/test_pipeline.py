import unittest

from pipeline import window_by_datetime, read_files_from_dir, parse_xml, process_to_RO


class TestPipeline(unittest.TestCase):
    def test_read_files_from_dir(self):
        files = read_files_from_dir('data')
        self.assertTrue(len(files) > 0)

    def test_parse_xml(self):
        files = read_files_from_dir('data')
        df = parse_xml(files)
        self.assertFalse(df.empty)

    def test_window_by_datetime(self):
        files = read_files_from_dir('data')
        df = parse_xml(files)
        windowed = window_by_datetime(df, '1D')
        self.assertTrue(len(windowed) > 0)

    def test_process_to_RO(self):
        files = read_files_from_dir('data')
        df = parse_xml(files)
        windowed = window_by_datetime(df, '1D')
        ros = process_to_RO(windowed)
        self.assertTrue(len(ros) > 0)


if __name__ == '__main__':
    unittest.main()
