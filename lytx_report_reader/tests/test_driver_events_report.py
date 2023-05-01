import os
import csv
import unittest

import pandas as pd

from ..run import EventsReport

test_file = 'test.csv'
test_rows = [
            ['0a', '0b', '0c'],
            ['1a', '1b', '1c'],
        ]

class TestEventsReport(unittest.TestCase):
    def setUp(self) -> None:
        with open(test_file, 'w') as csv_file: #Create CSV for testing
            writer = csv.writer(csv_file, dialect='excel')
            writer.writerows(test_rows)

    def tearDown(self) -> None:
        os.remove(test_file) #Delete Created csv on completion
    
    #READ CSV TESTS
    def test_read_csv_data_matches(self):
        test_data = pd.DataFrame(test_rows)
        read_data = EventsReport().read_csv(test_file, use_headers=False)
        print({
            'Test': test_data,
            'Read': read_data,
        })
        self.assertTrue(read_data.equals(test_data))

    def test_read_csv_returns_dataframe(self):
        read_data = EventsReport().read_csv(test_file)
        self.assertIsInstance(read_data, pd.DataFrame)
    
    def test_set_report_headers(self):
        pass