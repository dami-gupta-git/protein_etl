import pytest

@pytest.mark.parametrize("file_name, expected", [
       ["a.csv",("M1", "2020", 5.13),]
    ]
)
def test_csv_reader(file_name, expected):
    # Test CSVReader - match that the test file produces expected output
    pass
