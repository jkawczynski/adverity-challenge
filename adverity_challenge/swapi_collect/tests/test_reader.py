from unittest import TestCase, mock

import petl as etl
from parameterized import parameterized
from swapi_collect.models import SwapiCollection
from swapi_collect.reader import get_header, get_value_count, read_collection

MOCK_DATA = etl.fromcolumns(
    (
        (
            "row1val1",
            "row2val1",
            "row3val1",
            "row4val1",
            "row5val1",
        ),
        (
            "row1val2",
            "row2val2",
            "row3val2",
            "row4val2",
            "row5val2",
        ),
        (
            "row1val3",
            "row2val3",
            "row3val3",
            "row4val3",
            "row5val3",
        ),
    ),
    header=("header1", "header2", "header3"),
)


class GetHeaderTestCase(TestCase):
    """Tests case for `get_header` method"""

    def setUp(self):
        self.mock_collection = SwapiCollection()

    @mock.patch("swapi_collect.reader.get_data_from_collection", return_value=MOCK_DATA)
    def test_get_header(self, *args):
        """Should return first row of the data"""
        headers = get_header(self.mock_collection)
        self.assertEqual(headers, ("header1", "header2", "header3"))


class ReadCollectionTestCase(TestCase):
    """Test case for `read_collection`"""

    def setUp(self):
        self.mock_collection = SwapiCollection()

    @mock.patch(
        "swapi_collect.reader.get_data_from_collection",
        return_value=MOCK_DATA,
    )
    def test_read_collection_from_start(self, *args):
        """Should return header row and data from the start limited by 2 rows"""
        headers, data = read_collection(self.mock_collection, limit=2)

        expected_data = (
            (
                "row1val1",
                "row1val2",
                "row1val3",
            ),
            (
                "row2val1",
                "row2val2",
                "row2val3",
            ),
        )

        self.assertEqual(headers, ("header1", "header2", "header3"))
        self.assertEqual(tuple(data), expected_data)

    @mock.patch(
        "swapi_collect.reader.get_data_from_collection",
        return_value=MOCK_DATA,
    )
    def test_read_collection_start2(self, *args):
        """Should return header row and data from the the second row"""
        headers, data = read_collection(self.mock_collection, start=2)

        expected_data = (
            (
                "row3val1",
                "row3val2",
                "row3val3",
            ),
            (
                "row4val1",
                "row4val2",
                "row4val3",
            ),
            (
                "row5val1",
                "row5val2",
                "row5val3",
            ),
        )

        self.assertEqual(headers, ("header1", "header2", "header3"))
        self.assertEqual(tuple(data), expected_data)


VALUE_COUNT_MOCK_DATA = etl.fromcolumns(
    (
        (
            80,
            120,
            80,
            130,
            50,
            80,
        ),
        (
            "red",
            "blue",
            "red",
            "red",
            "blue",
            "green",
        ),
        (
            "male",
            "female",
            "male",
            "female",
            "female",
            "male",
        ),
    ),
    header=("mass", "eye_color", "gender"),
)


class GetValueCountTestCase(TestCase):
    """Test case for `get_value_count`"""

    def setUp(self):
        self.mock_collection = SwapiCollection()

    @parameterized.expand(
        [
            (
                ("mass",),
                ((80, 3), (50, 1), (120, 1), (130, 1)),
            ),
            (
                ("mass", "eye_color"),
                (
                    (80, "red", 2),
                    (50, "blue", 1),
                    (80, "green", 1),
                    (120, "blue", 1),
                    (130, "red", 1),
                ),
            ),
            (
                ("eye_color", "gender"),
                (
                    ("blue", "female", 2),
                    ("red", "male", 2),
                    ("green", "male", 1),
                    ("red", "female", 1),
                ),
            ),
        ]
    )
    @mock.patch(
        "swapi_collect.reader.get_data_from_collection",
        return_value=VALUE_COUNT_MOCK_DATA,
    )
    def test_value_count(self, columns, expected_data, *args):
        headers, value_count_data = get_value_count(
            self.mock_collection, columns=columns
        )
        self.assertEqual(headers, columns + ("value",))
        self.assertEqual(tuple(value_count_data), expected_data)
