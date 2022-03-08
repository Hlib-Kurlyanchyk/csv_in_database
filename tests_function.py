from cleaning_ops import *
import pytest


@pytest.mark.parametrize("input_data, output_data", [(" 4FS5D3S", "4FS5D3S"),
                                                     ("4FS5D3SH9JJK0", None),
                                                     ([], None),
                                                     (1, None),
                                                     ({}, None),
                                                     ('', None),
                                                     ("4FS5D", None)])
def test_immowelt_id_clean(input_data, output_data):
    assert immowelt_id_clean(input_data) == output_data


@pytest.mark.parametrize("input_data, output_data", [(" 12345 Bon-bon", 12345),
                                                     ("12345 Bon-bon 54321 Don-don", 12345),
                                                     ("34534543", 34534),
                                                     (4554, 4554),
                                                     (2.3, None),
                                                     ([], None),
                                                     ('', None),
                                                     ("ddsdvs", None)])
def test_address_clean(input_data, output_data):
    assert address_clean(input_data) == output_data


@pytest.mark.parametrize("input_data, output_data", [("123.456 Kaufpreis", 123456.0),
                                                     ("123.567,34", 123567.34),
                                                     ("1.200 EUR", 1200.0),
                                                     (2, 2.0),
                                                     ([], None),
                                                     ('', None),
                                                     ("ddsdvs", None)])
def test_price_clean(input_data, output_data):
    assert price_clean(input_data) == output_data

