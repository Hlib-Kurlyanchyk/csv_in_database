from cleaning_ops import *


def test_price_clean():
    assert price_clean("1.340") == 1340.0


def test_address_clean():
    assert address_clean(' 08986 Bool') is "08986"


def test_immowelt_id_clean():
    assert immowelt_id_clean('[]') is None
