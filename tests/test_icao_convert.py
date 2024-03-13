from adsb_actions.icao_convert import icao_to_n_or_c

def test_icao_convert():
    result = icao_to_n_or_c('aab1cf')
    assert result == "N78888"

    result = icao_to_n_or_c('c07bed')
    assert result == "C-GUYE"

    result = icao_to_n_or_c('C00BCF')
    assert result == "C-FEMG"
