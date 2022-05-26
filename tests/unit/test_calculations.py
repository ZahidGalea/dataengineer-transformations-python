from source.jobs.citibike.transformation.citibike_distance_calculation import haversine_distance


def test_haversine_distance():
    expected = '1.07'
    actual = haversine_distance(lat1=40.69102925677968,
                                lon1=-73.99183362722397,
                                lat2=40.6763947,
                                lon2=-73.99869893)
    assert expected == actual
