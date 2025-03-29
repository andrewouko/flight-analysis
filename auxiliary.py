def append_to_dict_lists(target_dict, row_dict):
    for key in row_dict:
        if key in target_dict:
            target_dict[key].append(row_dict[key])

carrier_dict = {
    'code': [],
    'name': [],
    'shortName': []
}

city_dict = {
    'code': [],
    'country': [],
    'name': []
}

airport_dict = {
    'city': [],
    'code': [],
    'latitude': [],
    'longitude': [],
    'name': []
}

aircraft_dict = {
    'code': [],
    'name': [],
    'width': []
}
