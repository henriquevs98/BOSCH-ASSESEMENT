
# Deprecated columns to be deleted in main.py
deprecated_columns= ['federal_agency_id',
                     'groups_with_access_code',
                     'ng_fill_type_code',
                     'ng_psi',
                     'ng_vehicle_class',
                     'cng_vehicle_class',
                     'lng_vehicle_class']

# Trash columns to be deleted in main.py
trash_columns = ['id',
                 'intersection_directions',
                 'station_phone',
                 'open_date',
                 'expected_date',
                 'access_days_time',
                 'cards_accepted',
                 'geocode_status',
                 'date_last_confirmed',
                 'federal_agency_code',
                 'federal_agency_name',
                 'access_detail_code',
                 'restricted_access',
                 'nps_unit_name',
                 'bd_blends',
                 'hydrogen_status_link',
                 'ev_pricing',
                 'ev_other_info',
                 'intersection_directions_(french)',
                 'access_days_time_(french)',
                 'bd_blends_(french)',
                 'groups_with_access_code_(french)',
                 'ev_pricing_(french)',
                 'rd_blends_(french)']

# Columns to check for null values and replace with str Unknown in main.py
replace_null_col_value = ['access_code', 
                          'station_address']

# Columns to be converted to datetime in main.py
convert_to_datetime = ['updated_at']

# Column to convert to int in main.py
convert_to_int_cng = ['cng_total_compression_capacity',
                      'cng_storage_capacity',
                      'cng_dispenser_num',
                      'cng_psi']

# Column to convert to int in main.py
convert_to_int_rd = ['rd_maximum_biodiesel_level']

# Values to map stations status_code column in main.py
status_map = {'E': 'Available',
              'P': 'Planned',
              'T': 'Temporarily Unavailable'}

# Values to map stations owner_type_code column in main.py
owner_type_map = {'FG': 'Federal Government Owned',
                  'J': 'Jointly Owned',
                  'LG': 'Local/Municipal Government Owned',
                  'P': 'Privately Owned',
                  'SG': 'State/Provincial Government Owned',
                  'T': 'Utility Owned'}

# Values to map stations fuel_type_coded column in main.py
fuel_type_map = {'BD': 'Biodiesel',
                 'CNG': 'Compressed Natural Gas',
                 'E85': 'Ethanol E85',
                 'ELEC': 'Electric',
                 'HY': 'Hydrogen',
                 'LNG': 'Liquefied Natural Gas',
                 'LPG': 'Liquefied Petroleum Gas',
                 'RD': 'Renewable Diesel'}

# Values to map stations facility_type column in main.py
facility_map = {'AIRPORT': 'Airport',
                'ARENA': 'Arena',
                'AUTO_REPAIR': 'Auto Repair Shop',
                'BANK': 'Bank',
                'B_AND_B': 'B&B',
                'BREWERY_DISTILLERY_WINERY': 'Brewery/Distillery/Winery',
                'CAMPGROUND': 'Campground',
                'CAR_DEALER': 'Car Dealer',
                'CARWASH': 'Carwash',
                'COLLEGE_CAMPUS': 'College Campus',
                'CONVENIENCE_STORE': 'Convenience Store',
                'CONVENTION_CENTER': 'Convention Center',
                'COOP': 'Co-Op',
                'FACTORY': 'Factory',
                'FED_GOV': 'Federal Government',
                'FIRE_STATION': 'Fire Station',
                'FLEET_GARAGE': 'Fleet Garage',
                'FUEL_RESELLER': 'Fuel Reseller',
                'GROCERY': 'Grocery Store',
                'HARDWARE_STORE': 'Hardware Store',
                'HOSPITAL': 'Hospital',
                'HOTEL': 'Hotel',
                'INN': 'Inn',
                'LIBRARY': 'Library',
                'MIL_BASE': 'Military Base',
                'MOTOR_POOL': 'Motor Pool',
                'MULTI_UNIT_DWELLING': 'Multi-Family Housing',
                'MUNI_GOV': 'Municipal Government',
                'MUSEUM': 'Museum',
                'NATL_PARK': 'National Park',
                'OFFICE_BLDG': 'Office Building',
                'OTHER': 'Other',
                'OTHER_ENTERTAINMENT': 'Other Entertainment',
                'PARK': 'Park',
                'PARKING_GARAGE': 'Parking Garage',
                'PARKING_LOT': 'Parking Lot',
                'PAY_GARAGE': 'Pay-Parking Garage',
                'PAY_LOT': 'Pay-Parking Lot',
                'PHARMACY': 'Pharmacy',
                'PLACE_OF_WORSHIP': 'Place of Worship',
                'PRISON': 'Prison',
                'PUBLIC': 'Public',
                'REC_SPORTS_FACILITY': 'Recreational Sports Facility',
                'REFINERY': 'Refinery',
                'RENTAL_CAR_RETURN': 'Rental Car Return',
                'RESEARCH_FACILITY': 'Research Facility/Laboratory',
                'RESTAURANT': 'Restaurant',
                'REST_STOP': 'Rest Stop',
                'RETAIL': 'Retail',
                'RV_PARK': 'RV Park',
                'SCHOOL': 'School',
                'GAS_STATION': 'Service/Gas Station',
                'SHOPPING_CENTER': 'Shopping Center',
                'SHOPPING_MALL': 'Shopping Mall',
                'STADIUM': 'Stadium',
                'STANDALONE_STATION': 'Standalone Station',
                'STATE_GOV': 'State/Provincial Government',
                'STORAGE': 'Storage Facility',
                'STREET_PARKING': 'Street Parking',
                'TNC': 'Transportation Network Company',
                'TRAVEL_CENTER': 'Travel Center',
                'TRUCK_STOP': 'Truck Stop',
                'UTILITY': 'Utility',
                'WORKPLACE': 'Workplace'}

# Values to map stations maximum_vehicle_class column in main.py
maximum_vehicle_map = {'LD': 'Light Duty Vehicle',
                       'MD': 'Medium Duty Vehicle',
                       'HD': 'Heavy Duty Vehicle'}

# Values to map stations fuel_type_coded column in main.py
fuel_code_map = {'Biodiesel': 'bd',
                 'Compressed Natural Gas': 'cng',
                 'Ethanol E85': 'e85',
                 'Electric': 'ev',
                 'Hydrogen': 'hydrogen',
                 'Liquefied Natural Gas': 'lng',
                 'Liquefied Petroleum Gas': 'lpg',
                 'Renewable Diesel': 'rd'}

# Values to map cng_fill_type_code column in main.py
cng_fill_map =  {'All': 'All',
                 'B': 'Fast-fill and time-fill',
                 'Q': 'Fast-fill',
                 'T': 'Time-fill'}

# Values to map cng_renewable_source column in main.py
cng_renewable_map = {'GEOTHERMAL': 'Geothermal',
                     'HYDRO': 'Hydropower',
                     'LANDFILL': 'Landfill',
                     'LIVESTOCK': 'Livestock Operations',
                     'NONE': 'None',
                     'SOLAR': 'Solar',
                     'WASTEWATER': 'Wastewater Treatment',
                     'WIND': 'Wind'}

# Values to map stations rd_blended_with_biodiesel column in main.py
rd_blended_map = {'Y': 'False',
                  'N': 'True'}

# Final column order to be apllied in main.py
reorder_columns = ['station_name',
                   'fuel_type',
                   'station_address',
                   'station_location',
                   'facility',
                   'owner_type',
                   'maximum_class',
                   'access_code',
                   'bd_blends',	
                   'ev_level1_evse_num',	
                   'ev_level2_evse_num',	
                   'ev_dc_fast_count',
                   'ev_network',
                   'hydrogen_status_link',
                   'lpg_primary',
                   'e85_blender_pump',	
                   'ev_connector_types',	
                   'hydrogen_is_retail',	
                   'cng_dispenser_num	',
                   'cng_on-site_renewable_source',	
                   'cng_total_compression_capacity',	
                   'cng_storage_capacity',
                   'cng_dispenser_num',
                   'lng_on-site_renewable_source',	
                   'e85_other_ethanol_blends',
                   'ev_pricing',
                   'lpg_nozzle_types',
                   'hydrogen_pressures',
                   'hydrogen_standards',
                   'cng_fill_type_code',
                   'cng_psi',
                   'ev_on-site_renewable_source',
                   'rd_blends',
                   'rd_blended_with_biodiesel',
                   'rd_maximum_biodiesel_level',
                   'cng_station_sells_renewable_natural_gas',
                   'lng_station_sells_renewable_natural_gas',
                   'ev_workplace_charging',
                   'ev_other_info',
                   'ev_network_web'
                   'status',
                   'updated_at']

# List to order final cng df columns in main.py
reorder_columns_cng = ['ID',
                       'station_name',
                       'fuel_type',
                       'station_address',
                       'station_location',
                       'facility',
                       'owner_type',
                       'maximum_class', 
                       'access_code',
                       'cng_total_compression_capacity', 
                       'cng_storage_capacity', 
                       'cng_dispenser_num',
                       'cng_psi',
                       'cng_station_sells_renewable_natural_gas',
                       'cng_fill_type',
                       'cng_renewable_source',
                       'updated_at']

reorder_columns_ev = ['ID',
                      'station_name',
                      'fuel_type',
                      'station_address',
                      'station_location',
                      'facility',
                      'owner_type',
                      'maximum_class',
                      'access_code',
                      'ev_level1_evse_num',
                      'ev_level2_evse_num',
                      'ev_dc_fast_count',
                      'ev_network',
                      'ev_connector_types',
                      'ev_workplace_charging',
                      'ev_renewable_source',
                      'updated_at']

# List to order final lng df columns in main.py
reorder_columns_lng = ['ID',
                       'station_name',
                       'fuel_type',
                       'station_address',
                       'station_location',
                       'facility',
                       'owner_type',
                       'maximum_class',
                       'access_code',
                       'lng_station_sells_renewable_natural_gas',
                       'lng_renewable_source',
                       'updated_at']

# List to order final rd df columns in main.py
reorder_columns_rd = ['ID',
                      'station_name',
                      'fuel_type',
                      'station_address',
                      'station_location',
                      'facility',
                      'owner_type',
                      'maximum_class',
                      'access_code',
                      'rd_blends',
                      'rd_maximum_biodiesel_level',
                      'rd_blended_biodiesel',
                      'updated_at',]
