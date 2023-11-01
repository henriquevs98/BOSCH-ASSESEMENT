# Trash columns to be deleted in main.py
trash_columns = ['odiNumber',
                 'vin',
                 'dateOfIncident',
                 'summary',
                 'manufacturer',
                 'components',
                 'size']

# Date columns that need to be fixed in main.py
fix_date = ['dateComplaintFiled']

# String columns that need to be fixed in main.py
fix_capitalize = ['productMake',
                  'productModel',
                  'manufacturer_product']

# Columns to be converted to datetime in main.py
convert_to_datetime = ['dateComplaintFiled']

# Columns to be converted to int in main.py
convert_to_int = ['numberOfInjuries',
                  'numberOfDeaths',
                  'complaintYear',
                  'productYear']

# Final column order to be apllied in main.py
reorder_columns = ['complaintYear',
                   'dateComplaintFiled',
                   'type',
                   'manufacturer_product',
                   'productMake',
                   'productModel',
                   'productYear',
                   'numberOfInjuries',
                   'numberOfDeaths',
                   'crash',
                   'fire']