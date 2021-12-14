import ee
from functools import partial
from joblib import Parallel
from utils import get_districts, append_band, export_state_data


if __name__ == '__main__':
    # Uncomment line 9 when running on colab.
    # ee.Authenticate()
    ee.Initialize()

    state_names = ["Gujarat", "Bihar", "Haryana", "Madhya Pradesh",
                   "Uttar Pradesh", "Rajasthan", "Punjab"]

    # Get all the districts of India.
    all_districts = get_districts()

    start_date = '2005-10-1'
    end_date = '2019-12-31'

    lst_image_collection = ee.ImageCollection("MODIS/006/MYD11A2") \
                             .select('LST_Day_1km', 'LST_Night_1km') \
                             .filterDate(start_date, end_date)

    # Convert the whole image collection into one large image for storage.
    lst_image = lst_image_collection.iterate(append_band)
    lst_image = ee.Image(lst_image)

    # Use all cores except 1.
    no_of_cores = -2
    folder_name = 'land surface temperature'
    export_state_lst_data = partial(
        export_state_data, lst_image, folder_name, all_districts)

    Parallel(n_jobs=no_of_cores, backend='multiprocessing')(
        export_state_lst_data(state_name) for state_name in state_names)
