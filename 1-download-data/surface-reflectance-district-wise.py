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

    sr_image_collection = ee.ImageCollection("MODIS/006/MOD09A1") \
                            .filterDate(start_date, end_date)

    sr_image = sr_image_collection.iterate(append_band)
    sr_image = ee.Image(sr_image)

    # Split the surface relectance bands into 2 separate images since there
    # are a lot of bands and the data might be too large to fit into a single
    # image for large districts.
    sr_narrowband_image = sr_image.select([0, 1, 2, 3])
    sr_wideband_image = sr_image.select([4, 5, 6])

    # Use all cores except 1.
    no_of_cores = -2
    export_state_sr_narrowband_data = partial(export_state_data,
                                              sr_narrowband_image,
                                              'surface reflectance 1',
                                              all_districts)
    export_state_sr_wideband_data = partial(export_state_data,
                                            sr_wideband_image,
                                            'surface reflectance 2',
                                            all_districts)

    Parallel(n_jobs=no_of_cores, backend='multiprocessing')(
        export_state_sr_narrowband_data(state_name)
        for state_name in state_names)
    Parallel(n_jobs=no_of_cores, backend='multiprocessing')(
        export_state_sr_wideband_data(state_name)
        for state_name in state_names)
