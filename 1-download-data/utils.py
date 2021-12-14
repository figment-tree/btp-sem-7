import ee
from joblib import delayed, wrap_non_picklable_objects
import time


def get_districts():
    # Get a feature collection of level 2 administrative boundaries.
    all_districts = ee.FeatureCollection(
        "FAO/GAUL/2015/level2").select('ADM0_NAME', 'ADM1_NAME', 'ADM2_NAME')
    # Filter out districts from India only and retain just the state_name &
    # district names for further processing.
    all_districts = all_districts.filter(ee.Filter.eq('ADM0_NAME', 'India')) \
                                 .select('ADM1_NAME', 'ADM2_NAME')

    return all_districts


def append_band(current, previous):
    # Rename the band
    previous = ee.Image(previous)
    # Append it to the result
    # (Note: only return current item on first element/iteration)
    accum = ee.Algorithms.If(ee.Algorithms.IsEqual(
        previous, None), current, previous.addBands(ee.Image(current)))
    # Return the accumulation
    return accum


def export_image(image, file_name, folder_name, scale=30, crs='EPSG:4326'):
    task = ee.batch.Export.image.toDrive(image=image,
                                         scale=scale,
                                         folder=folder_name,
                                         fileNamePrefix=file_name,
                                         description="BTP 7th sem",
                                         crs=crs,
                                         fileFormat='GeoTIFF')
    task.start()
    while task.status()['state'] == 'READY':
        time.sleep(20)
    if (task.status()['state'] == 'FAILED'):
        print(f'Failed to export {file_name}.')
    print(f'Exporting {file_name}.')


@delayed
@wrap_non_picklable_objects
def export_state_data(image, folder_name, all_districts, state_name):
    # Filter the feature collection to subset this state_name.
    state_districts = all_districts.filter(
        ee.Filter.eq('ADM1_NAME', state_name))

    # Extract the features from the feature collection.
    state_districts = state_districts.getInfo()['features']

    for district in state_districts:
        names = district["properties"]
        # Make file name of format state_district.
        file_name = f'{names["ADM1_NAME"]}_{names["ADM2_NAME"]}'
        image_region = district['geometry']

        scale = 500

        while True:
            try:
                export_image(image.clip(image_region), file_name,
                             folder_name, scale)
            except ee.ee_exception.EEException:
                print(f'Retry {file_name}')
                time.sleep(10)
                continue
            break
