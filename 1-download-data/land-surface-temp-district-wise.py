import ee
import time

# Uncomment line 5 when running on colab.
# ee.Authenticate()
ee.Initialize()


def export_image(image, name, folder='data', scale=30, crs='EPSG:4326'):
    task = ee.batch.Export.image.toDrive(image=image,
                                         scale=scale,
                                         folder=folder,
                                         fileNamePrefix=name,
                                         description="BTP 7th sem",
                                         crs=crs,
                                         fileFormat='GeoTIFF')
    task.start()
    while task.status()['state'] == 'RUNNING':
        print('Running...')
        time.sleep(10)
    print(f'Exported {name}.')


def appendBand(current, previous):
    # Rename the band
    previous = ee.Image(previous)
    current = current.select('LST_Day_1km', 'QC_Day')
    # Append it to the result
    # (Note: only return current item on first element/iteration)
    accum = ee.Algorithms.If(ee.Algorithms.IsEqual(
        previous, None), current, previous.addBands(ee.Image(current)))
    # Return the accumulation
    return accum


states = ["Gujarat", "Bihar", "Haryana", "Madhya Pradesh",
          "Uttar Pradesh", "Rajasthan", "Punjab"]

# Get a feature collection of level 2 administrative boundaries.
districts = ee.FeatureCollection(
    "FAO/GAUL/2015/level2").select('ADM1_NAME', 'ADM2_NAME')

india_bounding_box = ee.Geometry.Rectangle(
    66.093750, 7.841615, 98.261719, 36.914764)
start_date = '2005-1-1'
end_date = '2020-12-31'

lst_image_collection = ee.ImageCollection("MODIS/006/MYD11A2") \
                         .filterBounds(india_bounding_box) \
                         .filterDate(start_date, end_date)

image = lst_image_collection.iterate(appendBand)
image = ee.Image(image)

for state in states:
    # Filter the feature collection to subset this state.
    state_districts = districts.filter(ee.Filter.eq('ADM1_NAME', state))

    # Extract the features from the feature collection.
    state_districts = state_districts.getInfo()['features']

    for district in state_districts:
        names = district["properties"]
        # Make file name of format state_district.
        fname = f'{names["ADM1_NAME"]}_{names["ADM2_NAME"]}'
        region = district['geometry']

        scale = 500

        while True:
            try:
                export_image(image.clip(region), fname,
                             'land surface temperature', scale)
            except ee.ee_exception.EEException:
                print('Retry')
                time.sleep(10)
                continue
            break