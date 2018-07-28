def return_min_max( fn, lat, lon, variable ):
    ''' return min/max WRF output T2 data '''
    print('working with {} starting'.format( fn ))
    ds = xr.open_dataset( fn, autoclose=True )
    da = ds[ variable ] # get the array as it is easier to process on

    # try to slice it...
    da_pt = da.where((da.lat == lat) )

    #fill the dataframe with min and max value with kelvin to C conversion
    day_min = da_pt.resample(time='D').min() - 273.15
    day_max = da_pt.resample(time='D').max() - 273.15

    df = pd.DataFrame( index=day_min.time )

    df['min'] = day_min
    df['max'] = day_max

    # return dataframe rounded at 2 decimals --> SNAP Temperature standard
    return df.round( 2 )

def return_val( fn, row, col, variable ):
    ''' return min/max WRF output T2 data '''
    print('working with {} starting'.format( fn ))
    ds = xr.open_dataset( fn, autoclose=True )
    da = ds[ variable ] # get the array as it is easier to process on

    # try to slice it...
    da_pt = da[:,row, col]
    # day_min = da_pt.resample(time='D').min() - 273.15
    df = pd.DataFrame( {'mean':da_pt},index=da_pt.time )

    # return dataframe rounded at 2 decimals --> SNAP Temperature standard
    return df.round( 2 )

def closest_point( lon, lat, da ):
    ''' the spatial way to do the lookup aka the RIGHT WAY  :)'''
    from shapely.geometry import Point
    from shapely.ops import nearest_points, unary_union

    lons = da.lon.data.ravel()
    lats = da.lat.data.ravel()

    pts = unary_union([ Point(*i) for i in zip(lons, lats) ])
    pt = Point(lon, lat)

    # get the nearest point
    nearest = nearest_points(pt, pts)[1]
    return nearest.x, nearest.y

if __name__ == '__main__':
    import xarray as xr
    import os, glob
    import numpy as np
    import pandas as pd
    from multiprocessing import Pool
    from functools import partial
    import warnings
    from affine import Affine
    import geopandas as gpd
    from shapely.geometry import Point

    warnings.filterwarnings( 'ignore' ) # so we dont have to look at xarray warnings about pd.TimeGrouper

    location = {
            'Fairbanks' : ( -147.71, 64.83 ),
            'Greely' : ( -145.6076, 63.8858 ),
            'Whitehorse' : ( -135.0568 , 60.7212 ),
            'Coldfoot' : ( -150.1772 , 67.2524 )
            }

    # reproject the points to the 3338...
    geom = [Point(j) for i,j in location.items()]
    df = pd.DataFrame(location).T
    df.columns = ['lon', 'lat']
    df['geometry'] = geom
    pts_proj = gpd.GeoDataFrame(df, crs={'init':'epsg:4326'}).to_crs( '+proj=stere +lat_0=90 +lat_ts=90 +lon_0=-150 +k=0.994 +x_0=2000000 +y_0=2000000 +datum=WGS84 +units=m +no_defs' )
    pts_proj['lat'] = pts_proj.geometry.apply( lambda x: x.x )
    pts_proj['lon'] = pts_proj.geometry.apply( lambda x: x.y )

    location = {}
    for j,i in pts_proj.iterrows():
        locations[j] = (i['lon'], i['lat'])

    # global setup arguments
    variable = 'T2'

    path = '/workspace/Shared/Tech_Projects/wrf_data/project_data/wrf_data/hourly_fix/{}'.format( variable.lower() )
    wildcard = '*GFDL*.nc'

    for k, v in location.items() :

        out_fn = '/workspace/Shared/Tech_Projects/wrf_data/project_data/app_data_extraction/WRF_extract_GFDL_1970-2100_{}.csv'.format( k )
        lat,lon = v
        # list the data we want to extract from
        files = sorted( glob.glob( os.path.join( path, wildcard ) ) )

        # grab lat/lon from a single file
        tmp_ds = xr.open_dataset( files[0], autoclose=True )
        a = Affine( 20000, 0, tmp_ds.yc.min(), 0, -20000, tmp_ds.xc.max() )
        x,y = v
        col, row = ~a * (x, y)
        col, row = [ np.round(int(i),0) for i in [col,row] ]
        
        # lon, lat = closest_point( lon, lat, tmp_ds ) # update to the closest
        
        # close and cleanup -- the hard way to avoid memory leaks
        tmp_ds.close()
        tmp_ds = None

        # run in parallel
        pool = Pool( 32 )
        func = partial( return_val, row=row, col=col, variable=variable.lower() )
        ls_df = pool.map( func, files )
        pool.close()
        pool.join()

        # concat and write to disk
        df = pd.concat( ls_df )
        df = df.sort_index()
        df.to_csv( out_fn )
