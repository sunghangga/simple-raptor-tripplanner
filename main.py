import time
import partridge as ptg
import geopandas as gpd
import pyproj
from shapely.geometry import Point
from helpers import add_footpath_transfers, get_trip_ids_for_stop, stop_times_for_kth_trip

## STEP 1
# load a GTFS of AC Transit
path = 'gtfs.zip'
_date, service_ids = ptg.read_busiest_date(path)
view = {'trips.txt': {'service_id': service_ids}}
feed = ptg.load_feed(path, view)


## STEP 2
# convert all known stops in the schedule to shapes in a GeoDataFrame
gdf = gpd.GeoDataFrame(
    {"stop_id": feed.stops.stop_id.tolist()},
    geometry=[
        Point(lon, lat)
        for lat, lon in zip(
            feed.stops.stop_lat,
            feed.stops.stop_lon)])
gdf = gdf.set_index("stop_id")
gdf.crs = {'init': 'epsg:4326'}

# re-cast to meter-based projection to allow for distance calculations
aeqd = pyproj.Proj(
    proj='aeqd',
    ellps='WGS84',
    datum='WGS84',
    lat_0=gdf.iloc[0].geometry.centroid.y,
    lon_0=gdf.iloc[0].geometry.centroid.x).srs
gdf = gdf.to_crs(crs=aeqd)


## STEP 3
# let's use this example origin and destination
# to find the time it would take to go from one to another
from_stop_name = "BLANCO & DRESDEN"
to_stop_name = "BLANCO OPPOSITE BROOKSTONE" # MARTIN & N. FRIO

# QA: we know the best way to connect these two is the 51A -> 1T
# if we depart at 8:30 AM, schedule should suggest:
#     take 51A 8:37 - 8:49
#     make walk connection
#     take 1T 8:56 - 9:03
# total travel time: 26 minutes

# look at all trips from that stop that are after the depart time
departure_secs = 8.5 * 60 * 60

# get all information, including the stop ids, for the start and end nodes
from_stop = feed.stops[feed.stops.stop_name == from_stop_name].head(1).squeeze()
to_stop = feed.stops[["BLANCO OPPOSITE BROOKSTONE" in f for f in feed.stops.stop_name]].head(1).squeeze()

# extract just the stop ids
from_stop_id = from_stop.stop_id
to_stop_id = to_stop.stop_id


## STEP 4
# initialize lookup with start node taking 0 seconds to reach
time_to_stops = {from_stop_id: 0}

# setting transfer limit at 1
TRANSFER_LIMIT = 0
for k in range(TRANSFER_LIMIT + 1):
    print("\nAnalyzing possibilities with {} transfers".format(k))
    
    # generate current list of stop ids under consideration
    stop_ids = list(time_to_stops.keys())
    print("\tinital qualifying stop ids count: {}".format(len(stop_ids)))
    
    # update time to stops calculated based on stops accessible
    tic = time.perf_counter()
    time_to_stops = stop_times_for_kth_trip(feed, departure_secs, from_stop_id, stop_ids, time_to_stops)
    toc = time.perf_counter()
    print("\tstop times calculated in {:0.4f} seconds".format(toc - tic))

    added_keys_count = len((time_to_stops.keys())) - len(stop_ids)
    print("\t\t{} stop ids added".format(added_keys_count))
    
    # now add footpath transfers and update
    tic = time.perf_counter()
    stop_ids = list(time_to_stops.keys())
    time_to_stops = add_footpath_transfers(stop_ids, time_to_stops, gdf)
    toc = time.perf_counter()
    print("\tfootpath transfers calculated in {:0.4f} seconds".format(toc - tic))

    added_keys_count = len((time_to_stops.keys())) - len(stop_ids)
    print("\t\t{} stop ids added".format(added_keys_count))

print(time_to_stops)
exit()
assert to_stop_id in time_to_stops, "Unable to find route to destination within transfer limit"
print("Time to destination: {} minutes".format(time_to_stops[to_stop_id]/60))