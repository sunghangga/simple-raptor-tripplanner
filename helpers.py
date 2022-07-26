import geopandas as gpd
from copy import copy
from typing import List, Dict, Any

# assume all xfers are 3 minutes
TRANSFER_COST = (5 * 60)

def add_footpath_transfers(
    stop_ids: List[str],
    time_to_stops_orig: Dict[str, Any],
    stops_gdf: gpd.GeoDataFrame,
    transfer_cost=TRANSFER_COST,
) -> Dict[str, Any]:
    # prevent upstream mutation of dictionary
    time_to_stops = copy(time_to_stops_orig)
    stop_ids = list(stop_ids)

    # add in transfers to nearby stops
    for stop_id in stop_ids:
        stop_pt = stops_gdf.loc[stop_id].geometry

        # TODO: parameterize? transfer within .2 miles
        meters_in_miles = 1610
        qual_area = stop_pt.buffer(meters_in_miles/5)
        
        # get all stops within a short walk of target stop
        mask = stops_gdf.intersects(qual_area)

        # time to reach new nearby stops is the transfer cost plus arrival at last stop
        arrive_time_adjusted = time_to_stops[stop_id] + TRANSFER_COST

        # only update if currently inaccessible or faster than currrent option
        for arrive_stop_id, row in stops_gdf[mask].iterrows():
            if arrive_stop_id in time_to_stops:
                if time_to_stops[arrive_stop_id] > arrive_time_adjusted:
                    time_to_stops[arrive_stop_id] = arrive_time_adjusted
            else:
                time_to_stops[arrive_stop_id] = arrive_time_adjusted
    
    return time_to_stops

def get_trip_ids_for_stop(feed, stop_id: str, departure_time: int):
    """Takes a stop and departure time and get associated trip ids."""
    # find stop time based on stop id and >= departure time
    mask_1 = feed.stop_times.stop_id == stop_id
    mask_2 = feed.stop_times.departure_time >= departure_time
    
    # extract the list of qualifying trip ids
    potential_trips = feed.stop_times[mask_1 & mask_2].trip_id.unique().tolist()
    return potential_trips

def stop_times_for_kth_trip(
    feed,
    departure_secs,
    from_stop_id: str,
    stop_ids: List[str],
    time_to_stops_orig: Dict[str, Any],
) -> Dict[str, Any]:
    # prevent upstream mutation of dictionary
    time_to_stops = copy(time_to_stops_orig)
    stop_ids = list(stop_ids)

    for i, ref_stop_id in enumerate(stop_ids):
        # how long it took to get to the stop so far (0 for start node)
        baseline_cost = time_to_stops[ref_stop_id]

        # get list of all trips associated with this stop
        potential_trips = get_trip_ids_for_stop(feed, ref_stop_id, departure_secs)
        for potential_trip in potential_trips:

            # get all the stop time arrivals for that trip and sort by sequence
            stop_times_sub = feed.stop_times[feed.stop_times.trip_id == potential_trip]
            stop_times_sub = stop_times_sub.sort_values(by="stop_sequence")

            # get the "hop on" point
            # get stop id details in stop_times_sub equals to ref_stop_id
            from_her_subset = stop_times_sub[stop_times_sub.stop_id == ref_stop_id]
            from_here = from_her_subset.head(1).squeeze()

            # get all following stops with condition: >= stop sequence and journey type is departure
            stop_times_after_mask = stop_times_sub.stop_sequence >= from_here.stop_sequence
            stop_times_after = stop_times_sub[stop_times_after_mask]

            # for all following stops, calculate time to reach
            ''' zip function is like:
            a = ("1", "2", "3")
            b = ("a", "b", "c", "d")
            x = zip(a, b)
            >> (('1', 'a'), ('2', 'b'), ('3', 'c'))
            '''
            arrivals_zip = zip(stop_times_after.arrival_time, stop_times_after.stop_id)
            for arrive_time, arrive_stop_id in arrivals_zip:
                
                # time to reach is diff from start time to arrival (plus any baseline cost)
                arrive_time_adjusted = arrive_time - departure_secs + baseline_cost
            
                # only update if does not exist yet or is faster
                if arrive_stop_id in time_to_stops: # doesn't exist yet
                    if time_to_stops[arrive_stop_id] > arrive_time_adjusted:
                        time_to_stops[arrive_stop_id] = arrive_time_adjusted
                else: # it's faster
                    time_to_stops[arrive_stop_id] = arrive_time_adjusted

    return time_to_stops