"""
note: to avoid misinterpretations, we need to give Google Maps the following:
	  [suburb] [state] [postcode], e.g. "Narrabundah ACT 2604"

note: distance_matrix returns dictionaries; for example, 

origin: 		Moutajup VIC 3294
destinations:  Sydney NSW 2000, Narrabundah ACT 2604

result:
 	{'destination_addresses': ['Sydney NSW 2000, Australia', 'Narrabundah ACT 2604, Australia'], 
 		'origin_addresses': ['3001 Victoria Point Rd, Moutajup VIC 3294, Australia'], 
 		'rows': [{'elements': [{'distance': {'text': '1,092 km', 'value': 1092052}, 
 		'duration': {'text': '11 hours 14 mins', 'value': 40428}, 'status': 'OK'}, 
 		{'distance': {'text': '885 km', 'value': 884851}, 'duration': {'text': '9 hours 21 mins', 'value': 33666}, 
 		'status': 'OK'}]}], 'status': 'OK'}
"""

import schedule
import time
import googlemaps
import pandas as pd
import sys
from collections import defaultdict
import json

## s e t t i n g s

DAILY_CMAX = 30

## p o s t c o d e s

d = pd.read_csv("auspost_postcodes/aus_subs.txt", dtype={"postcode": str})
print("total suburb-postcode pairs: {}".format(len(d)))
# many suburbs share the same postcode
d = d.drop_duplicates("postcode")
print("unique postcodes: {}".format(len(d.postcode)))
# make a list of state - postcode records (MSW-ACT only) to feed into Google Maps
d = d.loc[d.loc[:,"state"].isin(["NSW","ACT"]),:]
ogs = (d["sub"] + ' ' + d["state"] + ' ' + d["postcode"]).tolist()
NORIGS = len(ogs)
print("total origins from NSW-ACT: {}".format(NORIGS))
pcs = set(list(d.postcode.unique()))

## v e n u e s

# load best selling NSW-ACT venues (i.e. such that Ticketek sold at least 1,000 tickets, ever)
v = pd.read_csv("auspost_postcodes/venues_over_1K_trans.csv", dtype={"postcode": str})
print("important NSW-ACT venues: {}".format(len(v)))
# there may be come suplicate postcodes 
venue_pcodes = list(v.postcode.unique())
NVENUS = len(venue_pcodes)
print("unique venue postcodes:", NVENUS)

## t a r g e t s

NDIST = (len(pcs))*NVENUS - len(pcs & set(venue_pcodes))
print("distances to be collected: {}".format(NDIST))
print("days needed to collect: {:.1f}".format(NDIST/DAILY_CMAX))

# initiate API

gmaps = googlemaps.Client(key="AIzaSyCsJnOb6VESNe9C-BXpkbrLppPA2ygCJMg")


# create a dict that contains all venu postcodes relevant to a particular origin

to_collect = defaultdict(list)

for origin in pcs:
	for venue in venue_pcodes:
		if venue != origin:
			to_collect[origin].append(venue)

# dictionary to store how many venue postcodes have to be considered for each origin
collection_status = {o: {"todo": len(to_collect[o]), "done": 0} for o in to_collect}

#sys.exit(0)


leftover = 0
done_flag = 1

run_cnt = 0

res_dict = defaultdict(list)  # {"origin_1": [list of distance records],..}

# function to collect distances; we'd like to run it with the scheduler
def collect_distances():

	global leftover, done_flag

	global run_cnt

	run_cnt += 1

	print("running day ", run_cnt)

	TODAYS_LIMIT = leftover + DAILY_CMAX 

	print("todays limit is ", TODAYS_LIMIT)

	todays_file = "day_" + str(run_cnt) + ".json"

	print("will save to file ", todays_file)

	res = []

	for o in collection_status:
		
		left_todo = collection_status[o]["todo"] - collection_status[o]["done"]

		print("for origin {} left to do {}".format(o,left_todo))
		
		if left_todo > 0:

			done_flag = 0

			if left_todo <= TODAYS_LIMIT:

				print("will be collecting for vanues:", to_collect[o][-left_todo:])

				h = googlemaps.distance_matrix(origins=o, destinations=to_collect[o][-left_todo:], units="metric", mode="driving")
				print("h=",h)
				print(type(h))
				res.append(h)

				TODAYS_LIMIT -= left_todo
				print("todays limit is now", TODAYS_LIMIT)

				for v in to_collect[o][-left_todo:]:
					collection_status[o]["done"] += len(left_todo)

			else: # need to do more than the limit allows
							
				print("need to collect more than limit allows..")
				idx_ven = to_collect[o][collection_status[o]["done"]: collection_status[o]["done"] + TODAYS_LIMIT]
				print("idx_ven=",idx_ven)
				print("just maps:", googlemaps.distance_matrix(origins=o, destinations=idx_ven, units="metric", mode="driving"))
				h = googlemaps.distance_matrix(origins=o, destinations=idx_ven, units="metric", mode="driving")
				print("h=",h)
				print(type(h))
				res.append(h)
				leftover = left_todo - TODAYS_LIMIT
		else:

			continue

	with open(todays_file, "w") as f:
		json.dump(res, f)

	return done_flag


schedule.every().day.at("12:23").do(collect_distances)

while True:
    schedule.run_pending()
    time.sleep(1)
