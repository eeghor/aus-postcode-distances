import schedule
import time
import googlemaps
import pandas as pd
import sys

## s e t t i n g s

DAILY_CMAX = 2499

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
venue_pcodes = set(v.postcode.unique())
NVENUS = len(venue_pcodes)
print("unique venue postcodes:", NVENUS)

## t a r g e t s

NDIST = NORIGS*NVENUS - len(pcs & venue_pcodes)
print("distances to be collected: {}".format(NDIST))
print("days needed to collect: {:.1f}".format(NDIST/DAILY_CMAX))

sys.exit(0)

# initiate API

gmaps = googlemaps.Client(key="AIzaSyCsJnOb6VESNe9C-BXpkbrLppPA2ygCJMg")

ORIG_DONE = 0
ORIG_TODO = 0
SOFAR_THIS_ORG = 0

# function to collect distances; we'd like to run it with the scheduler
def collect_distances():

	global ORIG_DONE, SOFAR_THIS_ORG, ORIG_TODO

	if ORIG_DONE < ORIG_TODO:  # not all distances done yet
			
		if SOFAR_THIS_ORG + DAILY_CMAX < ORIG_TODO:  # then keep collecting for this origin
			
			dest = dest_lst[SOFAR_THIS_ORG:SOFAR_THIS_ORG + DAILY_CMAX]
			
			res = googlemaps.distance_matrix(origins=org, destinations=dest, units="metric", mode="driving")

			SOFAR_THIS_ORG += DAILY_CMAX

		else: 
			
			dest = dest_lst[SOFAR_THIS_ORG:TOCOLLECT]  # finish the rest for this origin
			
			res = googlemaps.distance_matrix(origins=org, destinations=dest, units="metric", mode="driving")
			
			# switch to the next origin
			ORIG_DONE += 1
			
			org = dest_lst.pop()
			SOFAR_THIS_ORG = 0
			TOCOLLECT = len(dest_lst) - 1
			
			dest = dest_lst[SOFAR_THIS_ORG: TOCOLLECT - DAILY_CMAX]
			res = googlemaps.distance_matrix(origins=org, destinations=dest, units="metric", mode="driving")

		print
	
	else:

		return -1

schedule.every().day.at("10:30").do(collect_distances)

while True:
    schedule.run_pending()
    time.sleep(1)
