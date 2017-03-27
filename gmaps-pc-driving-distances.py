import schedule
import time
import googlemaps
import pandas as pd
import sys

# look into postcodes
d = pd.read_csv("auspost_postcodes/aus_subs.txt", dtype={"postcode": str})
print("total suburbs with postcodes: {}".format(len(d)))
# many suburbs share the same postcode
d = d.drop_duplicates("postcode")
print("unique postcodes: {}".format(len(d.postcode)))
# load best selling NSW-ACT venues (i.e. such that Ticketek sold at least 1,000 tickets, ever)
v = pd.read_csv("auspost_postcodes/venues_over_1K_trans.csv", dtype={"postcode": str})
print("important NSW-ACT venues: {}".format(len(v)))
# make a list of venue postcodes
vns = v.postcode.tolist()
# make a list of state - postcode records (MSW-ACT only)
d = d.loc[d.loc[:,"state"].isin(["NSW","ACT"]),:]
ogs = (d["sub"] + ' ' + d["state"] + ' ' + d["postcode"]).tolist()
pcs = list(d.postcode.unique())
print("total origins from NSW-ACT: {}".format(len(ogs)))
psc_to_collect = set(pcs) - set(vns)
print("there are {} non-venue postcodes to be used as origins".format(len(psc_to_collect)))

NDIST = len(ogs)*len(vns) - len(psc_to_collect)
print("requests needed: {}".format(NDIST))
print("days to collect: {:.1f}".format(NDIST/2500))
sys.exit(0)



dest_lst = d["destinations"].tolist()

N_PCODES = 2870  #len(dest_lst)
N_DIST = N_PCODES*(N_PCODES - 1) - sum(range(1, N_PCODES ))

print("distances to collect: {}".format(N_DIST))
print("days needed to collect: {:.1f}".format(N_DIST/2500))



gmaps = googlemaps.Client(key="AIzaSyCsJnOb6VESNe9C-BXpkbrLppPA2ygCJMg")

DAILY_CMAX = 2500
ORIG_DONE = 0
SOFAR_THIS_ORG = 0

org = dest_lst.pop()
TOCOLLECT = N_PCODES - 1  # initially, all other postcodes but the last one
		
# function to collect distances; we'd like to run it with the scheduler
def collect_distances():

	global ORIG_DONE, SOFAR_THIS_ORG, TOCOLLECT

	if ORIG_DONE < N_DIST:  # not all origina done yet
			
		if SOFAR_THIS_ORG + DAILY_CMAX < TOCOLLECT:  # then keep collection for this origin
			
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
