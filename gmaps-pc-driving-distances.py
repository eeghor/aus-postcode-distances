import schedule
import time
import googlemaps
import pandas as pd

d = pd.read_csv("auspost_postcodes/aus_subs.txt")
print("total postcodes: {}".format(len(d)))

d["destinations"] = d["sub"] + ' ' + d["state"] + ' ' + d["postcode"].map(str)

dest_lst = d["destinations"].tolist()

N_PCODES = len(dest_lst)
N_DIST = N_PCODES*(N_PCODES - 1) - sum(range(1, N_PCODES + 1) - 1)

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

		return ORIG_DONE
	
	else:

		return -1

schedule.every().day.at("10:30").do(collect_distances)

while True:
    schedule.run_pending()
    time.sleep(1)
