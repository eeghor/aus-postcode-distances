import schedule
import time
import googlemaps
import pandas as pd

d = pd.read_csv("auspost_postcodes/aus_subs.txt")
print("total postcodes: {}".format(len(d)))

d["destinations"] = d["sub"] + ' ' + d["state"] + ' ' + d["postcode"].map(str)

dest_lst = d["destinations"].tolist()

gmaps = googlemaps.Client(key="AIzaSyCsJnOb6VESNe9C-BXpkbrLppPA2ygCJMg")

TOTAL = len(dest_lst) - 1
DAILY_CMAX = 2500
SOFAR = 0
SOFAR_THIS_ORG = 0
TOTAL_THIS_ORG = TOTAL  # initially, all other postcodes but the last one

# function to collect distances; we'd like to run it with the scheduler
def collect_distances():

	if SOFAR < TOTAL:  # not all distances collected yet
	
		if SOFAR_THIS_ORG + DAILY_CMAX < TOTAL_THIS_ORG:  # then keep collection for this origin
			org = dest_lst.pop()
			dest = dest_lst[SOFAR_THIS_ORG:SOFAR_THIS_ORG + DAILY_CMAX]	
			res = googlemaps.distance_matrix(origins=org, destinations=dest, units="metric", mode="driving")
			SOFAR_THIS_ORG += DAILY_CMAX
			SOFAR += DAILY_CMAX  # say started from 0, now 2500
		else: 
			NXTORG = TOTAL_THIS_ORG - (SOFAR_THIS_ORG + DAILY_CMAX)  # this many can do for the next origin
			dest = dest_lst[SOFAR_THIS_ORG:TOTAL_THIS_ORG]  # finish the rest for this origin
			res = googlemaps.distance_matrix(origins=org, destinations=dest, units="metric", mode="driving")
			# switch to the next origin
			SOFAR += (TOTAL_THIS_ORG - SOFAR_THIS_ORG)
			# reser sofar for the nex torigin
			SOFAR_THIS_ORG = 0
			org = dest_lst.pop()
			TOTAL_THIS_ORG = len(dest_lst)
			
			dest = dest_lst[SOFAR_THIS_ORG:SOFAR_THIS_ORG + NXTORG]
			res = googlemaps.distance_matrix(origins=org, destinations=dest, units="metric", mode="driving")
		return SOFAR
	else:

		return -1

	p = dest_lst[-1]
	# schedule.every().day.at("10:30").do(collect_distances)

# while True:
#     schedule.run_pending()
#     time.sleep(1)
