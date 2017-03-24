import schedule
import time
import googlemaps
import pandas as pd

d = pd.read_csv("auspost_postcodes/aus_subs.txt")
print("total postcodes: {}".format(len(d)))

d["destinations"] = d["sub"] + ' ' + d["state"] + ' ' + d["postcode"].map(str)

dest_lst = d["destinations"].tolist()

gmaps = googlemaps.Client(key="AIzaSyCsJnOb6VESNe9C-BXpkbrLppPA2ygCJMg")

DAILY_CMAX = 2499

# function to collect distances; we'd like to run it with the scheduler
# def collect_distances():
p = dest_lst[-1]

distances = googlemaps.distance_matrix(origins=[dest_lst.pop()], destinations=dest_lst, units="metric", mode="driving")


# schedule.every().day.at("10:30").do(collect_distances)

# while True:
#     schedule.run_pending()
#     time.sleep(1)
