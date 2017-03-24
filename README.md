# aus-postcode-distances
Get driving distances between Australian postcodes from Google Maps.
- Due to the limit of 2,500 free requests per day imposed by the [Standard Google Maps Geocoding API](https://developers.google.com/maps/documentation/geocoding/usage-limits) we will use [schedule](https://github.com/dbader/schedule) to collect distances by batches over some days
