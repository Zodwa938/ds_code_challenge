# Title: Bellville South 1-Arcminute Subsample
# Spatial filtering with unit conversion

#Install Required Packages
install.packages("sf")
install.packages("dplyr")
install.packages("units")

#Load/Activate Packages
library(sf)
library(dplyr)
library(units)

# Configuration
SR_DATA_URL <- "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr_hex.csv.gz"
CENTROID_LON <- 18.642
CENTROID_LAT <- -33.928
TOLERANCE_KM <- 1.85  # 1 arc-minute ≈ 1.85 km at this latitude

#Increase R timeout from 1minute to 5minutes
options(timeout = 300)

# 1. Load and prepare data
sr_data <- read_csv(SR_DATA_URL) %>%
  filter(!is.na(longitude), !is.na(latitude)) %>%
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326)

# 2. Create centroid and buffer
centroid <- st_sfc(st_point(c(CENTROID_LON, CENTROID_LAT)), crs = 4326)
buffer <- st_buffer(centroid, dist = TOLERANCE_KM * 1000)  # meters

# 3. Spatial filter with proper distance calculation
final_subsample <- sr_data %>%
  mutate(
    distance_km = st_distance(., centroid) %>%
      units::set_units("km") %>%
      as.numeric()
  ) %>%
  filter(distance_km <= TOLERANCE_KM)

# 4. Save and verify
st_write(final_subsample, "bellville_south_1arcmin_filtered.geojson")

cat("Final request count:", nrow(final_subsample), "\n")
print(summary(final_subsample$distance_km))

# Visual verification
plot(st_geometry(buffer), main = paste("1.85 km Filter Zone (≈1 arc-minute)\n",
                                       nrow(final_subsample), "requests"))
plot(st_geometry(final_subsample), add = TRUE, col = "red", pch = 16, cex = 0.5)
plot(centroid, add = TRUE, col = "blue", pch = 4, cex = 2, lwd = 2)

