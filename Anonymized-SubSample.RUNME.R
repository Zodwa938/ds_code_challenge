#Install Required Packages
install.packages("sf")
install.packages("dplyr")
install.packages("lubridate")
install.packages("readr")
install.packages("logger")

# Load Requred Packages
library(sf)          # for spatial data handling
library(dplyr)       # for data wrangling
library(lubridate)   # for datetime manipulation
library(readr)       # for writing CSV
library(logger)      # for logging

# Logging start
log_info("Starting anonymization script at {Sys.time()}")

# Step 1: Read the GeoJSON data
input_file <- "C:/Users/ntomb/OneDrive/Desktop/DATA_CHALLENGE_SOLUTIONS/Question3/Question3.2/outputs/sr_bellville_with_wind.geojson"
data <- st_read(input_file, quiet = TRUE)

# Step 2: Drop personally identifiable columns
drop_columns <- c( "address", "reference_number")
data_clean <- data %>% select(-any_of(drop_columns))

# Step 3: Round latitude and longitude coordinates to ~500m accuracy
# Approx 0.005 degrees of lat/lon ≈ 500 meters
coords <- st_coordinates(data_clean)
data_clean$lat_round <- round(coords[, "Y"], 3)  # ~0.001 = 111m, so 0.003–0.005 is ≈ 300–500m
data_clean$lon_round <- round(coords[, "X"], 3)

# Drop original geometry and replace with rounded coordinate points
data_anonymized <- data_clean %>%
  st_drop_geometry() %>%
  st_as_sf(coords = c("lon_round", "lat_round"), crs = 4326, remove = FALSE)

# Step 4: Round timestamps to the nearest 6-hour block
# This reduces temporal precision while preserving utility
data_anonymized <- data_anonymized %>%
  mutate(creation_timestamp = ymd_hms(creation_timestamp),
         creation_timestamp_6h = floor_date(creation_timestamp, unit = "6 hours"))

# Step 5: Clean column names and drop now-unnecessary columns
final_data <- data_anonymized %>%
  select(-geometry, -creation_timestamp) %>%
  rename(timestamp = creation_timestamp_6h)

# Step 6: Write the anonymized dataset to a separate file
output_path <- "C:/Users/ntomb/OneDrive/Desktop/DATA_CHALLENGE_SOLUTIONS/Question3/Question3.3/sr_bellville_anonymized_v2.geojson"

st_write(final_data, output_path, driver = "GeoJSON", delete_dsn = TRUE, quiet = TRUE)

log_info("Anonymization complete. Output saved to {output_path}")

log_info("Anonymization complete. Output saved to {output_path}")
