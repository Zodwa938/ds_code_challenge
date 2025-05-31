# Join Bellville South Service Requests with Cleaned Wind Data

# Install and Load Required Libraries
packages <- c("sf", "dplyr", "lubridate", "logger", "readr","readODS","stringr" )
install_if_missing <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
}
lapply(packages, install_if_missing)
lapply(packages, library, character.only = TRUE)

log_info("Script started at {Sys.time()}")

# Load Data (Wind Data)
file_path <- "C:/Users/ntomb/OneDrive/Desktop/DATA_CHALLENGE_SOLUTIONS/Question3/Question3.2/Wind_2020.ods"
raw_wind <- read_ods(file_path, sheet = 1)

log_info("Loaded wind data: {nrow(raw_wind)} rows")

# Clean Wind Data
cleaned_wind <- raw_wind %>%
  rename_with(~ str_trim(.)) %>%
  rename_with(~ gsub("\\s+", " ", .)) %>%
  filter(`Air Quality Measurement Site` == "Bellville South AQM Site") %>%
  mutate(
    # Format Date and Time
    `Date and Time` = dmy_hm(`Date and Time`, quiet = TRUE),
    `Date and Time` = format(`Date and Time`, "%d/%m/%Y %H:%M"),
    
    # Coerce wind speed and direction to numeric
    `Wind Speed (m/s)` = as.numeric(`Wind Speed (m/s)`),
    `Wind Direction (degrees)` = as.numeric(`Wind Direction (degrees)`)
  )

log_info("Filtered for Bellville South AQM Site and formatted columns")

# Replace malformed or unconvertible values with NA
cleaned_wind <- cleaned_wind %>%
  mutate(
    `Date and Time` = ifelse(str_detect(`Date and Time`, "^\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2}$"),
                             `Date and Time`, NA),
    `Wind Speed (m/s)` = ifelse(is.na(`Wind Speed (m/s)`) | `Wind Speed (m/s)` < 0, NA, `Wind Speed (m/s)`),
    `Wind Direction (degrees)` = ifelse(is.na(`Wind Direction (degrees)`) |
                                          `Wind Direction (degrees)` < 0 |
                                          `Wind Direction (degrees)` > 360, NA, `Wind Direction (degrees)`)
  )

log_info("Applied NA to invalid rows")

# Save Cleaned Wind Data
output_path <- "cleaned_wind_2020.csv"
write.csv(cleaned_wind, output_path, row.names = FALSE)
log_info("Saved cleaned wind dataset to {output_path}")


# Load Datasets
# Service request data
sr_file <- "C:/Users/ntomb/OneDrive/Desktop/DATA_CHALLENGE_SOLUTIONS/Question3/Question3.1/bellville_south_1arcmin_filtered.geojson"
sr_data <- st_read(sr_file, quiet = TRUE)

# Wind data
wind_file <- "cleaned_wind_2020.csv"
wind_data <- read_csv(wind_file, show_col_types = FALSE)

log_info("Loaded {nrow(sr_data)} service request records and {nrow(wind_data)} wind records")


# Time Standardization
# Parse wind datetime to POSIXct
wind_data <- wind_data %>%
  mutate(`Date and Time` = dmy_hm(`Date and Time`)) %>%
  rename(wind_datetime = `Date and Time`)

# Round down service request creation time to nearest hour
sr_data <- sr_data %>%
  mutate(creation_timestamp = ymd_hms(creation_timestamp),
         sr_hour = floor_date(creation_timestamp, unit = "hour"))

# Join Datasets
joined_data <- sr_data %>%
  left_join(wind_data, by = c("sr_hour" = "wind_datetime"))


# Logging and Summary
missing_wind <- sum(is.na(joined_data$`Wind Speed (m/s)`))
threshold <- 0.05  # Allow up to 5% of join errors

log_info("Number of service requests without matching wind data: {missing_wind}")
log_info("Join error rate: {round(missing_wind / nrow(sr_data) * 100, 2)}%")

if (missing_wind / nrow(sr_data) > threshold) {
  stop("Join error rate exceeded threshold of 5% — aborting.")
}

# Save Result
output_file <- "sr_bellville_with_wind.geojson"
st_write(joined_data, output_file, delete_dsn = TRUE, quiet = TRUE)
log_info("Joined dataset saved to {output_file}")

log_info("Script finished at {Sys.time()}")
