# Install required packages
install.packages("sf")
install.packages("data.table")
install.packages("logger")
install.packages("geosphere")
install.packages("dplyr")

# Load/Activate required libraries
library(sf)
library(data.table)
library(logger)
library(geosphere)
library(dplyr)


# Increase R timeout from 1 minute to 5 minutes
options(timeout = 300)

# Configure logging
log_layout(layout_glue_colors)
log_threshold(INFO)

# Constants
ERROR_THRESHOLD <- 0.05  # 5% error threshold
H3_RESOLUTION <- 8
VALIDATION_SAMPLE_SIZE <- 1000  # Number of records to validate

main <- function() {
  # Start timer
  start_time <- Sys.time()
  log_info("Starting H3 spatial join process")
  
  tryCatch({
    ## 1. Load Datasets 
    log_info("Loading datasets...")
    
    # Load H3 polygons at resolution 8
    h3_polygons <- st_read(
      "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/city-hex-polygons-8.geojson", 
      quiet = TRUE
    )
    
    # Load service requests (without H3 indices)
    service_requests <- fread(
      "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr.csv.gz"
    )
    
    # Load validation data (with pre-calculated H3 indices)
    validation_data <- fread(
      "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr_hex.csv.gz"
    )
    
    log_info("Datasets loaded successfully")

    log_info("H3 polygons: {nrow(h3_polygons)} features")
    record_count <- format(nrow(service_requests), big.mark = ',')
    log_info("Service requests: {record_count} records")
    
    ## 2. Preprocess Data 
    log_info("Preprocessing data...")
    
    # Filter valid coordinates and convert to SF object
    valid_requests <- service_requests %>%
      filter(
        !is.na(latitude),
        !is.na(longitude),
        between(latitude, -90, 90),
        between(longitude, -180, 180)
      ) %>%
      st_as_sf(
        coords = c("longitude", "latitude"),
        crs = st_crs(h3_polygons)
      )
    
    invalid_requests <- service_requests %>%
      filter(
        is.na(latitude) | is.na(longitude) |
          !between(latitude, -90, 90) |
          !between(longitude, -180, 180)
      )
    
    log_info("{format(nrow(invalid_requests), big.mark = ',')} records with invalid coordinates will get index 0")
    
    ## 3. Spatial Join 
    log_info("Performing spatial join (this may take several minutes)...")
    join_start <- Sys.time()
    
    # Perform the spatial join
    joined_data <- st_join(
      valid_requests,
      h3_polygons,
      join = st_within
    ) %>%
      as.data.table()
    
    join_duration <- difftime(Sys.time(), join_start, units = "secs")
    log_info("Spatial join completed in {round(join_duration, 1)} seconds")
    
    ## 4. Handle Join Results 
    failed_joins <- sum(is.na(joined_data$index))
    total_attempted <- nrow(valid_requests)
    failure_rate <- failed_joins / total_attempted
    
    log_info("Join results: {failed_joins} failed joins out of {total_attempted} attempts")
    log_info("Failure rate: {scales::percent(failure_rate)}")
    
    # Check against error threshold
    if (failure_rate > ERROR_THRESHOLD) {
      stop(sprintf(
        "Join failure rate (%2.1f%%) exceeds threshold (%2.1f%%)", 
        failure_rate * 100, 
        ERROR_THRESHOLD * 100
      ))
    }
    
    ## 5. Combine Results 
    log_info("Combining valid and invalid records...")
    
    # Add index=0 to invalid records
    invalid_requests[, index := 0]
    
    # Select common columns to keep
    common_cols <- intersect(names(joined_data), names(invalid_requests))
    
    final_output <- rbind(
      joined_data[, ..common_cols],
      invalid_requests[, ..common_cols],
      fill = TRUE
    )
    
    ## 6. Validation 
    log_info("Validating against reference data...")
    
    # Ensure I'm comparing the same columns
    comparison_cols <- intersect(names(final_output), names(validation_data))
    
    # Sample data for validation (first N records)
    sample_size <- min(VALIDATION_SAMPLE_SIZE, nrow(final_output))
    our_sample <- final_output[1:sample_size, ..comparison_cols]
    ref_sample <- validation_data[1:sample_size, ..comparison_cols]
    
    # Compare H3 indices
    mismatches <- sum(our_sample$index != ref_sample$h3_level8_index, na.rm = TRUE)
    match_rate <- 1 - (mismatches / sample_size)
    
    log_info("Validation results:")
    log_info("- Compared {sample_size} records")
    log_info("- Mismatches: {mismatches}")
    log_info("- Match rate: {scales::percent(match_rate)}")
    
    if (match_rate < 0.95) {
      log_warn("Match rate is below 95% - verify join logic")
    }
    
    ## 7. Save Results ----------------------------------------------------------
    log_info("Saving results...")
    if (!dir.exists("output")) dir.create("output")
    
    fwrite(final_output, "output/sr_with_h3_joined.csv")
    log_info("Results saved to output/sr_with_h3_joined.csv")
    
    ## 8. Performance Metrics --------------------------------------------------
    total_duration <- difftime(Sys.time(), start_time, units = "secs")
    log_info("Processing completed in {round(total_duration, 1)} seconds")
    
    return(final_output)
    
  }, error = function(e) {
    log_error("Processing failed: {e$message}")
    stop(e)
  })
}

# Execute the script
result <- main()
