#' @importFrom magrittr %>% %<>%
#' @importFrom methods new

# Create package environment to store global vars
pkg_globals <- new.env(parent = emptyenv())

#' Connect to Aquarius server
#'
#' Before you use other functions in this package to get data from Aquarius,
#' you must use this function to log in. If accessing NPS data, you must be on
#' the DOI network and you only need to provide the read-only username and
#' password.
#'
#'
#' @inheritParams timeseriesClient_connect
#' @param publish_api_url URL for the Aquarius Publish API.
#'
#' @return Invisibly returns server response
#' @export
#'
#' @examples
#' \dontrun{
#' keyring::key_set("aquarius", "aqreadonly")  # Save credentials in keyring - only need to do this once per user & computer
#' connectToAquarius("aqreadonly")  # Connect to Aquarius using password saved in keyring
#' }
connectToAquarius <- function(username, password = keyring::key_get("aquarius", username), hostname = "https://aquarius.nps.gov/aquarius", publish_api_url = "https://aquarius.nps.gov/aquarius/Publish/v2") {
  # Connects to https://aquarius.nps.gov/aquarius
  resp <- timeseries$connect(hostname, username, password)
  # Store publish API URL in global vars
  assign("publish_api_url", publish_api_url, envir = pkg_globals)
  return(invisible(resp))  # Invisibly return http response
}

#' Disconnect from Aquarius server
#'
#' Use this function to log out when you are done retrieving data.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' disconnectFromAquarius()
#' }
disconnectFromAquarius <- function() {
  timeseries$disconnect()
}

#' Get names of folders in Aquarius
#'
#' Use this to see the names of the folders in Aquarius. Use `search_string` to
#' see only folder names containing that text.
#'
#' @param search_string Optional. Text to search for in folder names. Accepts regular expressions. Case insensitive.
#'
#' @return Character vector of folder names
#' @export
#'
#' @examples
#' \dontrun{
#' getFolders()
#' getFolders("OLYM")
#' }
getFolders <- function(search_string) {
  # Get url from pkg global vars
  url <- get("publish_api_url", envir = pkg_globals)
  # Get all locations in the Aquarius DB
  loc_data <- httr::GET(paste0(url,'/GetLocationDescriptionList',sep=''))
  all_locs <- jsonlite::fromJSON(httr::content(loc_data,"text"))$LocationDescriptions
  folders <- sort(unique(all_locs$PrimaryFolder))
  # Filter folder names if search string specified
  if (!missing(search_string)) {
    folders <- folders[grep(search_string, folders, ignore.case = TRUE)]
  }

  return(folders)
}

#' Get information about locations in Aquarius
#'
#' Use this to see locations in Aquarius.
#'
#' @param search_string Optional. Text to search for in location names and identifiers. Accepts regular expressions. Case insensitive.
#' @param folder Optional. Limit results to locations in this folder. You must use the full folder name (e.g. "National Park Service.Klamath Network", *not* "Klamath Network").
#' @param include_subfolders Ignored if `folder` not specified. Indicates whether results should include locations in subfolders.
#'
#' @return Tibble containing location information
#' @export
#'
#' @examples
#' \dontrun{
#' getLocationInfo()
#' }
getLocationInfo <- function(search_string, folder, include_subfolders = TRUE) {
  # Get url from pkg global vars
  url <- get("publish_api_url", envir = pkg_globals)
  # Get all locations in the Aquarius DB
  loc_data <- httr::GET(paste0(url,'/GetLocationDescriptionList',sep=''))
  all_locs <- jsonlite::fromJSON(httr::content(loc_data,"text"))$LocationDescriptions
  all_locs <- tibble::as_tibble(all_locs) %>%
    dplyr::mutate(LastModified = timeseries$parseIso8601(LastModified))

  if (!missing(search_string)) {
    all_locs <- all_locs %>%
      dplyr::filter(grepl(search_string, Name, ignore.case = TRUE) | grepl(search_string, Identifier, ignore.case = TRUE))
  }

  if (!missing(folder) && nrow(all_locs) > 0 && include_subfolders) {
    regex <- paste0("^", folder, "(\\..+|$)")
    all_locs <- all_locs %>%
      dplyr::filter(grepl(regex, PrimaryFolder, ignore.case = TRUE))
  } else if (!missing(folder) && nrow(all_locs) > 0 && !include_subfolders) {
    regex <- paste0("^", folder, "$")
    all_locs <- all_locs %>%
      dplyr::filter(grepl(regex, PrimaryFolder, ignore.case = TRUE))
  }

  return(all_locs)
}

#' Get list of datasets at a location
#'
#' @param loc_ids Character vector of location identifier(s)
#'
#' @return A tibble with one line per dataset
#' @export
#'
#' @examples
#' \dontrun{
#' getDatasetInfo("MORA_SUP")
#' }
getTimeSeriesInfo <- function(loc_ids) {
  # Get url from pkg global vars
  url <- get("publish_api_url", envir = pkg_globals)

  datasets <- tibble::tibble()
  for (id in loc_ids) {
    # Get a list of datasets at a specific location
    resp <- httr::GET(paste0(url,'/GetTimeSeriesDescriptionList?LocationIdentifier=',id,sep=''))
    loc_dataset <- jsonlite::fromJSON(httr::content(resp,"text"))$TimeSeriesDescriptions %>%
      tibble::as_tibble()
    if (nrow(loc_dataset) == 0) {
      warning(paste("No time series found for location ID", id))
    }
    datasets <- rbind(datasets, loc_dataset)
  }
  if (nrow(datasets) > 0) {
    datasets <- dplyr::arrange(datasets, LocationIdentifier, Parameter, Label) %>%
      dplyr::mutate(dplyr::across(c("LastModified", "RawStartTime", "RawEndTime", "CorrectedStartTime", "CorrectedEndTime"), timeseries$parseIso8601))
  }

  return(datasets)
}

#' Get corrected time series data
#'
#' @param ts_id Time series identifier
#' @param start Optional. Only retrieve data after and including this date/time. Can be an R POSIXct object, or a character string in ISO 8601 format (YYYY-MM-DDThh:mm:ss+/-hh:mm)
#' @param end Optional. Only retrieve data after and including this date/time. Can be an R POSIXct object, or a character string in ISO 8601 format (YYYY-MM-DDThh:mm:ss+/-hh:mm)
#'
#' @return A list that includes timeseries data and metadata
#' @export
#'
#' @examples
#' \dontrun{
#' # Get timeseries data from Jan 1 2012 at midnight PST to Jan 1 2013 at midnight PST.
#' time_series <- getTimeSeries("Discharge.Working@Location", start = "2012-01-01T00:00:00−08:00", end = "2013-01-01T00:00:00−08:00")
#' }
getTimeSeries <- function(ts_id, start, end) {
  dataset <- timeseries$getTimeSeriesCorrectedData(ts_id, start, end)
  # Clean things up a bit - convert text dates into R dates
  if ("Timestamp" %in% names(dataset$Points)) {
    dataset$Points <- dataset$Points %>%
      tibble::as_tibble() %>%
      dplyr::mutate(Timestamp = timeseries$parseIso8601(Timestamp))
  }

  dataset$TimeRange$StartTime <- try(timeseries$parseIso8601(dataset$TimeRange$StartTime))
  dataset$TimeRange$EndTime <- try(timeseries$parseIso8601(dataset$TimeRange$EndTime))
  dataset$ResponseTime <- try(timeseries$parseIso8601(dataset$ResponseTime))
  dataset$Approvals <- try(dplyr::mutate(dataset$Approvals, dplyr::across(c("DateAppliedUtc", "StartTime", "EndTime"), timeseries$parseIso8601)) %>%
    tibble::as_tibble())
  dataset$Methods <- try(dplyr::mutate(dataset$Methods, dplyr::across(c("StartTime", "EndTime"), timeseries$parseIso8601)) %>%
    tibble::as_tibble())
  dataset$Grades <- try(dplyr::mutate(dataset$Grades, dplyr::across(c("StartTime", "EndTime"), timeseries$parseIso8601)) %>%
    tibble::as_tibble())
  dataset$GapTolerances <- try(dplyr::mutate(dataset$GapTolerances, dplyr::across(c("StartTime", "EndTime"), timeseries$parseIso8601)) %>%
    tibble::as_tibble())
  dataset$InterpolationTypes <- try(dplyr::mutate(dataset$InterpolationTypes, dplyr::across(c("StartTime", "EndTime"), timeseries$parseIso8601)) %>%
    tibble::as_tibble())

  return(dataset)
}

#' Get all corrected time series data at a location
#'
#' @param loc_ids A character vector of location identifiers
#' @param parameters Optional. A character vector of parameters to include. Retrieves data for all parameters if omitted.
#' @param labels Optional. A character vector of labels to include. Retrieves data for all labels if omitted.
#' @inheritParams getTimeSeries
#'
#' @return A list of time series by location
#' @export
#'
#' @examples
#' \dontrun{
#' olym_clim_ts <- getTimeSeriesByLocation("OLYM_MET_DPC", c("Air Temp", "Snow Depth"), c("Avg", "Depth"))
#' }
getTimeSeriesByLocation <- function(loc_ids, parameters, labels, start, end) {
  ts_ids <- getTimeSeriesInfo(loc_ids) %>%
    dplyr::select(Identifier, LocationIdentifier, Parameter, Label) %>%
    unique()

  if (!missing(parameters)) {
    ts_ids <- dplyr::filter(ts_ids, Parameter %in% parameters)
    if (!all(parameters %in% ts_ids$Parameter)) {
      missing_params <- parameters[!parameters %in% ts_ids$Parameter]
      warning(paste("The parameters", paste(missing_params, collapse = ", "), "are not present at these locations."))
    }
  }

  if (!missing(labels)) {
    ts_ids <- dplyr::filter(ts_ids, Label %in% labels)
    if (!all(labels %in% ts_ids$Label)) {
      missing_labels <- labels[!labels %in% ts_ids$Label]
      warning(paste("The labels", paste(missing_labels, collapse = ", "), "are not present at these locations."))
    }
  }

  all_timeseries <- list()
  if (nrow(ts_ids) > 0) {
    for (i in 1:nrow(ts_ids)) {
      loc_identifier <- ts_ids$LocationIdentifier[i]
      ts_identifier <- ts_ids$Identifier[i]
      all_timeseries[[loc_identifier]][[ts_identifier]] <- getTimeSeries(ts_identifier, start, end)
    }
  }

  return(all_timeseries)
}

#' Get all corrected time series data in a folder containing multiple locations
#'
#' @param folder Limit results to locations in this folder. You must use the full folder name (e.g. "National Park Service.Klamath Network", *not* "Klamath Network")
#' @inheritParams getTimeSeries
#' @inheritParams getTimeSeriesByLocation
#' @inheritParams getLocationInfo
#'
#' @return A list of time series by location
#' @export
#'
#' @examples
#' \dontrun{
#' olym_clim_ts <- getTimeSeriesByFolder("OLYM_Climate", c("Air Temp", "Snow Depth"), c("Avg", "Depth"))
#' }
getTimeSeriesByFolder <- function(folder, parameters, labels, start, end, include_subfolders = TRUE) {
  locs <- getLocationInfo(folder = folder, include_subfolders = include_subfolders)
  locs <- locs$Identifier

  all_timeseries <- list()
  if (length(locs) > 0) {
    all_timeseries <- getTimeSeriesByLocation(locs, parameters, labels, start, end)
  } else {
    warning("No locations found in this folder")
  }

  return(all_timeseries)
}
