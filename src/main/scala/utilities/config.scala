package utilities

import scala.collection.{mutable => m}

object config {

  def loadConfig(isRunningLocally: Boolean): m.Map[String, String] = {
    val file_paths = m.Map[String, String]()
    if (isRunningLocally) {
      file_paths += (
        "covid_cases_us" -> "data/nytimes_us-counties.csv",
        "covid_cases_global" -> "data/COVID-19-geographic-disbtribution-worldwide.csv",
        "social_distancing_us" -> "data/social_distancing/04",
        "climate" -> "data/daily_weather_2020.csv",
        "air_traffic" -> "data/air_traffic",
        "population_data" -> "data/Population.csv",
        "geographic_data" -> "data/cbg_geographic_data.csv",
        "area_dictionary" -> "data/mappings/area_dictionary.csv",
        "county_dictionary_us" -> "data/mappings/mapping_counties_us.csv",
        "fips_code" -> "data/mappings/cbg_fips_codes.csv",
        "states_dictionary" -> "data/mappings/mapping_states_us.csv",
        "population_data" -> "data/Population.csv",
        "geographic_data" -> "data/cbg_geographic_data.csv",
        "countries_mapping" -> "data/mappings/mapping_countries.csv",
        "airport_codes" -> "data/mappings/airport_codes.csv"
      )
    } else {
      file_paths += (
        "covid_cases_us" -> "project/data/nytimes_us-counties.csv",
        "covid_cases_global" -> "project/data/COVID-19-geographic-disbtribution-worldwide.csv",
        "social_distancing_us" -> "project/data/social_distancing",
        "climate" -> "project/data/daily_weather_2020.csv",
        "air_traffic" -> "project/data/air_traffic",
        "population_data" -> "project/data/Population.csv",
        "geographic_data" -> "project/data/cbg_geographic_data.csv",
        "area_dictionary" -> "project/data/mappings/area_dictionary.csv",
        "county_dictionary_us" -> "project/data/mappings/mapping_counties_us.csv",
        "fips_code" -> "project/data/mappings/cbg_fips_codes.csv",
        "states_dictionary" -> "project/data/mappings/mapping_states_us.csv",
        "population_data" -> "project/data/Population.csv",
        "geographic_data" -> "project/data/cbg_geographic_data.csv",
        "countries_mapping" -> "project/data/mappings/mapping_countries.csv",
        "airport_codes" -> "project/data/mappings/airport_codes.csv"
      )
    }
    file_paths
  }
}
