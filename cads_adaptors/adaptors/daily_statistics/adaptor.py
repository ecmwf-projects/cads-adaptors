"""Adaptor for post-processing ERA5 and ERA5-land data to produce daily statistics."""

# Copyright 2022, European Union.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from copy import copy
from datetime import timedelta
from typing import Any

import dateutil

from cads_adaptors import MarsCdsAdaptor
from cads_adaptors.adaptors.mars import execute_mars
from cads_adaptors.exceptions import InvalidRequest
from cads_adaptors.tools.general import ensure_list
from cads_adaptors.tools.hcube_tools import merge_requests

# define variables type
ACCUMULATED_FIELDS = [
    "large_scale_precipitation_fraction",
    "downward_uv_radiation_at_the_surface",
    "boundary_layer_dissipation",
    "surface_sensible_heat_flux",
    "surface_latent_heat_flux",
    "surface_solar_radiation_downwards",
    "surface_thermal_radiation_downwards",
    "surface_net_solar_radiation",
    "surface_net_thermal_radiation",
    "top_net_solar_radiation",
    "top_net_thermal_radiation",
    "eastward_turbulent_surface_stress",
    "northward_turbulent_surface_stress",
    "eastward_gravity_wave_surface_stress",
    "northward_gravity_wave_surface_stress",
    "gravity_wave_dissipation",
    "top_net_solar_radiation_clear_sky",
    "top_net_thermal_radiation_clear_sky",
    "surface_net_solar_radiation_clear_sky",
    "surface_net_thermal_radiation_clear_sky",
    "toa_incident_solar_radiation",
    "vertically_integrated_moisture_divergence",
    "total_sky_direct_solar_radiation_at_surface",
    "clear_sky_direct_solar_radiation_at_surface",
    "surface_solar_radiation_downward_clear_sky",
    "surface_thermal_radiation_downward_clear_sky",
    "surface_runoff",
    "sub_surface_runoff",
    "snow_evaporation",
    "snowmelt",
    "large_scale_precipitation",
    "convective_precipitation",
    "snowfall",
    "evaporation",
    "runoff",
    "total_precipitation",
    "convective_snowfall",
    "large_scale_snowfall",
    "potential_evaporation",
    "total_evaporation",
    "evaporation_from_bare_soil",
    "evaporation_from_the_top_of_canopy",
    "evaporation_from_open_water_surfaces_excluding_oceans",
    "evaporation_from_vegetation_transpiration",
]
MEAN_FIELDS = [
    "mean_boundary_layer_dissipation",
    "mean_convective_precipitation_rate",
    "mean_convective_snowfall_rate",
    "mean_eastward_gravity_wave_surface_stress",
    "mean_eastward_turbulent_surface_stress",
    "mean_evaporation_rate",
    "mean_gravity_wave_dissipation",
    "mean_large_scale_precipitation_fraction",
    "mean_large_scale_precipitation_rate",
    "mean_large_scale_snowfall_rate",
    "mean_northward_gravity_wave_surface_stress",
    "mean_northward_turbulent_surface_stress",
    "mean_potential_evaporation_rate",
    "mean_runoff_rate",
    "mean_snow_evaporation_rate",
    "mean_snowfall_rate",
    "mean_snowmelt_rate",
    "mean_sub_surface_runoff_rate",
    "mean_surface_direct_short_wave_radiation_flux",
    "mean_surface_direct_short_wave_radiation_flux, clear_sky",
    "mean_surface_downward_uv_radiation_flux",
    "mean_surface_downward_long_wave_radiation_flux",
    "mean_surface_downward_long_wave_radiation_flux, clear_sky",
    "mean_surface_downward_short_wave_radiation_flux",
    "mean_surface_downward_short_wave_radiation_flux, clear_sky",
    "mean_surface_latent_heat_flux",
    "mean_surface_net_long_wave_radiation_flux",
    "mean_surface_net_long_wave_radiation_flux_clear_sky",
    "mean_surface_net_short_wave_radiation_flux",
    "mean_surface_net_short_wave_radiation_flux, clear_sky",
    "mean_surface_runoff_rate",
    "mean_surface_sensible_heat_flux",
    "mean_top_downward_short_wave_radiation_flux",
    "mean_top_net_long_wave_radiation_flux",
    "mean_top_net_long_wave_radiation_flux, clear_sky",
    "mean_top_net_short_wave_radiation_flux",
    "mean_top_net_short_wave_radiation_flux, clear_sky",
    "mean_total_precipitation_rate",
    "mean_vertically_integrated_moisture_divergence",
]


class Era5DailyStatisticsCdsAdaptor(MarsCdsAdaptor):
    def remove_partial_periods(
        self,
        in_xarray_dict: dict[str, Any],
        date_list: list[str] | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        if date_list is None:
            return in_xarray_dict

        # If single date, selection is singular::
        if len(date_list) == 1:
            selection = date_list
        else:
            # Slicing is faster, so slice if we can:
            consecutive_dates = True
            date_obj_list = [dateutil.parser.parse(date) for date in date_list]
            for i, date in enumerate(date_obj_list[:-1]):
                if date != date_obj_list[i + 1] - timedelta(days=1):
                    consecutive_dates = False
                    break
            if consecutive_dates:
                selection = slice(date_list[0], date_list[-1])
            else:
                self.context.add_user_visible_log(
                    "WARNING: Please note that for the daily statistics datasets "
                    "selection of non-consecutive dates is significantly "
                    "slower than consecutive dates."
                )
                selection = date_list

        out_xarray_dict = {}
        for tag, in_dataset in in_xarray_dict.items():
            out_xarray_dict[tag] = in_dataset.sel(valid_time=selection)

        return out_xarray_dict

    def pre_mapping_modifications(self, request):
        # Move the receipt flag from the request to the adaptor attributes (currently not in use)
        self.receipt = request.pop("receipt", False)

        # Extract post-process steps from the request before applying the mapping
        if request.pop("post_process", []):
            self.context.add_user_visible_log(
                "WARNING: Post-processing steps cannot be applied to the daily statistics datasets. "
                "The post-processing steps you have requested have been ignored"
            )

        download_format = request.pop("download_format", "zip")
        self.set_download_format(ensure_list(download_format))

        # Some quick checks to ensure valid request
        if len(ensure_list(request.get("daily_statistic", "daily_mean"))) > 1:
            raise InvalidRequest(
                "Multiple daily statistic values in a single request is not supported."
            )
        if len(ensure_list(request.get("time_zone", "UTC+00:00"))) > 1:
            raise InvalidRequest(
                "Multiple time zone values in a single request is not supported."
            )
        if len(ensure_list(request.get("frequency", "1_hourly"))) > 1:
            raise InvalidRequest(
                "Multiple frequency values in a single request is not supported."
            )

        return request

    def get_date_list_extended(
        self, date_list: list[str], time_zone_hour: int, first_valid_date_str: str
    ) -> list[str]:
        """Return extended date list with one day before and after the requested dates.

        Args
        ----
            date_list (list[str]):
                List of dates in the format "YYYY-MM-DD"
            time_zone_hour (int):
                Time zone offset in hours
            first_valid_date_str (str, optional):
                First valid date in the format "YYYY-MM-DD". Defaults to "1940-01-01".

        Raises
        ------
            InvalidRequest: if the requested dates are not valid

        Returns
        -------
            list[str]: extended list of dates including one day before and after the requested dates
        """
        # Pre-process dates
        _date_obj_list = [dateutil.parser.parse(date) for date in date_list]

        # Check requested dates are valid
        first_valid_date = dateutil.parser.parse(first_valid_date_str)
        if time_zone_hour > 0:
            first_valid_date = first_valid_date + timedelta(days=1)

        date_obj_list = [date for date in _date_obj_list if date >= first_valid_date]
        if len(date_obj_list) == 0:
            raise InvalidRequest(
                "Your request did not provide a valid time-period, please check your date selection."
            )
        if len(date_obj_list) != len(_date_obj_list):
            self.context.add_user_visible_error(
                "Some of the dates you requested are not valid, and have been removed from the request."
            )
            date_list = [date.strftime("%Y-%m-%d") for date in date_obj_list]

        # Ensure that we have one day before, and one day after the requested dates
        #  This should include any partial periods within the selection
        date_obj_list_extended = copy(date_obj_list)
        for date in date_obj_list:
            if date - timedelta(days=1) not in date_obj_list_extended:
                date_obj_list_extended.append(date - timedelta(days=1))
            if date + timedelta(days=1) not in date_obj_list_extended:
                date_obj_list_extended.append(date + timedelta(days=1))
        date_obj_list_extended.sort()

        # Remove the first date if it is before the first valid date (e.g. 1939-12-31)
        if date_obj_list_extended[0] < first_valid_date:
            date_obj_list_extended = date_obj_list_extended[1:]

        # This is all the dates with any extra days before or after the requested dates
        return [date.strftime("%Y-%m-%d") for date in date_obj_list_extended]

    def retrieve_list_of_results(self, request: dict[str, Any]) -> list[str]:
        request = self.normalise_request(request=request)

        # Turn back into a single request
        if len(self.mapped_requests) > 1:
            mapped_request = merge_requests(self.mapped_requests)
        else:
            mapped_request = self.mapped_requests[0]

        # Ensure that data_format is removed from request, output is always netCDF
        _ = mapped_request.pop("data_format", None)
        self.context.info(f"Daily stats, mapped_request = {mapped_request}")

        # Take a copy of the request, so we can remove elements that are not required for MARS
        mars_request = copy(mapped_request)

        # Extract the non-MARS parameters from the request, with defaults if not provided
        statistic: str = mars_request.pop("daily_statistic", "daily_mean")
        time_zone: str = mars_request.pop("time_zone", "UTC+00:00")
        time_zone_hour: int = int(time_zone.lower().replace("utc", "")[:3])
        frequency_str: str = mars_request.pop("frequency", "1_hourly")
        frequency: int = int(frequency_str.replace("_hourly", ""))

        # Pre-process dates
        date_list: list[str] = ensure_list(mars_request["date"])
        date_list_extended = self.get_date_list_extended(
            date_list, time_zone_hour, self.config.get("first_valid_date", "1940-01-01")
        )

        # TODO: The accumulation_period logic is hard-coded to ERA5, i.e. based on the dataset value.
        #       It could be made more flexible in the future if required
        accumulation_period = 1  # Set a default value
        if mars_dataset := mars_request.get("dataset", None):
            mars_dataset = ensure_list(mars_dataset)
            if len(mars_dataset) > 1:
                raise InvalidRequest(
                    "Only one product_type per request is supported for daily statistics."
                )
            mars_request["dataset"] = mars_dataset[0]
            # Accumulation period is required for adjusting the request time
            #  to get the correct values for the day requested
            # The default values below are for ERA5 oper. Technically, members for ERA5 wave data should be 1
            #  but as there are no accumulated variables in the wave data, we can ignore this.
            accumulation_period_to_dataset_mapping = self.config.get(
                "accumulation_period_to_dataset_mapping",
                {
                    "reanalysis": 1,
                    "mean": 3,
                    "members": 3,
                },
            )
            accumulation_period = accumulation_period_to_dataset_mapping.get(
                mars_request["dataset"], None
            )
            if accumulation_period is None:
                raise InvalidRequest(
                    f"Unrecognised product_type: {mars_request['dataset']}"
                )

        # Split by variable as time handling varies, and best to have a clean consistent approach
        variables = ensure_list(self.input_request["variable"])
        variable_mapping = self.mapping.get("remap", {}).get("variable", {})

        self.context.debug(f"Daily stats, variable_mapping = {variable_mapping}")
        # Map variables to param ids
        param_ids = {var: variable_mapping.get(var, var) for var in variables}
        self.context.debug(f"Daily stats, param_ids = {param_ids}")

        results: list[str] = []
        for var, param_id in param_ids.items():
            # Accumulated variables checks
            if var in ACCUMULATED_FIELDS and not self.config.get(
                "accumulated_variables_supported", True
            ):
                self.context.add_user_visible_error(
                    "Daily statistics of accumulated variables are not supported for this dataset, "
                    f"skipping: {var}."
                )
                continue

            if statistic in ["daily_sum"] and var not in ACCUMULATED_FIELDS:
                self.context.add_user_visible_error(
                    f"Daily sum is not available for this variable, skipping: {var}."
                )
                continue

            self.context.debug(f"Daily stats, var, param_id = {var}, {param_id}")

            # Accumulated and Mean fields are accumulated for the hour up to the time stamp, therefore the
            # values at 00:00 represent the values from 23:00 to 00:00 from the previous day.
            # Therefore, we shift the time zone hour back by 1 to get the correct values for the day requested
            if var in ACCUMULATED_FIELDS + MEAN_FIELDS:
                this_hour = time_zone_hour - accumulation_period
            else:
                this_hour = time_zone_hour

            # List of times to request at the requested frequency.
            # Ensure hours are wrapped into the 0–23 range and are unique and sorted,
            # as expected by MARS.
            raw_hours = [
                (i + (this_hour % frequency)) % 24 for i in range(0, 24, frequency)
            ]
            unique_sorted_hours = sorted(set(raw_hours))
            this_time: list[str] = [f"{hour:02d}:00:00" for hour in unique_sorted_hours]

            # Create a request for this variable
            this_request = {
                **mars_request,
                "date": date_list_extended,
                "time": this_time,
                "param": [param_id],
            }
            self.context.debug(f"Daily stats, this_request = {this_request}")
            # Get the data from MARS
            mars_result = execute_mars(
                this_request,
                context=self.context,
                config=self.config,
                target_fname=f"{var}.grib",
                mapping=self.mapping,
            )

            # Create daily statistic post processing step.
            # NOTE: Could append existing pp_steps here, but for now just overwrite
            self.post_process_steps = self.pp_mapping(
                [
                    {
                        "method": statistic,
                        "time_shift": {"hours": this_hour},
                    },
                    # Use a bespoke function to select based on the requested date_list
                    {"method": "remove_partial_periods", "date_list": date_list},
                ]
            )

            results += self.convert_format(
                self.post_process(mars_result),
                "netcdf",
                context=self.context,
                config=self.config,
            )

        # Check that we have produced a result
        if len(results) == 0:
            self.context.add_user_visible_error(
                "Your request did not return any data. Please check that your data selection matches "
                "the statistic you requested and try again."
            )
            raise InvalidRequest("No data returned")

        # A check to ensure that if there is more than one result file, and download_format
        #  is as_source, we over-ride and zip the files
        if len(results) > 1 and self.download_format == "as_source":
            self.download_format = "zip"

        return results
