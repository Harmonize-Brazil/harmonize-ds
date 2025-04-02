..
    This file is part of Python Client Library for Harmonize Datasources.
    Copyright (C) 2023 INPE.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program. If not, see <https://www.gnu.org/licenses/gpl-3.0.html>.


Running Harmonize Datasources Client in the Command Line
========================================================

The ``Harmonize Datasources`` client installs a command line tool named ``harmonize_ds`` that allows to retrieve trajectory data.


If you want to know the Harmonize Datasources version, use the option ``--version`` as in::

    harmonize_ds --version


List the available collections::

    harmonize_ds collections 


To get more information about a specific collection, use the ``describe`` command::

    harmonize_ds describe --id 'HARMONIZE-WFS' --collection_id 'bdc_lcc:anomaly_cdays_temp_max_ne_mun_epiweek'


Retrieve the trajectory given a longitude and latitude::

    harmonize-ds download --collection_id 'bdc_lcc:zika_cases_north_mun_week' --id 'HARMONIZE-WFS' --filename '/path/obs.shp' --filter '{"date": "2017-01-01"}' --verbose
