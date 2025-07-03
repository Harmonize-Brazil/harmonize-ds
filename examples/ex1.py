#
# This file is part of Python Client Library for the Harmonize Datasources.
# Copyright (C) 2025 INPE.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/gpl-3.0.html>.
#

"""Python Client Library for the Harmonize Datasources."""

from harmonize_ds import HARMONIZEDS

print("Available collections:")
for collection in HARMONIZEDS.collections():
    print(collection)

#Get specific collection
zica = HARMONIZEDS.get_collection(
   id="bdc_lcc-wfs", collection_id="bdc_lcc:zika_cases_north_mun_week"
)

# Describe operation
metadata_zica = zica.describe()
print(metadata_zica)

print(f"Title: {zica.title}")
print(f"Abstract: {zica.abstract}")

df = zica.get(filter={
   "date": "2017-02-01/2017-02-30",
   'bbox': [-49.15454736, -1.95658217, -48.55769686, -1.69057331]
   })

print(df.head())
