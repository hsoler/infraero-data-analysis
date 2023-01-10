import os
import numpy as np
import pandas as pd
import xarray as xr

'''
Notes for the unwary:

1) file names have the following inconsistency: some of the file names have the
optional "-n" appending, where n ranges from 1 to 4 and has no particular meaning
in the database.

2) files prior to 2017 are of the format "xls", while those
from 2017 onwards are of the format "xlsx"

3) in each original file, not all airports are included, only some of them (perhaps
because they didn exist at that particular date or don't exist anymore).
'''

def get_last_year(path, first_year):
    l_year = first_year
    while True:
        if os.path.isdir(path + str(l_year)):
            l_year += 1
        else:
            break
    return l_year - 1

def get_calendar(path, first_year):
    'Currently the "calendar" I use is merely an indexing from 0 upto the last month'
    'available, and additional metadata should be provided separately.'
    return [i for i in range(12*(get_last_year(path, first_year) - first_year))]

def get_file_name_mapping(path, first_year, months, full_year_only=True):
    'Provides a table for accessing all files from the root.'
    'abs_file_name -> real_file_path OR "empty"'
    'Necessary due to inconsistency in the original base file name pattern.'
    #file names are of the format aaa[-n].[xls OR xlsm] where "a" stands for any letter
    #in the alphabet and "n" is any number from 0 to 9; e.g. "mai-2.xlsm"
    table = dict()
    last_year = get_last_year(path, first_year)
    for year in range(first_year, last_year + 1):
        table[str(year)] = dict()
        for month in months:
            table[str(year)][month] = "" #indicates the month does not exist in the year
            #assume n in [-n] ranges from 0 to 9, because who knows
            for n in range(10):
                append = ""
                if n:
                    append = "-" + str(n - 1)
                extension = ".xslm"
                if year < 2017:
                    extension = ".xls"
                file_path = path + str(year) + "/" + month + append + extension
                if os.path.isfile(file_path):
                    table[str(year)][month] = file_path
        if full_year_only:
            for month in months:
                if not table[str(year)][month]: #i.e. is EMPTY
                    del table[str(year)]
                    break
    return table

def get_df_mapping(file_name_mapping, service):
    'Returns a dictionary of dataframe objects from the file paths provided.'
    'If fill is set to "True", then "empty" values will be converted to "0".'
    df_mapping = dict()
    for year in file_name_mapping:
        df_mapping[year] = dict()
        for month in file_name_mapping[year]:
            df_mapping[year][month] = dict()
            for i, sheet_id in enumerate(service):
                if int(year) < 2019: #arbitrarily inserted footer in the original database
                    skipfooter = 2
                else:
                    skipfooter = 0
                df = pd.read_excel(file_name_mapping[year][month], sheet_name=i
                , index_col=1, skiprows=4, thousands=".", decimal=",", skipfooter=skipfooter)
                df.drop("Unnamed: 0", axis=1, inplace=True)
                df_mapping[year][month][sheet_id] = df
    return df_mapping

def get_all_airports(df_mapping, service):
    'Returns a dictionary comprising all airports present in the dataframe provided as keys'
    'and all their id representations as list values.'
    all_airports = dict()
    all_airports["infraero"] = ["infraero"]
    for year in df_mapping:
        for month in df_mapping[year]:
            df = df_mapping[year][month][service[0]] #any sheet, except "4", works
            for ind in range(9, df.shape[0], 9): #the first 9 lines actually comprise aggregate values
                if df.index[ind][:4] not in all_airports:
                    all_airports[df.index[ind][:4]] = list()
                if df.index[ind] not in all_airports[df.index[ind][:4]]:
                    all_airports[df.index[ind][:4]].append(df.index[ind])
    return all_airports

def get_da(df_mapping, service, discrimination, orientation, calendar, all_airports):
    'Returns an xarray DataArray from the dataframe map provided, which may be'
    'further manipulated with the xarray library and/or exported as a netCDF4 file'
    da = xr.DataArray(coords=[
        ("airport", list(all_airports)),
        ("service", service),
        ("discrimination", discrimination),
        ("orientation", orientation),
        ("time", calendar),]).fillna(-1).astype(np.dtype(np.int32))
    for i, year in enumerate(df_mapping):
        for j, month in enumerate(df_mapping[year]):
            date = calendar[(i * 12) + j]
            df = df_mapping[year][month]
            def map_ind(n):
                return n
            def map_ind_alt(n):
                if n > 2:
                    return n - 1
                return n
            for airp in all_airports:
                airp_id = ""
                for airp_df_id in all_airports[airp]:
                    if airp_df_id in df[service[0]].index:
                        airp_id = airp_df_id
                if airp_id:
                    airp_ind = df[service[0]].index.get_loc(airp_id)
                    for k, serv in enumerate(service): #very gross stuff below
                        if k:
                            col_map=(0, 2, 6)
                        else: #i.e. swap cols for departure and arrival for the first sheet
                            col_map=(2, 0, 6)
                        if k == 4: #i.e. passengers
                            map_ind_func = map_ind
                        else:
                            map_ind_func = map_ind_alt
                        for l, discr in enumerate(discrimination):
                            if l == 3 and k != 3: #i.e. is cabotage and is not passengers
                                continue
                            ind = map_ind_func(l)
                            for m, orient in enumerate(orientation):
                                if m == 2 and k != 2: #i.e. is transit and is not mail
                                    continue
                                col = col_map[m]
                                da.loc[
                                    airp,
                                    serv,
                                    discr,
                                    orient,
                                    date] = df[serv].iloc[airp_ind + ind, col] #THIS is pretty
    return da

#TODO: add support to file name maps without full years in get_df_mapping, get_all_airports and get_da

def run(v=False):
    #remember to change this for when you use the code
    path = "DataBases/infraero/as_provided/"
    #brazilian months first three letters
    braz_months = [
        "jan",
        "fev",
        "mar",
        "abr",
        "mai",
        "jun",
        "jul",
        "ago",
        "set",
        "out",
        "nov",
        "dez"
    ]
    #Not expected to change in the future
    first_year = 2012
    #values I want to use as replacement for the original [xls, xlsx] sheet labels:
    my_services = ["aircraft", "cargo", "mail", "passengers"]
    #values I want to use as replacement for the original discrimination labels:
    my_discrimination = ["reg-national", "reg-regional", "reg-international", "reg-cabotage"
    , "irreg-national", "irreg-international"]
    #values I want to use as replacement for the original orientation labels:
    my_orientation = ["departure", "arrival", "transit"]
    my_calendar = get_calendar(path, first_year)
    f_map = get_file_name_mapping(path, first_year, braz_months)
    df_map = get_df_mapping(f_map, my_services)
    all_airps = get_all_airports(df_map, my_services)
    da = get_da(df_map, my_services, my_discrimination, my_orientation, my_calendar, all_airps)
    if v:
        return f_map, df_map, all_airps, da
    return da

