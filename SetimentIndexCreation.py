import pandas as pd

cols = ["Market_and_Exchange_Names", "Contract_Market_Name", "CFTC_Contract_Market_Code", "FutOnly_or_Combined",    "Contract_Units",    "Open_Interest_All",
    
    # positions
    "Prod_Merc_Positions_Long_All", "Prod_Merc_Positions_Short_All",    "Swap_Positions_Long_All", "Swap__Positions_Short_All", "Swap__Positions_Spread_All",    "M_Money_Positions_Long_All", "M_Money_Positions_Short_All", "M_Money_Positions_Spread_All",    "Other_Rept_Positions_Long_All", "Other_Rept_Positions_Short_All", "Other_Rept_Positions_Spread_All",    "Tot_Rept_Positions_Long_All", "Tot_Rept_Positions_Short_All",    "NonRept_Positions_Long_All", "NonRept_Positions_Short_All",    
    
    # weekly changes (flows)
    "Change_in_Open_Interest_All",    "Change_in_Prod_Merc_Long_All", "Change_in_Prod_Merc_Short_All",    "Change_in_Swap_Long_All", "Change_in_Swap_Short_All", "Change_in_Swap_Spread_All",    "Change_in_M_Money_Long_All", "Change_in_M_Money_Short_All", "Change_in_M_Money_Spread_All",    "Change_in_Other_Rept_Long_All", "Change_in_Other_Rept_Short_All", "Change_in_Other_Rept_Spread_All",    "Change_in_Tot_Rept_Long_All", "Change_in_Tot_Rept_Short_All",    "Change_in_NonRept_Long_All", "Change_in_NonRept_Short_All",    
    
    # percent of open interest (already-normalized versions)
    "Pct_of_Open_Interest_All",    "Pct_of_OI_Prod_Merc_Long_All", "Pct_of_OI_Prod_Merc_Short_All",    "Pct_of_OI_Swap_Long_All", "Pct_of_OI_Swap_Short_All", "Pct_of_OI_Swap_Spread_All",    "Pct_of_OI_M_Money_Long_All", "Pct_of_OI_M_Money_Short_All", "Pct_of_OI_M_Money_Spread_All",    "Pct_of_OI_Other_Rept_Long_All", "Pct_of_OI_Other_Rept_Short_All", "Pct_of_OI_Other_Rept_Spread_All",    "Pct_of_OI_Tot_Rept_Long_All", "Pct_of_OI_Tot_Rept_Short_All",    "Pct_of_OI_NonRept_Long_All", "Pct_of_OI_NonRept_Short_All",    
    
    # trader counts (participation)
    "Traders_Tot_All",    "Traders_Prod_Merc_Long_All", "Traders_Prod_Merc_Short_All",    "Traders_Swap_Long_All", "Traders_Swap_Short_All", "Traders_Swap_Spread_All",    "Traders_M_Money_Long_All", "Traders_M_Money_Short_All", "Traders_M_Money_Spread_All",    "Traders_Other_Rept_Long_All", "Traders_Other_Rept_Short_All", "Traders_Other_Rept_Spread_All",    "Traders_Tot_Rept_Long_All", "Traders_Tot_Rept_Short_All",
    
    # concentration (crowding)
    "Conc_Gross_LE_4_TDR_Long_All", "Conc_Gross_LE_4_TDR_Short_All",    "Conc_Gross_LE_8_TDR_Long_All", "Conc_Gross_LE_8_TDR_Short_All",    "Conc_Net_LE_4_TDR_Long_All", "Conc_Net_LE_4_TDR_Short_All",    "Conc_Net_LE_8_TDR_Long_All", "Conc_Net_LE_8_TDR_Short_All",]

def load_all_data(cols):
    path_to_data = str(input("Insert path to Disaggregated - Futures only CFTC COT dataset (all data) in csv form"))
    data = pd.read_csv(path_to_data)

    data["CFTC_Contract_Market_Code"] = data["CFTC_Contract_Market_Code"].astype(str).str.zfill(6)

    data_filtered = data[data["CFTC_Contract_Market_Code"] == "067651"].copy()

    data_filtered = data_filtered[data_filtered["FutOnly_or_Combined"] == "FutOnly"].copy()

    data_filtered["Report_Date_as_YYYY_MM_DD"] = pd.to_datetime(data_filtered["Report_Date_as_YYYY_MM_DD"])
    data_filtered["COT_available_date"] = data_filtered["Report_Date_as_YYYY_MM_DD"] + pd.offsets.BDay(3)  # Tue -> Fri, as reports are made available on Fridays

    num_cols = [
        "Open_Interest_All",
        "M_Money_Positions_Long_All", "M_Money_Positions_Short_All",
        "Other_Rept_Positions_Long_All", "Other_Rept_Positions_Short_All",
        "Prod_Merc_Positions_Long_All", "Prod_Merc_Positions_Short_All",
    ]
    for c in num_cols:
        if c in data_filtered.columns:
            data_filtered[c] = (
                data_filtered[c].astype(str)
                .str.replace("\u202f", "", regex=False)
                .str.replace(" ", "", regex=False)
                .str.replace(".", "", regex=False)
                .str.replace(",", "", regex=False)   # counts are integers -> remove commas too
            )
            data_filtered[c] = pd.to_numeric(data_filtered[c], errors="coerce")

    data_filtered = data_filtered.sort_values("COT_available_date")
    data_filtered["period"] = data_filtered["COT_available_date"]
    data_filtered.set_index("period", inplace=True)

    try:
        data_filtered = data_filtered[[c for c in cols]]
    except:
        raise Exception("data is missing expected columns")

    tosave = str(input("do you want to save the filtered Dataset? (Y/N)")).lower()
    if tosave == "y":
        data_filtered.to_csv("dataFiltered.csv")
        print("data saved in local folder as dataFiltered.csv")

    return data_filtered


def load_data_from_filtered(cols):
    path_to_data = str(input("Insert path to filtered data, if data is saved as dataFiltered.csv insert Y"))
    if path_to_data.lower() == "y":   # FIX: your old condition was always True
        path_to_data = "dataFiltered.csv"

    datafiltered = pd.read_csv(path_to_data)

    # ensure period is datetime index (often comes back as string from csv)
    datafiltered["period"] = pd.to_datetime(datafiltered["period"])
    datafiltered.set_index("period", inplace=True)

    # your completeness check (minimal tweak: actually check missing cols)
    missing = [c for c in cols if c not in datafiltered.columns]
    if missing:
        raise Exception(f"data is incomplete, missing columns: {missing}")

    return datafiltered

data_q = str(input("do you have a filtered dataset (Check Code for columns of interest) Y/N")).lower()
if data_q == 'y':
    data = load_data_from_filtered(cols)
else:
    data = load_all_data(cols)

initiald = pd.to_datetime('2006-06-13')
closest = pd.to_datetime('1990-01-12')


to_regress = pd.DataFrame(data["Open_Interest_All"])
#construction of dataframe with metrics to build the index
to_regress['M_Money_Net'] = (data['M_Money_Positions_Long_All'] - data['M_Money_Positions_Short_All'])#/to_regress['Open_Interest_All']
to_regress['Other_Net'] = (data['Other_Rept_Positions_Long_All'] - data['Other_Rept_Positions_Short_All'])#/to_regress['Open_Interest_All']
to_regress['Producer_Net'] = (data['Prod_Merc_Positions_Long_All'] - data['Prod_Merc_Positions_Short_All'])#/to_regress['Open_Interest_All']
to_regress['dM_Money_Net'] = to_regress['M_Money_Net'].diff()
to_regress['dOther_Net'] = to_regress['Other_Net'].diff()
to_regress['dProducer_Net'] = to_regress['Producer_Net'].diff()

W = 156

def zroll(s, w=W):
    return (s - s.rolling(w).mean()) / s.rolling(w).std()

to_regress["zMM"] = zroll(to_regress["M_Money_Net"])
to_regress["zOR"] = zroll(to_regress["Other_Net"])
to_regress["zPR"] = zroll(to_regress["Producer_Net"])

to_regress["zdMM"] = zroll(to_regress["dM_Money_Net"])
to_regress["zdOR"] = zroll(to_regress["dOther_Net"])
to_regress["zdPR"] = zroll(to_regress["dProducer_Net"])

to_regress["Sent_COT_level"] = (to_regress["zMM"] + to_regress["zOR"] - to_regress["zPR"]) / 3 #part of the index provided by levels
to_regress["Sent_COT_flow"] = (to_regress["zdMM"] + to_regress["zdOR"] - to_regress["zdPR"]) / 3 #part of the index provided by the differences


to_regress["Sentiment_Index"] = 0.8*to_regress["Sent_COT_level"] #+ 0.2 * to_regress["Sent_COT_flow"]
to_regress.to_pickle('sentimentIndex.pkl')
