"""
Module: page_process_mining_simple
This module provides functionalities for process mining analysis using Streamlit and PM4Py libraries. 
It allows users to load data from files or use example datasets, define relevant columns, and perform 
process mining analysis to generate statistics and visualizations.
Functions:
    clear_tempfiles(): Deletes files in the temporary files folder that are older than one hour.
    make_text_from_seconds(seconds): Converts seconds to a string with time in seconds, minutes, hours, days, weeks, or months.
"""
import os
import time
import streamlit as st
import numpy as np
import pandas as pd
import pm4py

st.set_page_config(page_title="Process Mining made easy", page_icon="⛏️", layout="wide")
st.title("Process Mining made easy")

TEMPORARY_FILES_FOLDER_NAME = "tempfilesProcessMiningSimple"
if not os.path.exists(TEMPORARY_FILES_FOLDER_NAME): #create folder if not exists
    os.makedirs(TEMPORARY_FILES_FOLDER_NAME)

def clear_tempfiles():
    """
    Clears all temporary files in the specified temporary folder that are older than 1 hour.
    This function iterates through all files in the temporary folder and deletes those
    that were last modified more than one hour ago.
    Parameters: None
    Returns: None
    """
    # clears all in tempfiles which is older than 1 hour
    temp_folder = TEMPORARY_FILES_FOLDER_NAME
    now = time.time()
    cutoff = now - 3600  # 1 hour ago

    for filename in os.listdir(temp_folder):
        file_path = os.path.join(temp_folder, filename)
        if os.path.isfile(file_path):
            file_modified_time = os.path.getmtime(file_path)
            if file_modified_time < cutoff:
                os.remove(file_path)

clear_tempfiles() #deletes all files in tempfiles older than 1 hour

@st.cache_data
def load_file_to_dataframe(filename):
    """Loads file and returns event log.
    Extra function used to allow streamlit caching of data, so that dataload is not done with every re-run.

    Args:
        filename (str): File name

    Returns:
        dataframe: Pandas dataframe
    """
    df = pd.read_excel(filename, sheet_name=0)
    return df

@st.cache_data
def load_event_log_from_dataframe(df, case_column, activity_column, timestamp_column, timestamp_end_column = None):
    """Loads dataframe and returns event log.
    Extra function used to allow streamlit caching of data, so that dataload is not done with every re-run.

    Args:
        df (dataframe): Pandas dataframe

    Returns:
        event log: PM4Py event log
    """
    if timestamp_end_column is None:
        new_log = pm4py.format_dataframe(df, case_id=case_column, activity_key=activity_column, timestamp_key=timestamp_column)
    else:
        #convert data, each row is split into two rows, one for start and one for end
        new_data = {case_column: [], activity_column: [], timestamp_column: []}
        for i, row in df.iterrows():
            new_data[case_column].append(row[case_column])
            new_data[activity_column].append(str(row[activity_column]) + "_start")
            new_data[timestamp_column].append(row[timestamp_column])
            new_data[case_column].append(row[case_column])
            new_data[activity_column].append(str(row[activity_column]) + "_end")
            new_data[timestamp_column].append(row[timestamp_end_column])
        df_new = pd.DataFrame(new_data)
        new_log = pm4py.format_dataframe(df_new, case_id=case_column, activity_key=activity_column, timestamp_key=timestamp_column)
    return new_log

def make_text_from_seconds(seconds):
    '''
    Converts seconds to a string with time in seconds, minutes, hours, days, weeks or months.
    '''
    if seconds < 60 * 2:
        return f"{int(round(seconds, 0))} seconds"
    elif seconds < 3600 * 1:
        return f"{int(round(seconds / 60, 0))} minutes"
    elif seconds < 86400 * 1:
        return f"{round(seconds / 3600, 1)} hours"
    elif seconds < 604800 * 2:
        return f"{round(seconds / 86400, 1)} days"
    elif seconds < 2592000 * 1:
        return f"{round(seconds / 604800, 1)} weeks"
    else:
        return f"{round(seconds / 2592000, 1)} months"


df = None
log = None

st.header("Load data")
st.text("Choose how to load data for process mining analysis, either from a file or use example data.")

select_load_procedure = st.selectbox("Select load procedure", ["Load from file", "Load example data 1 - Purchase orders", "Load example data 2 - Tablet production"])

if select_load_procedure == "Load from file":
    #allow user upload of excel file
    uploaded_file = st.file_uploader("Choose an Excel file, data from first sheet is loaded, if you see 'Network Error', most likely file is opened in Excel, close it.", type="xlsx")
    if uploaded_file is not None:
        try:
            df = load_file_to_dataframe(uploaded_file)
            st.success("File loaded successfully!")
        except Exception as e:
            st.error(f"Error loading file: {e}, if it is a network error most likely you have the file open in Excel. Close it.")

if select_load_procedure == "Load example data 1 - Purchase orders":
    # Load example dataframe
    example_data = {
        'case_id': ["1", "1", "1", "1", "1", 
                    "2", "2", "2", "2", "2", 
                    "3", "3", "3", "3", "3", "3", 
                    "4", "4", "4", "4", "4"],
        'activity': ["Create purchase order", "Select vendor", "Send PO", "Receive goods", "Authorize invoice", 
                     "Create purchase order", "Select vendor", "Send PO", "Receive goods", "Authorize invoice", 
                     "Create purchase order", "Select vendor", "Send PO", "Receive goods", "Quality defect", "Authorize invoice", 
                     "Create purchase order", "Select vendor", "Send PO", "Receive goods", "Authorize invoice"],
        'timestamp': pd.to_datetime(["01.10.2023", "03.10.2023", "04.10.2023", "03.11.2023", "08.11.2023", 
                                     "04.10.2023", "09.10.2023", "12.10.2023", "02.11.2023", "11.11.2023", 
                                     "18.10.2023", "23.10.2023", "23.10.2023", "30.10.2023", "03.11.2023", "25.11.2023", 
                                     "01.11.2023", "08.11.2023", "12.11.2023", "01.12.2023", "08.12.2023"], format="%d.%m.%Y")
    }
    df = pd.DataFrame(example_data)

if select_load_procedure == "Load example data 2 - Tablet production":
    # Load example dataframe
    example_data = {
        'case_id': ["1", "1", "1", 
                    "2", "2", "2", 
                    "3", "3", "3", 
                    "4", "4", "4", "4", 
                    "5", "5", "5", "5"],
        'activity': ["Granulation", "Tabletting", "Coating", 
                     "Granulation", "Tabletting", "Coating", 
                     "Granulation", "Tabletting", "Coating", 
                     "Granulation", "Tabletting", "Coating", "Sorting", 
                     "Granulation", "Tabletting", "Coating", "Sorting"],
        'timestamp_start': pd.to_datetime(["01.10.2023", "14.10.2023", "18.10.2023", 
                                     "04.10.2023", "12.10.2023", "18.10.2023", 
                                     "09.10.2023", "19.10.2023", "25.10.2023", 
                                     "11.10.2023", "20.10.2023", "24.10.2023", "30.10.2023", 
                                     "18.10.2023", "24.10.2023", "31.10.2023", "04.11.2023"], format="%d.%m.%Y"),
        'timestamp_end': pd.to_datetime(["10.10.2023", "17.10.2023", "22.10.2023", 
                                     "08.10.2023", "14.10.2023", "24.10.2023", 
                                     "10.10.2023", "23.10.2023", "30.10.2023", 
                                     "17.10.2023", "23.10.2023", "29.10.2023", "02.11.2023", 
                                     "22.10.2023", "27.10.2023", "03.11.2023", "08.11.2023"], format="%d.%m.%Y"),
    }
    df = pd.DataFrame(example_data)
    st.info("Try to set timestamp_end columns to 'timestamp_end to see how the activities are automatically split into start/end.")

#read excel file first sheet as pandas dataframe


if "df" in locals() and df is not None:

    with st.expander("View loaded data", expanded=False, icon="💾"):
        st.subheader("Data table")

        if select_load_procedure[10:] == "Load example":
            st.text("All data is shown below.")
            st.dataframe(df, hide_index=True)
        elif len(df) > 30:
            st.text("First 15 rows of data are shown below.")
            st.dataframe(df.head(15), hide_index=True)
        else:
            st.text("All data is shown below.")
            st.dataframe(df, hide_index=True)

    with st.expander("Define columns with relevant data", expanded=True, icon="🔡"):
        #user can now choose, which column to take for case, activity and timestamp
        st.subheader("Choose columns for case, activity and timestamp")
        column_definition_col1, column_definition_col2, column_definition_col3, column_definition_col4 = st.columns(4)

        index_case_column = 0
        if "case_id" in df.columns:
            index_case_column = df.columns.get_loc("case_id")
        with column_definition_col1:
            case_column = st.selectbox("Select column for case", df.columns, index=index_case_column)
            st.text("A case is a sequence of activities that belong together. This column should contain the case identifier.")

        index_activity_column = 0
        if "activity" in df.columns:
            index_activity_column = df.columns.get_loc("activity")
        with column_definition_col2:
            activity_column = st.selectbox("Select column for activity", df.columns, index=index_activity_column)
            st.text("An activity is a step in the process. This column should contain the activity identifier.")

        index_timestamp_start_column = 0
        if "timestamp" in df.columns:
            index_timestamp_start_column = df.columns.get_loc("timestamp")
        elif "timestamp_start" in df.columns:
            index_timestamp_start_column = df.columns.get_loc("timestamp_start")
        with column_definition_col3:
            timestamp_start_column = st.selectbox("Select column for timestamp", df.columns, index=index_timestamp_start_column)
            st.text("A timestamp is the time when an activity was executed. This column should contain the timestamp.")

        index_timestamp_end_column = None
        # if "timestamp_end" in df.columns:
        #     index_timestamp_end_column = df.columns.get_loc("timestamp_end")
        with column_definition_col4:
            timestamp_end_column = st.selectbox("Select column for timestamp end", df.columns, index=index_timestamp_end_column)
            st.text(" If not available, leave empty. Defines the end timestamp for each activity. If defined, activities will be split in 'activity start' and 'activity end' events.")

    #show warning if user selects the same column for case and activity
    any_warning = False
    if case_column == activity_column:
        st.warning("Case and activity column should be different")
        any_warning = True
    #show warning if user selects the same column for case and timestamp start
    if case_column == timestamp_start_column:
        st.warning("Case and timestamp start column should be different")
        any_warning = True
    #show warning if user selects the same column for case and timestamp end
    if case_column == timestamp_end_column:
        st.warning("Case and timestamp end column should be different")
        any_warning = True
    #show warning if user selects the same column for activity and timestamp start
    if activity_column == timestamp_start_column:
        st.warning("Activity and timestamp start column should be different")
        any_warning = True
    #show warning if user selects the same column for activity and timestamp end
    if activity_column == timestamp_end_column:
        st.warning("Activity and timestamp end column should be different")
        any_warning = True
    #show warning if user selects the same column for timestamp start and timestamp end
    if timestamp_start_column == timestamp_end_column:
        st.warning("Timestamp start and timestamp end column should be different")
        any_warning = True
    #show warning if timestamp start column is not of type datetime
    if df[timestamp_start_column].dtype != 'datetime64[ns]':
        st.warning("Timestamp start column should be of type datetime, convert to datetime in Excel first")
        any_warning = True
    #show warning if timestamp end column is not of type datetime
    if timestamp_end_column is not None:
        if df[timestamp_end_column].dtype != 'datetime64[ns]':
            st.warning("Timestamp end column should be of type datetime, convert to datetime in Excel first")
            any_warning = True
    #convert dataframe to event log
    if any_warning is False:
        log = load_event_log_from_dataframe(df, case_column, activity_column, timestamp_start_column, timestamp_end_column)


if log is not None:

    with st.expander("Process Mining Statistics", expanded=False, icon="🧮"):
        st.header("Process Mining Statistics")
        st.text("This section performs process mining analysis on the uploaded data.")

        st.subheader("Basic statistics")
        #get all case durations and write some basic statistics
        log_case_durations = pm4py.get_all_case_durations(log)
        log_case_durations = np.array(log_case_durations)

        log_variants = pm4py.get_variants(log)
        log_variants = dict(sorted(log_variants.items(), key=lambda item: item[1], reverse=True)) #sort dictionary by value in descending order, to ensure that list_of_variants is in descending order
        list_of_variants = list(log_variants.keys())

        st.text(f"""
                The dataset contains {len(log_case_durations)} cases with an average case duration of {make_text_from_seconds(np.mean(log_case_durations))}.  
                There are {len(log_variants)} different variants in the dataset.
                """)

        #create dataframe with main kpis
        df_kpis = pd.DataFrame(
            {
                '# of cases': [len(log_case_durations)],
                'mean duration': [make_text_from_seconds(np.mean(log_case_durations))],
                'median duration': [make_text_from_seconds(np.median(log_case_durations))],
                'minimum duration': [make_text_from_seconds(np.min(log_case_durations))],
                'maximum duration': [make_text_from_seconds(np.max(log_case_durations))],
                'stdev duration': [make_text_from_seconds(np.std(log_case_durations))]
            }
        )
        st.dataframe(df_kpis, hide_index=True)

        #find number of variants
        st.subheader("Variant details")

        #change log variants dictionary, value should be dictionary with '# of cases' as key and old value as value
        log_variants = {k: {"# of cases": v} for k, v in log_variants.items()}

        #add all the other kpis to the dictionary
        for variant, kpis in log_variants.items():
            filtered_log = pm4py.filter_variants(log, [variant])
            filtered_log_case_durations = pm4py.get_all_case_durations(filtered_log)
            filtered_log_case_durations = np.array(filtered_log_case_durations)
            kpis["mean duration"] = make_text_from_seconds(np.mean(filtered_log_case_durations))
            kpis["median duration"] = make_text_from_seconds(np.median(filtered_log_case_durations))
            kpis["min duration"] = make_text_from_seconds(np.min(filtered_log_case_durations))
            kpis["max duration"] = make_text_from_seconds(np.max(filtered_log_case_durations))
            kpis["stdev duration"] = make_text_from_seconds(np.std(filtered_log_case_durations))

        #create new dict from log_variants, key is joined with ., value is same
        log_variants_new = {('.'.join(k)): v for k, v in log_variants.items()}

        df_variants = pd.DataFrame.from_dict(log_variants_new, orient='index', columns=['# of cases', 'mean duration', 'median duration', 'min duration', 'max duration', 'stdev duration'])
        df_variants = df_variants.reset_index().rename(columns={'index': 'variant'})
        df_variants = df_variants.sort_values(by='# of cases', ascending=False)
        st.dataframe(df_variants, hide_index=True)


        st.subheader("Start and end activities")
        #find start activities
        start_end_activities_col1, start_end_activities_col2 = st.columns(2)
        with start_end_activities_col1:
            st.text("Start activities")
            log_start_activities = pm4py.get_start_activities(log)
            df_start_activities = pd.DataFrame.from_dict(log_start_activities, orient='index', columns=['# of cases'])
            df_start_activities = df_start_activities.reset_index().rename(columns={'index': 'activity'})
            df_start_activities = df_start_activities.sort_values(by='# of cases', ascending=False) #sort by number of cases in descending order
            st.dataframe(df_start_activities, hide_index=True)
        #find end activities
        with start_end_activities_col2:
            st.text("End activities")
            log_end_activities = pm4py.get_end_activities(log)
            df_end_activities = pd.DataFrame.from_dict(log_end_activities, orient='index', columns=['# of cases'])
            df_end_activities = df_end_activities.reset_index().rename(columns={'index': 'activity'})
            df_end_activities = df_end_activities.sort_values(by='# of cases', ascending=False)
            st.dataframe(df_end_activities, hide_index=True)

    with st.expander("Process Visualization", expanded=False, icon="📊"):
        st.header("Visualizations")
        st.subheader("Variant filter")
        #add selectbox where user can filter a variant
        list_of_variants.insert(0, "All")
        chosen_variant = st.selectbox("Select a variant to visualize", list_of_variants)
        #filter variant
        if chosen_variant == "All":
            filtered_log = log
        else:
            filtered_log = pm4py.filter_variants(log, [chosen_variant])
        log_case_durations = pm4py.get_all_case_durations(log)
        st.text(f"Filtered {len(pm4py.get_all_case_durations(filtered_log))} cases.")


        st.subheader("Statistics for filtered variant")
        #calculate dfg and performance dfg
        performance_dfg, start_activities_perf_dfg, end_activities_perf_dfg = pm4py.discover_performance_dfg(filtered_log)
        dfg, start_activities_dfg, end_activities_dfg = pm4py.discover_dfg(filtered_log)
        if len(dfg) > 0: #there are some activities found
            df_performance_dfg = pd.DataFrame.from_dict(performance_dfg, orient='index')
            # st.dataframe(df_performance_dfg, hide_index=False)
            dfg_for_dataframe = {k: {'frequency': v} for k, v in dfg.items()} #need to "convert" dictionary to be able to create dataframe similar to performance dfg (for later merge)
            df_dfg = pd.DataFrame.from_dict(dfg_for_dataframe, orient='index', columns=['frequency'])
            # st.dataframe(df_dfg, hide_index=False)
            #merge both dataframes on index
            df_merged = pd.merge(df_performance_dfg, df_dfg, left_index=True, right_index=True)
            df_merged = df_merged[['frequency', 'mean', 'median', 'min', 'max', 'stdev']]
            df_merged = df_merged.rename(columns={'mean': 'mean duration', 'median': 'median duration', 'min': 'min duration', 'max': 'max duration', 'stdev': 'stdev duration'})
            df_merged = df_merged.reset_index(names=['from', 'to'])
            df_merged = df_merged.sort_values(by='frequency', ascending=False)
            #make all columns strings
            df_merged = df_merged.astype(str)
            #apply make_text_from_seconds to columns with time
            df_merged['mean duration'] = df_merged['mean duration'].apply(lambda x: make_text_from_seconds(float(x)))
            df_merged['median duration'] = df_merged['median duration'].apply(lambda x: make_text_from_seconds(float(x)))
            df_merged['min duration'] = df_merged['min duration'].apply(lambda x: make_text_from_seconds(float(x)))
            df_merged['max duration'] = df_merged['max duration'].apply(lambda x: make_text_from_seconds(float(x)))
            df_merged['stdev duration'] = df_merged['stdev duration'].apply(lambda x: make_text_from_seconds(float(x)))
            st.dataframe(df_merged, hide_index=True)

            #user settings for charts
            st.subheader("Settings for visualizations")
            visualization_settings_col1, visualization_settings_col2, visualization_settings_col3 = st.columns(3)
            
            chosen_visualizations = []
            with visualization_settings_col1:
                list_of_visualizations = ["Performance Graph", "Frequency Graph"]
                chosen_visualizations = st.pills("Select visualizations to show", list_of_visualizations, selection_mode="multi", default=["Performance Graph", "Frequency Graph"])
            
            chosen_aggregation_measure = "mean"
            with visualization_settings_col2:
                list_of_aggregation_measures = ["mean", "median", "min", "max"]
                chosen_aggregation_measure = st.pills("Select an aggregation measure", list_of_aggregation_measures, default="mean")
            
            chosen_rankdir = "LR"
            with visualization_settings_col3:
                list_of_rankdir = ["left-right", "top-down"]
                chosen_rankdir = st.pills("Select a rank direction", list_of_rankdir, default="left-right")
                if chosen_rankdir == "left-right":
                    chosen_rankdir = "LR"
                else:
                    chosen_rankdir = "TB"

            #show visualizations
            if "Performance Graph" in chosen_visualizations:
                st.subheader("Performance Directly-Follows Graph")
                pm4py.vis.save_vis_performance_dfg(performance_dfg, start_activities_perf_dfg, end_activities_perf_dfg, TEMPORARY_FILES_FOLDER_NAME + "/" + "performance_dfg.png", 
                                                aggregation_measure=chosen_aggregation_measure, rankdir=chosen_rankdir)
                st.image(TEMPORARY_FILES_FOLDER_NAME + "/" + "performance_dfg.png")

            if "Frequency Graph" in chosen_visualizations:
                st.subheader("Directly-Follows Graph with frequencies")
                pm4py.vis.save_vis_dfg(dfg, start_activities_dfg, end_activities_dfg, TEMPORARY_FILES_FOLDER_NAME + "/" + "dfg.png", rankdir=chosen_rankdir)
                st.image(TEMPORARY_FILES_FOLDER_NAME + "/" + "dfg.png")
        else:
            st.write("No data for statistics or visualization available.")

st.divider()
st.caption("""
           Source code available at [GitHub](https://github.com/Sebastian-Koerth/process-mining-made-easy).  
           This app is using the [PM4Py](https://github.com/pm4py/pm4py-core) library for process mining analysis.
           """)