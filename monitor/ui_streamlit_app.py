import streamlit as st
st.set_page_config(
    page_title="Prediction Pipeline Dashboard",
    layout="wide",
)

from streamlit_echarts import st_pyecharts
from pyecharts import options as opts
from pyecharts.charts import Pie, Line
import pandas as pd
from datetime import datetime
import time

from liteindex import DefinedIndex

PREDICTIONS_INDEX = DefinedIndex("predictions_index", db_path="monitor.db")

# Mocked functions

def get_success_and_failure_counts(n_hours):
    current_time = time.time()
    for unique_id,  PREDICTIONS_INDEX.sort_by_key(key="created_at"):
        if PREDICTIONS_INDEX["created_at"] >= current_time - n_hours * 3600:
            break
    return {"success": 150, "failure": 50}

def get_failed_unique_ids_and_pipelines(n_hours):
    return [
        {"unique_id": "id1", "created_at": time.time() - 3600, "failed_pipeline": "tiling"},
        {"unique_id": "id2", "created_at": time.time() - 7200, "failed_pipeline": "translation"},
    ]

def get_pipeline_times_by_unique_id(n_hours):
    return [
        {"unique_id": "id1", "pipeline_name": "tiling", "time_taken": 3},
        {"unique_id": "id1", "pipeline_name": "translation", "time_taken": 5},
        {"unique_id": "id2", "pipeline_name": "tiling", "time_taken": 4},
        {"unique_id": "id2", "pipeline_name": "translation", "time_taken": 6},
    ]

def get_average_pipeline_times(n_hours):
    return {"tiling": 3.5, "translation": 5.5}

def get_pipeline_processing_times(n_hours):
    return {"tiling": [2, 3, 4, 5], "translation": [4, 5, 6, 7]}

def get_failure_count_for_time_range(n_hours):
    return 10

# Streamlit app

st.title("Prediction Pipeline Dashboard")

n_hours = st.number_input("Enter the number of hours:", min_value=1, value=8)

col1, col2, col3 = st.columns(3)

with col1:
    st.header("Success & Failure Counts")
    success_failure_counts = get_success_and_failure_counts(n_hours)
    data = [list(success_failure_counts.items())]

    pie_chart = (
        Pie()
        .add("", data, radius=["40%", "75%"], rosetype="area")
        .set_global_opts(title_opts=opts.TitleOpts(title="Success & Failure Counts"))
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
    )
    st_pyecharts(pie_chart)

with col2:
    st.header("Average Time Taken by Each Predictor")
    average_pipeline_times = get_average_pipeline_times(n_hours)
    data = [list(average_pipeline_times.items())]

    bar_chart = (
        Pie()
        .add("", data, radius=["40%", "75%"], rosetype="radius")
        .set_global_opts(title_opts=opts.TitleOpts(title="Average Time Taken by Each Predictor"))
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
    )
    st_pyecharts(bar_chart)

with col3:
    st.header("Failure Count in the Last N Hours")
    failure_count = get_failure_count_for_time_range(n_hours)
    st.write(f"Failure count in the last {n_hours} hours: {failure_count}")

st.header("Processing Times for Each Pipeline")

pipeline_processing_times = get_pipeline_processing_times(n_hours)
data = pd.DataFrame(pipeline_processing_times)

line_chart = (
    Line()
    .add_xaxis(data.index.tolist())
    .add_yaxis("Tiling", data["tiling"].tolist(), stack="stack1")
    .add_yaxis("Translation", data["translation"].tolist(), stack="stack1")
    .set_global_opts(title_opts=opts.TitleOpts(title="Processing Times for Each Pipeline"))
)

st_pyecharts(line_chart)

failed_unique_ids_and_pipelines = get_failed_unique_ids_and_pipelines(n_hours)
failed_df = pd.DataFrame(failed_unique_ids_and_pipelines)
failed_df['created_at'] = failed_df['created_at'].apply(lambda x: datetime.fromtimestamp(x))

st.header("Failed Unique IDs and Failed Pipeline Names")
st.write(failed_df)
