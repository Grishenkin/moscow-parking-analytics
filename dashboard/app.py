
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

st.set_page_config(
    page_title="Moscow Parking Analytics",
    page_icon="🅿️",
    layout="wide",
)

ROOT_DIR = Path(__file__).resolve().parents[1]
GOLD_DIR = ROOT_DIR / "data" / "gold"


# ---------------------------------------------------------
# Load Gold marts
# ---------------------------------------------------------

@st.cache_data
def load_data():
    district_summary = pd.read_parquet(
        GOLD_DIR / "district_summary"
    )

    adm_area_summary = pd.read_parquet(
        GOLD_DIR / "adm_area_summary"
    )

    object_type_summary = pd.read_parquet(
        GOLD_DIR / "object_type_summary"
    )

    parking_map = pd.read_parquet(
        GOLD_DIR / "parking_map"
    )

    return (
        district_summary,
        adm_area_summary,
        object_type_summary,
        parking_map,
    )


(
    district_df,
    adm_area_df,
    object_type_df,
    map_df,
) = load_data()


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

TYPE_NAMES = {
    "PAID_PARKING": "Платные парковки",
    "TAXI_PARKING": "Парковки такси",
    "INTERCEPT_PARKING": "Перехватывающие парковки",
}


def get_type_value(object_type, column):
    row = object_type_df[
        object_type_df["object_type"] == object_type
    ]

    if row.empty:
        return 0

    return int(row.iloc[0][column])


# ---------------------------------------------------------
# Header
# ---------------------------------------------------------

st.title("🅿️ Парковочная инфраструктура Москвы")

st.caption(
    "Аналитическая платформа на основе открытых данных "
    "Правительства Москвы"
)


# ---------------------------------------------------------
# Sidebar
# ---------------------------------------------------------

st.sidebar.header("Фильтры")

available_areas = sorted(
    map_df["adm_area"].dropna().unique().tolist()
)

selected_area = st.sidebar.selectbox(
    "Административный округ",
    ["Все округа"] + available_areas,
)

selected_type = st.sidebar.selectbox(
    "Тип парковки",
    [
        "Все типы",
        "PAID_PARKING",
        "TAXI_PARKING",
        "INTERCEPT_PARKING",
    ],
    format_func=lambda x: TYPE_NAMES.get(x, x),
)


# ---------------------------------------------------------
# Tabs
# ---------------------------------------------------------

tab_overview, tab_districts, tab_map = st.tabs(
    [
        "Обзор",
        "Районы",
        "Карта",
    ]
)


# =========================================================
# OVERVIEW
# =========================================================

with tab_overview:

    total_objects = int(
        object_type_df["object_count"].sum()
    )

    total_capacity = int(
        object_type_df["total_capacity"].sum()
    )

    paid_count = get_type_value(
        "PAID_PARKING",
        "object_count",
    )

    taxi_count = get_type_value(
        "TAXI_PARKING",
        "object_count",
    )

    intercept_count = get_type_value(
        "INTERCEPT_PARKING",
        "object_count",
    )

    st.subheader("Основные показатели")

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric(
        "Всего объектов",
        f"{total_objects:,}".replace(",", " "),
    )

    col2.metric(
        "Парковочных мест",
        f"{total_capacity:,}".replace(",", " "),
    )

    col3.metric(
        "Платные парковки",
        f"{paid_count:,}".replace(",", " "),
    )

    col4.metric(
        "Парковки такси",
        f"{taxi_count:,}".replace(",", " "),
    )

    col5.metric(
        "Перехватывающие",
        f"{intercept_count:,}".replace(",", " "),
    )

    st.divider()

    left, right = st.columns(2)

    with left:

        st.subheader(
            "Парковочные объекты по административным округам"
        )

        area_chart_df = (
            adm_area_df
            .sort_values(
                "total_objects",
                ascending=True,
            )
        )

        fig_area = px.bar(
            area_chart_df,
            x="total_objects",
            y="adm_area",
            orientation="h",
            labels={
                "total_objects": "Количество объектов",
                "adm_area": "Административный округ",
            },
        )

        fig_area.update_layout(
            height=500,
            showlegend=False,
        )

        st.plotly_chart(
            fig_area,
            use_container_width=True,
        )

    with right:

        st.subheader(
            "Структура парковочной инфраструктуры"
        )

        type_chart_df = object_type_df.copy()

        type_chart_df["type_name"] = (
            type_chart_df["object_type"]
            .map(TYPE_NAMES)
        )

        fig_type = px.pie(
            type_chart_df,
            names="type_name",
            values="object_count",
            hole=0.4,
        )

        fig_type.update_layout(
            height=500,
        )

        st.plotly_chart(
            fig_type,
            use_container_width=True,
        )

    st.subheader(
        "Вместимость по типам парковок"
    )

    capacity_df = object_type_df.copy()

    capacity_df["type_name"] = (
        capacity_df["object_type"]
        .map(TYPE_NAMES)
    )

    fig_capacity = px.bar(
        capacity_df,
        x="type_name",
        y="total_capacity",
        labels={
            "type_name": "Тип парковки",
            "total_capacity": "Количество парковочных мест",
        },
    )

    st.plotly_chart(
        fig_capacity,
        use_container_width=True,
    )


# =========================================================
# DISTRICTS
# =========================================================

with tab_districts:

    st.subheader("Аналитика по районам")

    district_filtered = district_df.copy()

    if selected_area != "Все округа":
        district_filtered = district_filtered[
            district_filtered["adm_area"]
            == selected_area
        ]

    top_districts = (
        district_filtered
        .sort_values(
            "total_objects",
            ascending=False,
        )
        .head(15)
        .sort_values(
            "total_objects",
            ascending=True,
        )
    )

    fig_district = px.bar(
        top_districts,
        x="total_objects",
        y="district",
        orientation="h",
        hover_data=[
            "taxi_parking_count",
            "intercept_parking_count",
            "paid_parking_count",
            "total_capacity",
        ],
        labels={
            "total_objects": "Количество объектов",
            "district": "Район",
        },
    )

    fig_district.update_layout(
        height=600,
    )

    st.plotly_chart(
        fig_district,
        use_container_width=True,
    )

    st.subheader("Данные по районам")

    st.dataframe(
        district_filtered
        .sort_values(
            "total_objects",
            ascending=False,
        ),
        use_container_width=True,
        hide_index=True,
    )


# =========================================================
# MAP
# =========================================================

with tab_map:

    st.subheader("Карта парковочной инфраструктуры")

    filtered_map = map_df.copy()

    if selected_area != "Все округа":
        filtered_map = filtered_map[
            filtered_map["adm_area"]
            == selected_area
        ]

    if selected_type != "Все типы":
        filtered_map = filtered_map[
            filtered_map["object_type"]
            == selected_type
        ]

    filtered_map["type_name"] = (
        filtered_map["object_type"]
        .map(TYPE_NAMES)
    )

    st.write(
        "Объектов на карте:",
        len(filtered_map),
    )

    if not filtered_map.empty:

        fig_map = px.scatter_mapbox(
            filtered_map,
            lat="latitude",
            lon="longitude",
            color="type_name",
            hover_name="parking_name",
            hover_data={
                "adm_area": True,
                "district": True,
                "address": True,
                "capacity": True,
                "latitude": False,
                "longitude": False,
            },
            zoom=9,
            height=700,
        )

        fig_map.update_layout(
            mapbox_style="open-street-map",
            margin={
                "r": 0,
                "t": 0,
                "l": 0,
                "b": 0,
            },
        )

        st.plotly_chart(
            fig_map,
            use_container_width=True,
        )

    else:
        st.warning(
            "Для выбранных фильтров объекты отсутствуют."
        )


# ---------------------------------------------------------
# Footer
# ---------------------------------------------------------

st.divider()

st.caption(
    "Источник данных: Портал открытых данных "
    "Правительства Москвы. "
    "Обработка и построение витрин: Apache Spark."
)
