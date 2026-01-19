import streamlit as st
import pandas as pd
import os

# Путь к вашему файлу
PARQUET_PATH = r"C:\projects\arctic_vacancies\data\superset\arctic_vacancies.parquet"

st.set_page_config(page_title="Арктический рынок труда", layout="wide")
st.title("📊 Арктический рынок труда")

if os.path.exists(PARQUET_PATH):
    df = pd.read_parquet(PARQUET_PATH)

    # Убедимся, что salary_avg числовая и без NaN
    df = df[pd.to_numeric(df["salary_avg"], errors="coerce").notnull()]
    df["salary_avg"] = pd.to_numeric(df["salary_avg"])

    # === САЙДБАР С ФИЛЬТРАМИ ===
    st.sidebar.header("🔍 Фильтры")

    # Фильтр по регионам
    all_regions = sorted(df["region"].unique())
    selected_regions = st.sidebar.multiselect(
        "регионы",
        options=all_regions,
        default=all_regions  # Все регионы по умолчанию
    )

    # Фильтр по опыту работы
    all_experience = sorted(df["experience"].dropna().unique())
    selected_experience = st.sidebar.multiselect(
        "Опыт работы",
        options=all_experience,
        default=all_experience
    )

    # Фильтр по типу занятости (ТОЛЬКО employment_type)
    all_employment = sorted(df["employment_type"].dropna().unique())
    selected_employment = st.sidebar.multiselect(
        "Тип занятости",
        options=all_employment,
        default=all_employment
    )

    # Фильтр по зарплате
    min_salary = int(df["salary_avg"].min())
    max_salary = int(df["salary_avg"].max())
    salary_range = st.sidebar.slider(
        "Зарплата (₽)",
        min_value=min_salary,
        max_value=max_salary,
        value=(min_salary, max_salary)
    )

    # Применяем фильтры
    filtered_df = df.copy()
    if selected_regions:
        filtered_df = filtered_df[filtered_df["region"].isin(selected_regions)]
    if selected_experience:
        filtered_df = filtered_df[filtered_df["experience"].isin(selected_experience)]
    if selected_employment:
        filtered_df = filtered_df[filtered_df["employment_type"].isin(selected_employment)]
    filtered_df = filtered_df[
        (filtered_df["salary_avg"] >= salary_range[0]) &
        (filtered_df["salary_avg"] <= salary_range[1])
    ]

    if filtered_df.empty:
        st.warning("Нет данных по выбранным фильтрам")
    else:
        st.markdown(f"**Найдено вакансий:** {len(filtered_df):,}")

        # === ТОП-10 ПРОФЕССИЙ ===
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Топ-10 профессий (по количеству)")
            top_prof_count = filtered_df["profession"].value_counts().head(10)
            st.bar_chart(top_prof_count)

        # === РАСПРЕДЕЛЕНИЕ ЗАРПЛАТ (ГИСТОГРАММА) ===
        with col2:
            st.subheader("Распределение зарплат")
            # Создаём гистограмму
            salary_bins = pd.cut(filtered_df["salary_avg"], bins=20)
            hist_data = salary_bins.value_counts().sort_index()
            # Обязательно преобразуем в строки!
            hist_data.index = hist_data.index.astype(str)
            st.bar_chart(hist_data)

        # === СРЕДНЯЯ ЗАРПЛАТА ПО РЕГИОНАМ ===
        st.subheader("Средняя зарплата по регионам")
        salary_by_region = filtered_df.groupby("region")["salary_avg"].mean().sort_values(ascending=False)
        st.bar_chart(salary_by_region)

        # === ЗАРПЛАТА ПО ОПЫТУ РАБОТЫ ===
        st.subheader("Средняя зарплата по опыту работы")
        salary_by_experience = filtered_df.groupby("experience")["salary_avg"].mean().sort_values(ascending=False)
        st.bar_chart(salary_by_experience)

        # === ТИП ЗАНЯТОСТИ ===
        col3, col4 = st.columns(2)
        with col3:
            st.subheader("Распределение по типу занятости")
            employment_dist = filtered_df["employment_type"].value_counts()
            st.bar_chart(employment_dist)

        # === САМАЯ ВЫСОКООПЛАЧИВАЕМАЯ ПРОФЕССИЯ ===
        with col4:
            st.subheader("🏆 Самая высокооплачиваемая профессия")
            avg_salary_by_prof = filtered_df.groupby("profession")["salary_avg"].agg(["mean", "count"])
            avg_salary_by_prof = avg_salary_by_prof[avg_salary_by_prof["count"] >= 2]  # Минимум 2 вакансии
            if not avg_salary_by_prof.empty:
                top_paid_prof = avg_salary_by_prof.sort_values("mean", ascending=False).iloc[0]
                st.metric(
                    label=top_paid_prof.name,
                    value=f"₽{top_paid_prof['mean']:,.0f}",
                    delta=f"{int(top_paid_prof['count'])} вакансий"
                )
            else:
                st.info("Недостаточно данных")

        # === ТАБЛИЦА ДАННЫХ ===
        st.subheader("📋 Данные по вакансиям")
        display_cols = ["profession", "region", "salary_avg", "experience", "employment_type"]
        st.dataframe(filtered_df[display_cols].head(20), width='stretch')

else:
    st.error("❌ Файл данных не найден!")
    st.markdown(f"Проверьте путь: `{PARQUET_PATH}`")