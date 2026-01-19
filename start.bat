@echo off

REM Запуск Streamlit-дашборда
cd /d "C:\projects\arctic_vacancies"
start /min python -m streamlit run dash.py --server.port=8501

echo === ГОТОВО! Дашборд: http://localhost:8501 ===