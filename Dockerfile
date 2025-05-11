FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY yn_dashboard.py .
COPY yn_processed_kpis/ yn_processed_kpis/

EXPOSE 8501

CMD ["streamlit", "run", "yn_dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]