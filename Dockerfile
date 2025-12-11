FROM python:3.12-slim
COPY . /app
WORKDIR /app
RUN pip3 install -r requirements.txt
EXPOSE 8080
COPY . /app
RUN pip3 install -r requirements.txt
ENTRYPOINT ["streamlit", "run", "streamlit.py"]
