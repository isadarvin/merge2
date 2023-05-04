FROM python:3.9

WORKDIR /merge2/
COPY . ./
ADD main.py .
ADD requirements.txt .

RUN pip install -r requirements.txt
CMD ["python", "./main.py"]