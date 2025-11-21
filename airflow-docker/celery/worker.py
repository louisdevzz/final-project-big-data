from celery import Celery

app = Celery(
    'myworker',
    broker='redis://192.168.80.49:6379/0',
    backend='db+postgresql://airflow:airflow@192.168.80.49/airflow'
)

@app.task
def heavy_job(x, y):
    return x + y
