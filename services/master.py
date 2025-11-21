from rq import Queue
from redis import Redis

redis_conn = Redis(host='192.168.80.49', port=6379, db=0)
queue = Queue('airflow',connection=redis_conn)

def do_task(name):
    print(f"Progressing task for {name}")

    
if __name__ == "__main__":
    job = queue.enqueue(do_task, 'Louis')
    print(f"Job enqueued: {job.id}")
