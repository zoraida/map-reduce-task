Map Reduce Task - Word Count
=================================

Requirements
------------

To install and run these examples you need:

- Python 2.7 or 3.3+
- virtualenv (not required if you are using Python 3.4)
- git (only to clone this repository)

Installation
------------

The commands below set everything up to run the examples:

    $ git clone https://github.com/zoraida/map-reduce-task.git
    $ cd map-reduce-task
    $ virtualenv venv
    $ . venv/bin/activate
    (venv) pip install -r requirements.txt
    $ python run_driver.py --mappers 4 --reducers 2 --port 5000 --i_path file1 file2 file3 --m_path /path/to/intermediate/dir  --o_path /path/to/output/dir
    $ python run_worker.py --driver http://localhost:5000 # You need to include the protocol scheme

How it works
------------
The driver exposes an API:

    $ PUT /task/{type}/{id} for requesting a task to be run by a worker
    $ POST /task/{type}/{id} for update task status to FINISHED
    $ GET /task/{type}/{id} for getting taks status info


- For now I want to try to keep both driver and worker simple:
  - All tasks states are keep in memory.
  - The driver keeps BLOCKED, READY and RUNNING queues. Initially only map tasks are on the READY queue whereas
  reduce tasks are on the BLOCKED one. 
  - When a worker request a task(PUT /task request):
    - Only when all the map tasks have finished reduce tasks start: if all map tasks have finished and reduce tasks are into
      the BLOCKED queue then they are moved to the READY queue.
    - If there is a READY task then it is given to the worker and moved to the RUNNING queue.
    - If there is no READY task but there are both, RUNNING and BLOCKING tasks, then the worker is invited to try later.
    - If all the tasks are in FINISHED state, then the worker is notified thus it will exit successfully. If all the
    workers have been already notified then the driver will exit successfully too.

  - When a worker notifies a finished task(POST /task request):
    - The task is removed from the RUNNING queue and its state is changed to FINISHED.
  
Some thoughts
-------------
- I would like to have implemented other approach for reduce tasks but for now, since a given map task may generate 
word counts for any word (thus any reducer) and data cannot be sent among processes I found it the simplest even not 
the most efficient. I considered allowing the driver to assign a reduce task to a requester worker before all map tasks 
completion and just let it monitor its input files. However, if any running map fails and there are no more workers
available, it would produce a deadlock :( Some preemption mechanism???
- Thinking on how to acknowledge the driver that workers are still alive:
  - providing a heartbeat end point to notice the driver that workers are still alive. Still not sure on
  timings and how to doit with a single threaded process on the worker. Maybe using multithread library and switching CPU
  between the main thread and a simple one that notifies the driver so I need some mechanism to be "regular".
  - Is the driver who knows the pid of the workers and pulls on the OS.

Other considerations
--------------------
- For the last 6 years I have been programing in either Scala or Java. I took this opportunity to play around with
Python so be nice with me :)
- I guess with gRPC I would be able to solve some the questions raised above. However, I found Flask easier to use, so 
I choose it instead.


Examples of API usage
---------------------

Requesting a task:
```
curl -H "Accept: application/json" -H "Content-Type: application/json" -H "worker-pid: 1234" -X PUT http://localhost:5000/task
{
  "i_path": [
    "file2"
  ],
  "id": 1,
  "job_status": 1,
  "job_uuid": "af2254b5-4e08-480f-8ae5-addba4014584",
  "n_buckets": 1,
  "o_path": "/path/intermediate",
  "status": 3,
  "type": "mapper"
}
```

Getting the status of a task:
```
curl -H "Accept: application/json" -H "Content-Type: application/json" -H "worker-pid: 1234" -X GET http://localhost:5000/task/mapper/0/status
{
  "status": 2
}
```

Updating the status of a task:

```
curl -H "Accept: application/json" -H "Content-Type: application/json" -H "worker-pid: 1234" -X POST http://localhost:5000/task/mapper/1/status -d '{"status":4}'
```