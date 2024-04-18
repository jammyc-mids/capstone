from workerpool import workerPool
from concurrent import futures

worker_table = []
with futures.ThreadPoolExecutor() as executor:
    worker_table.append(executor.submit(workerPool().start_classification_workers))
    worker_table.append(executor.submit(workerPool().start_disaggregation_workers))
    worker_table.append(executor.submit(workerPool().start_result_recorder_workers))

futures.wait(worker_table)
