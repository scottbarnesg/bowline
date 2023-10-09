import queue
from enum import Enum
from multiprocessing import Process, Queue, Value
from typing import Optional, Callable, List, Type, Dict

from pydantic import BaseModel

from bowline.models.result import Result
from bowline.utils.logger import get_logger

logger = get_logger(__name__)


class Signals(Enum):
    shutdown = "shutdown"


class Stats(Enum):
    inputs_processed = "inputs_processed"


class Processor:
    def __init__(self,
                 target_function: Callable,
                 name: str,
                 input_model: Optional[Type[BaseModel]] = None,
                 output_model: Optional[Type[BaseModel]] = None,
                 instances: Optional[int] = 1):
        self.target_function = target_function
        self.name = name
        self.input_model = input_model
        self.output_model = output_model
        self.instances = instances
        self._signal_queues = [Queue() for _ in range(instances)]
        self._input_queue = None
        self._output_queues = []
        self._processes = []
        self._stats = dict()
        # Run setup
        self._setup()

    def start(self):
        if self._processes:
            raise RuntimeError(f"This processor has already been started. You cannot start it again.")
        for instance in range(self.instances):
            process = Process(target=self._run,
                              args=(self.name, instance, self.target_function,
                                    self._stats[instance][Stats.inputs_processed.value], self._signal_queues[instance],
                                    self._input_queue, self._output_queues))
            self._processes.append(process)
        logger.info(f"Starting processor {self.name}...")
        for process in self._processes:
            process.start()

    def shutdown(self):
        logger.info(f"Shutting down processor {self.name}...")
        alive_processes = [process for process in self._processes if process.is_alive()]
        while alive_processes:
            for i in range(self.instances):
                if self._processes[i].is_alive():
                    self._signal_queues[i].put(Signals.shutdown.value)
                    logger.info(f"Waiting for process {self.name} instance {i} to shut down...")
                    self._processes[i].join(timeout=10)
            alive_processes = [process for process in self._processes if process.is_alive()]
            logger.info(alive_processes)

    def push_input(self, input: BaseModel):
        if not self.input_model:
            raise ValueError("No input model was specified. Cannot push data to this process.")
        if not self._input_queue:
            raise ValueError("Input queue does not exists. Did you specify an input model?")
        self._input_queue.put(input)

    def get_output(self) -> Optional[Result]:
        if not self._output_queues or all(output_queue.empty() for output_queue in self._output_queues):
            return None
        for output_queue in self._output_queues:
            if not output_queue.empty():
                return Result(processor=self.name, output=output_queue.get())

    def has_output(self) -> bool:
        return self._output_queues and any(not output_queue.empty() for output_queue in self._output_queues)

    def get_name(self) -> str:
        return self.name

    def get_input_model(self) -> Optional[BaseModel]:
        return self.input_model

    def get_output_model(self) -> Optional[BaseModel]:
        return self.output_model

    def get_input_queue(self) -> Optional[Queue]:
        return self._input_queue

    def set_input_queue(self, queue: Queue):
        self._input_queue = queue

    def get_output_queue(self) -> Optional[Queue]:
        if len(self._output_queues) > 1:
            raise ValueError(
                f"Processor has multiple output queues. Unable to determine which queue should be retrieved.")
        if not self._output_queues:
            return None
        return self._output_queues[0]

    def get_output_queues(self) -> Optional[List[Queue]]:
        return self._output_queues

    def add_output_queue(self, queue: Queue):
        if queue in self._output_queues:
            raise ValueError(f"{queue} is already set as an output queue.")
        self._output_queues.append(queue)

    def clear_output_queues(self):
        self._output_queues = []

    def get_stats(self) -> Dict[int, any]:
        return self._stats

    def _setup(self):
        if self.input_model:
            self._input_queue = Queue()
        if self.output_model:
            # By default, create one output queue. This can be overridden using add_output_queue()
            self._output_queues = [Queue()]
        for i in range(self.instances):
            self._stats[i] = {
                Stats.inputs_processed.value: Value('i', 0)
            }

    @staticmethod
    def _run(processor_name: str,
             instance: int,
             target_function: Callable,
             inputs_processed: Value,
             signal_queue: Queue,
             input_queue: Optional[Queue] = None,
             output_queues: Optional[List[Queue]] = None):
        while True:
            # Check for data from the signal queue
            if not signal_queue.empty():
                signal = signal_queue.get()
                if signal == Signals.shutdown.value:
                    logger.info(f"Shutting down instance {instance} for processor {processor_name}...")
                    return
            # Check for input if an input queue exists. If so, get input data from queue
            result = None
            if input_queue:
                try:
                    # Get data from input queue and run the target function with that input
                    input = input_queue.get(timeout=1)
                    result = target_function(input)
                    # Update stats
                    with inputs_processed.get_lock():
                        inputs_processed.value += 1
                except queue.Empty:
                    pass
            # Otherwise, call target function without arguments
            else:
                result = target_function()
            # If an output queue exists, push the result to the output queue
            if output_queues and result:
                for output_queue in output_queues:
                    output_queue.put(result)
