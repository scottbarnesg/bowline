import time
from enum import Enum
from multiprocessing import Process, Queue
from typing import Optional, Callable, Union, List, Type

from pydantic import BaseModel

from bowline.models.result import Result
from bowline.utils.logger import get_logger

logger = get_logger(__name__)


class Signals(Enum):
    shutdown = "shutdown"


class Processor:
    min_sleep_time = 0.001
    max_sleep_time = 1

    def __init__(self,
                 target_function: Callable,
                 name: str,
                 input_model: Optional[Type[BaseModel]] = None,
                 output_model: Optional[Type[BaseModel]] = None):
        self.target_function = target_function
        self.name = name
        self.input_model = input_model
        self.output_model = output_model
        self._signal_queue = Queue()
        self._input_queue = None
        self._output_queues = []
        self._process = None
        # Run setup
        self._setup()

    def start(self):
        if self._process:
            raise RuntimeError(f"This processor has already been started. You cannot start it again.")
        self._process = Process(target=self._run,
                                args=(self.name, self.target_function, self._signal_queue, self._input_queue,
                                      self._output_queues))
        logger.info(f"Starting processor {self.name}...")
        self._process.start()

    def shutdown(self):
        logger.info(f"Shutting down processor {self.name}...")
        self._signal_queue.put(Signals.shutdown)

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
            raise ValueError(f"Processor has multiple output queues. Unable to determine which queue should be retrieved.")
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

    def _setup(self):
        if self.input_model:
            self._input_queue = Queue()
        if self.output_model:
            # By default, create one output queue. This can be overridden using add_output_queue()
            self._output_queues = [Queue()]

    @staticmethod
    def _run(processor_name: str,
             target_function: Callable,
             signal_queue: Queue,
             input_queue: Optional[Queue] = None,
             output_queues: Optional[List[Queue]] = None):
        sleep_time = Processor.min_sleep_time
        while True:
            # Check for data from the signal queue
            if not signal_queue.empty():
                signal = signal_queue.get()
                if signal == Signals.shutdown:
                    logger.info(f"Shutting down background process for processor {processor_name}...")
                    return
            # Check for input if an input queue exists. If so, get input data from queue
            result = None
            if input_queue:
                if not input_queue.empty():
                    input = input_queue.get()
                    result = target_function(input)
                    # Reset sleep time since we got data
                    sleep_time = Processor.min_sleep_time
                else:
                    sleep_time = Processor.update_sleep_time(sleep_time)
                    time.sleep(sleep_time)
            # Otherwise, call target function without arguments
            else:
                result = target_function()
            # If an output queue exists, push the result to the output queue
            if output_queues and result:
                for output_queue in output_queues:
                    output_queue.put(result)

    @staticmethod
    def update_sleep_time(current_sleep_time: Union[float, int]) -> Union[float, int]:
        if current_sleep_time <= Processor.max_sleep_time:
            return current_sleep_time * 2
        else:
            return Processor.max_sleep_time
