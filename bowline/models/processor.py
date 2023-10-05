from enum import Enum
from multiprocessing import Process, Queue
from typing import Optional, Callable

from pydantic import BaseModel

from bowline.utils.logger import get_logger

logger = get_logger(__name__)


class Signals(Enum):
    shutdown = "shutdown"


class Processor:
    def __init__(self,
                 target_function: Callable,
                 name: str,
                 input_model: Optional[BaseModel] = None,
                 output_model: Optional[BaseModel] = None):
        self.target_function = target_function
        self.name = name
        self.input_model = input_model
        self.output_model = output_model
        self._signal_queue = Queue()
        self._input_queue = None
        self._output_queue = None
        self._process = None
        # Run setup
        self._setup()

    def start(self):
        if self._process:
            raise RuntimeError(f"This processor has already been started. You cannot start it again.")
        self._process = Process(target=self._run,
                                args=[self.name, self.target_function, self._signal_queue, self._input_queue,
                                      self._output_queue])
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

    def get_output(self) -> Optional[BaseModel]:
        if not self._output_queue or self._output_queue.empty():
            return None
        return self._output_queue.get()

    def has_output(self) -> bool:
        return self._output_queue and not self._output_queue.empty()

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
        return self._output_queue

    def set_output_queue(self, queue: Queue):
        self._output_queue = queue

    def _setup(self):
        if self.input_model:
            self._input_queue = Queue()
        if self.output_model:
            self._output_queue = Queue()

    @staticmethod
    def _run(processor_name: str,
             target_function: Callable,
             signal_queue: Queue,
             input_queue: Optional[Queue] = None,
             output_queue: Optional[Queue] = None):
        while True:
            # Check for data from the signal queue
            if not signal_queue.empty():
                signal = signal_queue.get()
                if signal == Signals.shutdown:
                    print(f"Shutting down processor {processor_name}")
                    return
            # Check for input if an input queue exists. If so, get input data from queue
            result = None
            if input_queue:
                if not input_queue.empty():
                    input = input_queue.get()
                    result = target_function(input)
            # Otherwise, call target function without arguments
            else:
                result = target_function()
            # If an output queue exists, push the result to the output queue
            if output_queue and result:
                output_queue.put(result)
