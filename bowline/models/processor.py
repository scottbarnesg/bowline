import json
import queue
from enum import Enum
from multiprocessing import Process, Queue, Value
from typing import Optional, Callable, List, Type, Dict

from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel

from bowline.models.result import Result
from bowline.utils.logger import get_logger

logger = get_logger(__name__)


class Signals(Enum):
    shutdown = "shutdown"


class Stats(Enum):
    inputs_processed = "inputs_processed"


class BaseProcessor:
    def __init__(self,
                 target_function: Callable,
                 name: str,
                 input_model: Optional[Type[BaseModel]] = None,
                 output_model: Optional[Type[BaseModel]] = None,
                 setup_function: Optional[Callable] = None,
                 instances: Optional[int] = 1):
        # Baseline parameters
        self.target_function = target_function
        self.name = name
        self.input_model = input_model
        self.output_model = output_model
        self.setup_function = setup_function
        self.instances = instances
        # Create signal queue for internal communication, such as shutdown signals
        self._signal_queues = [Queue() for _ in range(self.instances)]
        # List of processes, of length instances.
        self._processes = []
        # Data structure to hold statistics
        self._stats = dict()

    def get_name(self) -> str:
        return self.name

    def get_input_model(self) -> Optional[BaseModel]:
        return self.input_model

    def get_output_model(self) -> Optional[BaseModel]:
        return self.output_model

    def get_stats(self) -> Dict[int, any]:
        return self._stats

    def start(self):
        raise NotImplementedError("The start method has not been implemented for this Processor.")

    def get_input_from(self, input_processor: "BaseProcessor"):
        raise NotImplementedError("The get_input_from method has not been implemented for this Processor.")

    def shutdown(self):
        logger.info(f"Shutting down processor {self.name}...")
        alive_processes = [process for process in self._processes if process.is_alive()]
        while alive_processes:
            for i in range(self.instances):
                if self._processes[i].is_alive():
                    self._signal_queues[i].put(Signals.shutdown.value)
                    logger.debug(f"Waiting for process {self.name} instance {i} to shut down...")
                    self._processes[i].join(timeout=10)
            alive_processes = [process for process in self._processes if process.is_alive()]

    def push_input(self, input: BaseModel):
        raise NotImplementedError("The push_input method has not been implemented for this Processor.")

    def get_output(self) -> Optional[Result]:
        raise NotImplementedError("The get_output method has not been implemented for this Processor.")

    def has_output(self) -> bool:
        raise NotImplementedError("The has_output method has not been implemented for this Processor.")


class Processor(BaseProcessor):
    def __init__(self,
                 target_function: Callable,
                 name: str,
                 input_model: Optional[Type[BaseModel]] = None,
                 output_model: Optional[Type[BaseModel]] = None,
                 setup_function: Optional[Callable] = None,
                 instances: Optional[int] = 1):
        super().__init__(target_function, name, input_model, output_model, setup_function, instances)
        # Input and output queues for data
        self._input_queue = None
        self._output_queues = []
        # Run setup
        self._setup()

    def start(self):
        if self._processes:
            raise RuntimeError(f"This processor has already been started. You cannot start it again.")
        for instance in range(self.instances):
            process = Process(target=self._run,
                              args=(self.name, instance,
                                    self.target_function,
                                    self._stats[instance][Stats.inputs_processed.value],
                                    self._signal_queues[instance],
                                    self._input_queue,
                                    self._output_queues,
                                    self.setup_function))
            self._processes.append(process)
        logger.info(f"Starting processor {self.name}...")
        for process in self._processes:
            process.start()

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

    def get_input_from(self, input_processor: "Processor"):
        new_queue = Queue()
        input_processor.add_output_queue(new_queue)
        self.set_input_queue(new_queue)

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
             inputs_processed: Value,  # TODO: Refactor this to support more stats
             signal_queue: Queue,
             input_queue: Optional[Queue] = None,
             output_queues: Optional[List[Queue]] = None,
             setup_function: Optional[Callable] = None):
        # Run setup function if present. Output from the setup function will be passed as kwargs to the target function
        kwargs = {}
        if setup_function:
            kwargs = setup_function()
        # Start processor loop
        while True:
            # Check for data from the signal queue
            if not signal_queue.empty():
                signal = signal_queue.get()
                if signal == Signals.shutdown.value:
                    logger.debug(f"Shutting down instance {instance} for processor {processor_name}...")
                    return
            # Check for input if an input queue exists. If so, get input data from queue
            result = None
            if input_queue:
                try:
                    # Get data from input queue and run the target function with that input
                    input = input_queue.get(timeout=1)
                    try:
                        result = target_function(input, **kwargs)
                    except Exception as e:
                        logger.error(f"Target function {target_function} threw an exception")
                        logger.error(e)
                    # Update stats
                    with inputs_processed.get_lock():
                        inputs_processed.value += 1
                except queue.Empty:
                    pass
            # Otherwise, call target function without arguments
            else:
                try:
                    result = target_function(**kwargs)
                except Exception as e:
                    logger.error(f"Target function {target_function} threw an exception")
                    logger.error(e)
            # If an output queue exists, push the result to the output queue
            if output_queues and result:
                for output_queue in output_queues:
                    output_queue.put(result)


class KafkaProcessor(BaseProcessor):
    def __init__(self,
                 target_function: Callable,
                 name: str,
                 kafka_boostrap_server: str,
                 input_model: Optional[Type[BaseModel]] = None,
                 output_model: Optional[Type[BaseModel]] = None,
                 setup_function: Optional[Callable] = None,
                 instances: Optional[int] = 1):
        super().__init__(target_function, name, input_model, output_model, setup_function, instances)
        self.kafka_bootstrap_server = kafka_boostrap_server
        self.kafka_consumer: Optional[KafkaConsumer] = None
        self.kafka_producer: Optional[KafkaProducer] = None
        self._input_topic = None
        self._setup()

    def get_output_topic(self) -> str:
        return f"bowline-{self.name}-output"

    def get_consumer_group(self) -> str:
        return f"bowline-{self.name}"

    def set_input_topic(self, topic: str):
        self._input_topic = topic

    def get_input_from(self, input_processor: "KafkaProcessor"):
        # Have our KafkaConsumer subscribe to the output topic of the input processor.
        #    For now, only handle one input topic, and override any existing topics
        self.set_input_topic(input_processor.get_output_topic())

    def start(self):
        if self._processes:
            raise RuntimeError(f"This processor has already been started. You cannot start it again.")
        for instance in range(self.instances):
            process = Process(target=self._run,
                              args=(self.name,
                                    instance,
                                    self.target_function,
                                    self._stats[instance][Stats.inputs_processed.value],
                                    self._signal_queues[instance],
                                    self.input_model,
                                    self._input_topic,
                                    self.get_output_topic(),
                                    self.kafka_bootstrap_server,
                                    self.get_consumer_group(),
                                    self.setup_function))
            self._processes.append(process)
        logger.info(f"Starting processor {self.name}...")
        for process in self._processes:
            process.start()

    def push_input(self, input: BaseModel):
        if not self.input_model:
            raise ValueError("No input model was specified. Cannot push data to this process.")
        # Push data to the Kafka consumer's input channel
        if not self._input_topic:
            raise ValueError("No input topic exists for this processor.")
        self.kafka_producer.send(self._input_topic, input.model_dump_json().encode())

    def _setup(self):
        # Create a KafkaProducer.
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_server)
        # Set up stats
        for i in range(self.instances):
            self._stats[i] = {
                Stats.inputs_processed.value: Value('i', 0)
            }

    @staticmethod
    def _run(processor_name: str,
             instance: int,
             target_function: Callable,
             inputs_processed: Value,  # TODO: Refactor this to support more stats
             signal_queue: Queue,
             input_model: Optional[Type[BaseModel]],
             kafka_input_topic: Optional[str],
             kafka_output_topic: Optional[str],
             kafka_bootstrap_server: Optional[str],
             kafka_consumer_group: Optional[str],
             setup_function: Optional[Callable] = None,
             ):
        # Create KafkaConsumer and KafkaProducer
        kafka_consumer = None
        if kafka_input_topic:
            kafka_consumer = KafkaConsumer(kafka_input_topic,
                                           bootstrap_servers=kafka_bootstrap_server,
                                           group_id=kafka_consumer_group)
        kafka_producer = None
        if kafka_output_topic:
            kafka_producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_server)
        # Run setup function if present. Output from the setup function will be passed as kwargs to the target function
        kwargs = {}
        if setup_function:
            kwargs = setup_function()
        # Start processor loop
        while True:
            # Check for data from the signal queue
            if not signal_queue.empty():
                signal = signal_queue.get()
                if signal == Signals.shutdown.value:
                    logger.debug(f"Shutting down instance {instance} for processor {processor_name}...")
                    return
            # Check for input if an input queue exists. If so, get input data from queue
            result = None
            if kafka_consumer:
                # Get data from input queue and run the target function with that input
                records = kafka_consumer.poll(timeout_ms=1000, max_records=1)
                if records:
                    for message in list(records.values())[0]:
                        logger.info(f"Got message: {message}")
                        try:
                            result = target_function(input_model(**json.loads(message.value.decode("utf-8"))), **kwargs)
                            logger.info(f"Result: {result}")
                        except Exception as e:
                            logger.error(f"Target function {target_function} threw an exception")
                            logger.error(e)
                        # Update stats
                        with inputs_processed.get_lock():
                            inputs_processed.value += 1
            # Otherwise, call target function without arguments
            else:
                try:
                    result = target_function(**kwargs)
                except Exception as e:
                    logger.error(f"Target function {target_function} threw an exception")
                    logger.error(e)
            # If an output queue exists, push the result to the output queue
            if kafka_producer and result:
                logger.info(f"Publishing result to {kafka_output_topic}...")
                kafka_producer.send(kafka_output_topic, result.model_dump_json().encode())
