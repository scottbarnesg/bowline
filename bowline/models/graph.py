from collections import OrderedDict
from multiprocessing import Queue
from typing import Dict, List, Optional

from pydantic import BaseModel

from bowline.models.processor import Processor
from bowline.models.result import Result
from bowline.utils.logger import get_logger


logger = get_logger(__name__)


class ProcessorGraph:
    def __init__(self):
        # _processor_graph maps a Processor to the Processors that follow it in the graph.
        self._processor_graph: Dict[Processor, List[Processor]] = OrderedDict()
        # Tracks which processor's output to handle next. This helps balance outputs between processors
        # This is incremented first, so setting this to -1 makes it so that the 0th terminal processor is called first
        self._current_terminal_processor_index = -1

    def add_processor(self, new_processor: Processor, previous_processor: Optional[Processor] = None):
        # If previous_processor is not supplied, this is the first processor.
        if not previous_processor:  # TODO: Logic got mixed up here.
            # If the graph isn't empty, need a previous_processor to link new_processor to
            if self._processor_graph:
                raise ValueError("ProcessorGraph already contains one or more Processors. "
                                 "You must provide a previous processor to link it to.")
        else:
            # Verify previous_processor is already in the _processor_graph
            if previous_processor not in self._processor_graph.keys():
                raise ValueError(f"{previous_processor} has not been added to the ProcessorGraph.")
            # Add new processor to the list associated with previous_processor
            if new_processor in self._processor_graph[previous_processor]:
                raise ValueError(f"{new_processor} is already linked to {previous_processor}.")
            self._processor_graph[previous_processor].append(new_processor)
        # Add new processor as a key to _processor_graph, if it doesn't exist already.
        if new_processor not in self._processor_graph.keys():
            self._processor_graph[new_processor] = []

    def push_input(self, input: BaseModel):
        if not self._processor_graph:
            raise ValueError("There are no processors in the graph. You must add processors before you can push data.")
        first_processor = self._get_first_processor()
        if not type(input) == first_processor.get_input_model():
            raise ValueError(
                f"Input is of type {type(input)}, but the {first_processor.get_name()} processor expects a {first_processor.get_input_model()}")
        first_processor.push_input(input)

    def has_output(self) -> bool:
        for terminal_processor in self._get_terminal_processors():
            if terminal_processor.has_output():
                return True
        return False

    def get_output(self) -> Optional[Result]:
        """
        This implementation balances outputs between the terminal processors
        """
        for _ in range(len(self._get_terminal_processors())):
            terminal_processor = self._get_next_terminal_processor()
            if terminal_processor.has_output():
                return terminal_processor.get_output()

    def _get_next_terminal_processor(self) -> Processor:
        # Update current output queue index
        self._current_terminal_processor_index += 1
        if self._current_terminal_processor_index >= len(self._get_terminal_processors()):
            self._current_terminal_processor_index = 0
        print(f"Terminal processor index is {self._current_terminal_processor_index}")
        # Return that output queue
        return self._get_terminal_processors()[self._current_terminal_processor_index]

    def start(self):
        if not self._processor_graph:
            raise ValueError("There are no processors in the graph. Nothing to start.")
        # Build processor graph
        self._build_processor_chain()
        # Start processors
        self._start_all_processors()

    def shutdown(self):
        for processor in self._processor_graph.keys():
            processor.shutdown()

    def _build_processor_chain(self):
        # Walk the processor graph.
        for source_processor, target_processors in self._processor_graph.items():
            for target_processor in target_processors:
                # Validate that the output type of the source matches the input type of the target
                if type(source_processor.get_output_model()) is not type(target_processor.get_input_model()):
                    raise ValueError(f"Target Processor {target_processor} expected {type(target_processor.get_input_model())}, but Source Processor {source_processor} provides {type(source_processor.get_output_model())}")
                # Create an output queue in the source and make it the input queue for the target
                new_queue = Queue()
                source_processor.add_output_queue(new_queue)
                target_processor.set_input_queue(new_queue)
                logger.info(f"Setting output queue of {source_processor.get_name()} as input queue of {target_processor.get_name()}")

    def _start_all_processors(self):
        for processor in self._processor_graph.keys():
            processor.start()

    def _get_first_processor(self) -> Processor:
        """
        Return the first processor in the ProcessorGraph
        """
        return next(iter(self._processor_graph))

    def _get_terminal_processors(self) -> List[Processor]:
        terminal_processors = []
        for source_processor, target_processors in self._processor_graph.items():
            if not target_processors:
                terminal_processors.append(source_processor)
        return terminal_processors





