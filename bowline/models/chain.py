from typing import Optional, List

from pydantic import BaseModel

from bowline.models.processor import Processor
from bowline.utils.logger import get_logger


logger = get_logger(__name__)


class ProcessorChain:
    def __init__(self, processors: Optional[List[Processor]] = None):
        self.processors = processors
        if not self.processors:
            self.processors = []

    def add_processor(self, processor: Processor):
        """
        Appends a Processor to the processor chain.
        """
        self.processors.append(processor)

    def push_input(self, input: BaseModel):
        if not self.processors:
            raise ValueError("There are no processors in the chain. You must add processors before you can push data.")
        if not type(input) == self.processors[0].get_input_model():
            raise ValueError(f"Input is of type {type(input)}, but the {self.processors[0].get_name()} processor expects a {self.processors[0].get_input_model()}")
        self.processors[0].push_input(input)

    def get_output(self) -> Optional[BaseModel]:
        return self.processors[-1].get_output()

    def has_output(self) -> bool:
        return self.processors[-1].has_output()

    def start(self):
        """
        Start all Processors in the ProcessChain. Each Processor runs in its own background process.
        """
        if not self.processors:
            raise ValueError("No processors have been added to the chain. Nothing to start.")
        self._build_processor_chain()
        for processor in self.processors:
            processor.start()

    def _build_processor_chain(self):
        if not self.processors:
            raise ValueError("No processors have been added to the chain. Nothing to build.")
        # Walk the processors and make the output queue of each the input of the next.
        for index, processor in enumerate(self.processors):
            if index == 0:
                continue
            # Make sure the previous processor's output and current processor's input match
            previous_processor = self.processors[index - 1]
            if type(previous_processor.get_output_model()) is not type(previous_processor.get_input_model()):
                raise ValueError(
                    f"The output type of Processor {previous_processor.get_name()} does not match the input type of Processor {processor.get_name()}, which are {previous_processor.get_output_model()} and {processor.get_input_model()}")
            # Get the ouput queue of the previous processor and make it the input of this processor
            logger.debug(
                f"Setting output queue of {previous_processor.get_name()} as input queue of {processor.get_name()}"
            )
            processor.set_input_queue(previous_processor.get_output_queue())

    def shutdown(self):
        for processor in self.processors:
            processor.shutdown()
