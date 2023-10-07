import time
import unittest

from pydantic import BaseModel

from bowline import ProcessorChain
from bowline import Processor

"""
Reusable classes for testing
"""


class AddInputModel(BaseModel):
    x: int
    y: int


class AddOutputModel(BaseModel):
    result: int


class SquareOutputModel(BaseModel):
    result: int


def add_two_numbers(input: AddInputModel) -> AddOutputModel:
    result = input.x + input.y
    return AddOutputModel(result=result)


def square_number(input: AddOutputModel) -> SquareOutputModel:
    result = input.result * input.result
    return SquareOutputModel(result=result)


class TestProcessorChain(unittest.TestCase):
    def test_processor_chain(self):
        try:
            # Create processors
            add_two_numbers_processor = Processor(target_function=add_two_numbers,
                                                  name="Add two numbers",
                                                  input_model=AddInputModel,
                                                  output_model=AddOutputModel)
            square_number_processor = Processor(target_function=square_number,
                                                name="Square number",
                                                input_model=AddOutputModel,
                                                output_model=SquareOutputModel)
            # Create process chain
            processor_chain = ProcessorChain()
            processor_chain.add_processor(add_two_numbers_processor)
            processor_chain.add_processor(square_number_processor)
            # Start the processor chain
            processor_chain.start()
            # Push some data to the chain
            first_input_model = AddInputModel(x=2, y=2)
            processor_chain.push_input(first_input_model)
            second_input_model = AddInputModel(x=3, y=4)
            processor_chain.push_input(second_input_model)
            third_input_model = AddInputModel(x=123, y=456)
            processor_chain.push_input(third_input_model)
            # Wait for output
            while not processor_chain.has_output():
                pass
            # Verify the result are correct
            first_output = processor_chain.get_output()
            assert first_output.output == square_number(add_two_numbers(first_input_model))
            while not processor_chain.has_output():
                pass
            second_output = processor_chain.get_output()
            assert second_output.output == square_number(add_two_numbers(second_input_model))
            while not processor_chain.has_output():
                pass
            third_output = processor_chain.get_output()
            assert third_output.output == square_number(add_two_numbers(third_input_model))
        finally:
            # Shut down the processor chain
            processor_chain.shutdown()
