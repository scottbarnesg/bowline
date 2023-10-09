import random
import time
import unittest

from pydantic import BaseModel

from bowline import Processor
from bowline.models.processor import Stats


class AddInputModel(BaseModel):
    x: int
    y: int


class AddOutputModel(BaseModel):
    result: int


def add_two_numbers(input: AddInputModel) -> AddOutputModel:
    result = input.x + input.y
    return AddOutputModel(result=result)


class TestProcessorChain(unittest.TestCase):
    def test_processor(self):
        try:
            addition_processor = Processor(target_function=add_two_numbers,
                                           name="add",
                                           input_model=AddInputModel,
                                           output_model=AddOutputModel)
            # Start the processor
            addition_processor.start()
            # Push data to the processor
            first_input_model = AddInputModel(x=2, y=2)
            addition_processor.push_input(first_input_model)
            second_input_model = AddInputModel(x=3, y=4)
            addition_processor.push_input(second_input_model)
            third_input_model = AddInputModel(x=123, y=456)
            addition_processor.push_input(third_input_model)
            # Wait for results
            while not addition_processor.has_output():
                pass
            # Verify the result are correct
            first_output = addition_processor.get_output()
            assert first_output.output == add_two_numbers(first_input_model)
            while not addition_processor.has_output():
                pass
            second_output = addition_processor.get_output()
            assert second_output.output == add_two_numbers(second_input_model)
            while not addition_processor.has_output():
                pass
            third_output = addition_processor.get_output()
            assert third_output.output == add_two_numbers(third_input_model)
        finally:
            # Shut down the processor
            addition_processor.shutdown()

    def test_processor_multiple_instances(self):
        try:
            addition_processor = Processor(target_function=add_two_numbers,
                                           name="add",
                                           input_model=AddInputModel,
                                           output_model=AddOutputModel,
                                           instances=2)
            # Start the processor
            addition_processor.start()
            # Push lots of data to the processor
            num_inputs = 100
            for _ in range(num_inputs):
                input = AddInputModel(x=random.randint(0, 1000), y=random.randint(0, 1000))
                addition_processor.push_input(input)
            # Wait for processing to finish
            time.sleep(1)
            # Get and validate stats
            stats = addition_processor.get_stats()
            print(stats)
            assert stats[0][Stats.inputs_processed.value].value > 0
            assert stats[1][Stats.inputs_processed.value].value > 0
            assert stats[0][Stats.inputs_processed.value].value + stats[1][Stats.inputs_processed.value].value == num_inputs
        finally:
            # Shut down the processor
            addition_processor.shutdown()
