import time
import unittest

from pydantic import BaseModel

from bowline.models.processor import Processor


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
                                           name="Add & Print",
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
            # Let the processor finish processing the inputs
            while not addition_processor.has_output():
                time.sleep(1)
            # Verify the result are correct
            first_output = addition_processor.get_output()
            assert first_output == add_two_numbers(first_input_model)
            second_output = addition_processor.get_output()
            assert second_output == add_two_numbers(second_input_model)
            third_output = addition_processor.get_output()
            assert third_output == add_two_numbers(third_input_model)
        finally:
            # Shut down the processor
            addition_processor.shutdown()
