import random
import time
import unittest
from typing import Dict

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


def throws_exception(input: AddInputModel):
    raise ValueError("This is a ValueError")


def add_two_numbers_to_generated_number(input: AddInputModel, generated_number: int) -> AddOutputModel:
    result = input.x + input.y + generated_number
    return AddOutputModel(result=result)


def generate_number() -> Dict[str, any]:
    return {"generated_number": 10}


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
            assert stats[0][Stats.inputs_processed.value].value + stats[1][
                Stats.inputs_processed.value].value == num_inputs
        finally:
            # Shut down the processor
            addition_processor.shutdown()

    def test_target_function_throws_exception(self):
        try:
            addition_processor = Processor(target_function=throws_exception,
                                           name="throws-exception",
                                           input_model=AddInputModel)
            # Start the processor
            addition_processor.start()
            # Push some input to the processor
            addition_processor.push_input(AddInputModel(x=2, y=2))
            # Wait for processor to run
            time.sleep(1)
        finally:
            # Shut down the processor
            addition_processor.shutdown()

    def test_setup_function(self):
        try:
            addition_processor = Processor(target_function=add_two_numbers_to_generated_number,
                                           name="add",
                                           input_model=AddInputModel,
                                           output_model=AddOutputModel,
                                           setup_function=generate_number)
            # Start the processor
            addition_processor.start()
            # Push data to the processor
            first_input_model = AddInputModel(x=2, y=2)
            addition_processor.push_input(first_input_model)
            second_input_model = AddInputModel(x=3, y=4)
            addition_processor.push_input(second_input_model)
            third_input_model = AddInputModel(x=123, y=456)
            addition_processor.push_input(third_input_model)
            generated_int = generate_number()['generated_number']
            # Wait for results
            while not addition_processor.has_output():
                pass
            # Verify the result are correct
            first_output = addition_processor.get_output()
            assert first_output.output == add_two_numbers_to_generated_number(first_input_model, generated_int)
            while not addition_processor.has_output():
                pass
            second_output = addition_processor.get_output()
            assert second_output.output == add_two_numbers_to_generated_number(second_input_model, generated_int)
            while not addition_processor.has_output():
                pass
            third_output = addition_processor.get_output()
            assert third_output.output == add_two_numbers_to_generated_number(third_input_model, generated_int)
        finally:
            # Shut down the processor
            addition_processor.shutdown()

    def test_processor_delay(self):
        try:
            delay = 2
            addition_processor = Processor(target_function=add_two_numbers,
                                           name="add",
                                           input_model=AddInputModel,
                                           output_model=AddOutputModel,
                                           delay=delay)
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
            # Verify time to get next value is at least delay long
            start_time = time.monotonic()
            while not addition_processor.has_output():
                pass
            second_output = addition_processor.get_output()
            assert round(time.monotonic() - start_time) >= delay
            assert second_output.output == add_two_numbers(second_input_model)
            # Verify time to get next value is at least delay long
            start_time = time.monotonic()
            while not addition_processor.has_output():
                pass
            assert round(time.monotonic() - start_time) >= delay
            third_output = addition_processor.get_output()
            assert third_output.output == add_two_numbers(third_input_model)
        finally:
            # Shut down the processor
            addition_processor.shutdown()