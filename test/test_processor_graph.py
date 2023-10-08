import unittest
import math
import time

from pydantic import BaseModel

from bowline import Processor, ProcessorGraph


class AddInputModel(BaseModel):
    x: int
    y: int


class AddOutputModel(BaseModel):
    result: int


class SquareOutputModel(BaseModel):
    result: int


class SquareRootOutputModel(BaseModel):
    result: float


def add_two_numbers(input: AddInputModel) -> AddOutputModel:
    result = input.x + input.y
    return AddOutputModel(result=result)


def square_number(input: AddOutputModel) -> SquareOutputModel:
    result = input.result * input.result
    return SquareOutputModel(result=result)


def square_root(input: AddOutputModel) -> SquareRootOutputModel:
    result = math.sqrt(input.result)
    return SquareRootOutputModel(result=result)


class TestProcessorChain(unittest.TestCase):
    def test_processor_graph(self):
        try:
            # Create processors
            add_two_numbers_processor = Processor(target_function=add_two_numbers,
                                                  name="addition",
                                                  input_model=AddInputModel,
                                                  output_model=AddOutputModel)
            square_number_processor = Processor(target_function=square_number,
                                                name="square",
                                                input_model=AddOutputModel,
                                                output_model=SquareOutputModel)
            square_root_processor = Processor(target_function=square_root,
                                              name="sqrt",
                                              input_model=AddOutputModel,
                                              output_model=SquareRootOutputModel)
            # Create ProcessorGraph
            # This processor graph will run add_two_numbers(), then run square_number() and square_root() on its result.
            processor_graph = ProcessorGraph()
            processor_graph.add_processor(add_two_numbers_processor)
            processor_graph.add_processor(square_number_processor, add_two_numbers_processor)
            processor_graph.add_processor(square_root_processor, add_two_numbers_processor)
            # Start the ProcessorGraph
            processor_graph.start()
            # Push input to graph
            input = AddInputModel(x=2, y=2)
            processor_graph.push_input(input)
            # Get results
            for _ in range(2):
                # Wait for results to be available
                while not processor_graph.has_output():
                    pass
                result = processor_graph.get_output()
                if result.processor == square_number_processor.get_name():
                    assert result.output == square_number(add_two_numbers(input))
                else:  # sqrt
                    assert result.output == square_root(add_two_numbers(input))
            # Push another input value
            input = AddInputModel(x=3, y=4)
            processor_graph.push_input(input)
            # Get results
            for _ in range(2):
                # Wait for results to be available
                while not processor_graph.has_output():
                    pass
                result = processor_graph.get_output()
                if result.processor == square_number_processor.get_name():
                    assert result.output == square_number(add_two_numbers(input))
                else:  # sqrt
                    assert result.output == square_root(add_two_numbers(input))
            # Push another input with larger values
            input = AddInputModel(x=123, y=456)
            processor_graph.push_input(input)
            # Get results
            for _ in range(2):
                # Wait for results to be available
                while not processor_graph.has_output():
                    pass
                result = processor_graph.get_output()
                if result.processor == square_number_processor.get_name():
                    assert result.output == square_number(add_two_numbers(input))
                else:  # sqrt
                    assert result.output == square_root(add_two_numbers(input))
        finally:
            # Shut down processors
            processor_graph.shutdown()

    def test_processor_graph_balanced_output(self):
        try:
            # Create processors
            add_two_numbers_processor = Processor(target_function=add_two_numbers,
                                                  name="addition",
                                                  input_model=AddInputModel,
                                                  output_model=AddOutputModel)
            square_number_processor = Processor(target_function=square_number,
                                                name="square",
                                                input_model=AddOutputModel,
                                                output_model=SquareOutputModel)
            square_root_processor = Processor(target_function=square_root,
                                              name="sqrt",
                                              input_model=AddOutputModel,
                                              output_model=SquareRootOutputModel)
            # Create ProcessorGraph
            # This processor graph will run add_two_numbers(), then run square_number() and square_root() on its result.
            processor_graph = ProcessorGraph()
            processor_graph.add_processor(add_two_numbers_processor)
            processor_graph.add_processor(square_number_processor, add_two_numbers_processor)
            processor_graph.add_processor(square_root_processor, add_two_numbers_processor)
            # Start the ProcessorGraph
            processor_graph.start()
            # Push input to graph
            input = AddInputModel(x=2, y=2)
            processor_graph.push_input(input)
            input = AddInputModel(x=3, y=4)
            processor_graph.push_input(input)
            input = AddInputModel(x=123, y=456)
            processor_graph.push_input(input)
            # Sleep to make sure all results are processed
            time.sleep(1)
            # Get results
            for i in range(6):
                print(i)
                result = processor_graph.get_output()
                # Even results should be the square processor
                if i % 2 == 0:
                    assert result.processor == square_number_processor.get_name()
                # Odd results should be the sqrt processor
                else:
                    assert result.processor == square_root_processor.get_name()
        finally:
            # Shut down processors
            processor_graph.shutdown()
