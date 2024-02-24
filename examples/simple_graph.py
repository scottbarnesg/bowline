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
    print(f"{input.x} + {input.y} = {result}")
    return AddOutputModel(result=result)


def square_number(input: AddOutputModel) -> SquareOutputModel:
    result = input.result * input.result
    print(f"{input.result} squared is {result}")
    return SquareOutputModel(result=result)


def square_root(input: AddOutputModel) -> SquareRootOutputModel:
    result = math.sqrt(input.result)
    print(f"The square root of {input.result} is {result}")
    return SquareRootOutputModel(result=result)


if __name__ == '__main__':
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
    processor_graph.push_input(AddInputModel(x=2, y=2))
    processor_graph.push_input(AddInputModel(x=3, y=4))
    processor_graph.push_input(AddInputModel(x=123, y=456))
    # Get results
    for _ in range(6):  # We provided 3 inputs, and there are 2 terminal Processors, so 6 total results
        # Wait for results to be available
        while not processor_graph.has_output():
            pass
        result = processor_graph.get_output()
        print(f"Received output {result.output} from processor {result.processor}")
    # Shut down processors
    processor_graph.shutdown()
