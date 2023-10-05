import time

from pydantic import BaseModel
from bowline.models.processor import Processor
from bowline.models.chain import ProcessorChain


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


if __name__ == '__main__':
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
    processor_chain.push_input(AddInputModel(x=2, y=2))
    processor_chain.push_input(AddInputModel(x=3, y=4))
    processor_chain.push_input(AddInputModel(x=123, y=456))
    # Push some data to the chain. This will add the numbers, then square them.
    while not processor_chain.has_output():
        time.sleep(1)
    # Get the results
    print(f"Results: ")
    for _ in range(3):
        print(processor_chain.get_output())
    # Shut down the processor chain
    processor_chain.shutdown()