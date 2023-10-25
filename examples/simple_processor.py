from pydantic import BaseModel
from bowline import Processor


class AddInputModel(BaseModel):
    x: int
    y: int


class AddOutputModel(BaseModel):
    result: int


def add_two_numbers(input: AddInputModel) -> AddOutputModel:
    result = input.x + input.y
    return AddOutputModel(result=result)


if __name__ == '__main__':
    # Create and start processor
    addition_processor = Processor(target_function=add_two_numbers,
                                   name="add",
                                   input_model=AddInputModel,
                                   output_model=AddOutputModel)
    addition_processor.start()
    # Push data to the
    addition_processor.push_input(AddInputModel(x=2, y=2))
    addition_processor.push_input(AddInputModel(x=3, y=4))
    addition_processor.push_input(AddInputModel(x=123, y=456))
    # Get the results
    print("Results: ")
    for _ in range(3): # We pushed 3 inputs, we expect 3 outputs
        # Wait for results to be ready
        while not addition_processor.has_output():
            pass
        result = addition_processor.get_output()
        print(result)
    # Stop the processor
    addition_processor.shutdown()