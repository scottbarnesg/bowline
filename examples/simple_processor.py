from pydantic import BaseModel
from bowline import Processor


class AddInputModel(BaseModel):
    x: int
    y: int


def add_and_print(input: AddInputModel) -> None:
    result = input.x + input.y
    print(f"The sum of {input.x} and {input.y} equals {result}.")


if __name__ == '__main__':
    # Create and start processor
    addition_processor = Processor(target_function=add_and_print,
                                   name="Add & Print",
                                   input_model=AddInputModel)
    addition_processor.start()
    # Push data to the
    addition_processor.push_input(AddInputModel(x=2, y=2))
    addition_processor.push_input(AddInputModel(x=3, y=4))
    addition_processor.push_input(AddInputModel(x=123, y=456))
    # Stop the processor
    addition_processor.shutdown()