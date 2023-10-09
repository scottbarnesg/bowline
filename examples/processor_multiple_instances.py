import random
import time

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


try:
    addition_processor = Processor(target_function=add_two_numbers,
                                   name="add",
                                   input_model=AddInputModel,
                                   output_model=AddOutputModel,
                                   instances=2)
    # Start the processor
    addition_processor.start()
    # Push lots of data to the processor
    for _ in range(10):
        input = AddInputModel(x=random.randint(0, 1000), y=random.randint(0, 1000))
        addition_processor.push_input(input)
    # Wait for processing to finish
    time.sleep(1)
    # Get and validate stats
    stats = addition_processor.get_stats()
    print(stats)
    assert stats[0][Stats.inputs_processed.value].value > 0
    assert stats[1][Stats.inputs_processed.value].value > 0
finally:
    # Shut down the processor
    addition_processor.shutdown()