# Bowline

Easily build performant data stream processing pipelines in Python.

Bowline is a Python library that simplifies creating data pipelines that perform sequential computations
on data streams.

Key features of Bowline include:
- Performance: Each Bowline `Processor` runs in its own process, meaning that Bowline is ideal for high 
throughput and computationally heavy workloads.
- Simplicity: Bowline abstracts away the complexity of `multiprocessing` by handling process creation, 
inter-process communication, and clean process shutdown.
- Typing: Data inputs and ouputs are validated with `pydantic`.


## Installation

python -m pip install bowline-streaming

## Usage

The following section describe the `Processor` and `ProcessorChain` classes.

### Processors

Processors are the "building blocks" of the process chains. 
A Processor defines the function to be executed, data input and output formats, and a processor name.
Processors are executed in a background process, and data is transferred via process-safe queues.

#### Example

The following example shows creating and using a `Processor` that adds two numbers and prints the results.

```python
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
```

### Processor Chains

Processor Chains connect multiple Processors in a chain, such that the output of a Processor is passed as the input to the next.
This allows for the definition of pipelines that can run in sequence on streaming data.

#### Example

The following example shows creating and using a `ProcessorChain` that adds two numbers, then squares the result.

```python
import time

from pydantic import BaseModel
from bowline import Processor
from bowline import ProcessorChain


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
    # Create process chain. Processors are automatically connected in the order they are added.
    processor_chain = ProcessorChain()
    processor_chain.add_processor(add_two_numbers_processor)
    processor_chain.add_processor(square_number_processor)
    # Start the processor chain
    processor_chain.start()
    # Push some data to the chain. This will add the numbers, then square them.
    processor_chain.push_input(AddInputModel(x=2, y=2))
    processor_chain.push_input(AddInputModel(x=3, y=4))
    processor_chain.push_input(AddInputModel(x=123, y=456))
    # Let the processor finish processing the inputs
    while not processor_chain.has_output():
        time.sleep(1)
    # Get the results
    print(f"Results: ")
    for _ in range(3):
        print(processor_chain.get_output())
    # Shut down the processor chain
    processor_chain.shutdown()
```

## Considerations

Because Bowline uses `multiprocessing` behind the scenes, all data models must be serializable. 

## Local Development

1. Clone the repository: `git clone git@github.com:scottbarnesg/bowline.git`
2. Install `bowline` as an interactive package: `python -m pip install -e .`

### Running the tests

1. Install the test dependencies: `python -m pip install -e .[dev]`
2. Run the tests: `python -m pytest test/`

## FAQ

_Why is it called Bowline?_

The Bowline knot makes a reasonably secure loop in the end of a piece of rope. 
Two bowlines can be linked together to join two ropes.
 