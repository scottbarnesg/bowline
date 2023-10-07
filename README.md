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

`python -m pip install bowline-streaming`

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


class AddOutputModel(BaseModel):
    result: int


def add_and_print(input: AddInputModel) -> AddOutputModel:
    result = input.x + input.y
    return AddOutputModel(result=result)


if __name__ == '__main__':
    # Create and start processor
    addition_processor = Processor(target_function=add_and_print,
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
```

### Processor Chains

Processor Chains connect multiple Processors in a chain, such that the output of a Processor is passed as the input to the next.
This allows for the definition of pipelines that can run in sequence on streaming data.

#### Example

The following example shows creating and using a `ProcessorChain` that adds two numbers, then squares the result.

```python
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
                                          name="add",
                                          input_model=AddInputModel,
                                          output_model=AddOutputModel)
    square_number_processor = Processor(target_function=square_number,
                                        name="square",
                                        input_model=AddOutputModel,
                                        output_model=SquareOutputModel)
    # Create process chain
    processor_chain = ProcessorChain()
    processor_chain.add_processor(add_two_numbers_processor)
    processor_chain.add_processor(square_number_processor)
    # Start the processor chain
    processor_chain.start()
    # Push some data to the chain. This will add the numbers, then square them.
    processor_chain.push_input(AddInputModel(x=2, y=2))
    processor_chain.push_input(AddInputModel(x=3, y=4))
    processor_chain.push_input(AddInputModel(x=123, y=456))
    # Get the results
    print(f"Results: ")
    for _ in range(3):
        # Wait for output
        while not processor_chain.has_output():
            pass
        print(processor_chain.get_output())
    # Shut down the processor chain
    processor_chain.shutdown()
```

### Processor Graphs

Processor Chains connect multiple Processors in a graph, such that the output of a Processor can be passed to multiple subsequent Processors.
This allows for the definition of pipelines that can run with an arbitrary number of branching processors within them.

#### Example

The following example creates a `ProcessorGraph` in which two numbers are added together, then the result of that
calculation is passed to two subsequent processors: one which squares that result, and the other of which computes its
square root.

```python
import math

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

_Why doesn't Bowline use asyncio?_

Bowline is built on the `multiprocessing` library, so each `Processor` instance runs in its own process.
This means that async functionality is generally not needed (although you are welcome to implement it in the functions you want Bowline to run).

Bowline's primary use case is creating pipelines for streaming data that you want to perform computationally-heavy operations on. 
If you are looking for an async-first library that runs tasks in a single process and is designed for lighter workloads, a tool like Faust may be a better 
fit for you use case. 

_Why is it called Bowline?_

The Bowline knot makes a reasonably secure loop in the end of a piece of rope. 
Two bowlines can be linked together to join two ropes.
 