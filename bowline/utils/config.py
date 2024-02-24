from enum import Enum
from importlib import import_module
from typing import Dict, Union

import yaml

from bowline import Processor, ProcessorChain, ProcessorGraph


class ConfigTypes(Enum):
    chain = "chain"
    graph = "graph"


class ConfigKeys(Enum):
    processors = "processors"
    target_function = "target_function"
    input_model = "input_model"
    output_model = "output_model"


class ProcessorConfig:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self._config_data = self._load_configuration_file(self.file_path)

    def print_vars(self):
        print(locals())
        print(globals())

    def generate_processors(self) -> Union[ProcessorChain, ProcessorGraph]:
        processor_container_type = self._get_processor_container_type()
        if processor_container_type == ConfigTypes.chain:
            processor_chain = ProcessorChain()
            # Walk the chain and generate a processor for each
            processor_data = self._config_data[ConfigTypes.chain.value][ConfigKeys.processors.value]
            for processor_entry in processor_data:
                processor_name = list(processor_entry.keys())[0]
                target_function = processor_entry[processor_name][ConfigKeys.target_function.value]
                input_model = processor_entry[processor_name][ConfigKeys.input_model.value]
                output_model = processor_entry[processor_name][ConfigKeys.output_model.value]
                processor = self.generate_processor(processor_name, target_function, input_model, output_model)
                processor_chain.add_processor(processor)
            return processor_chain
        elif processor_container_type == ConfigTypes.graph:
            processor_graph = ProcessorGraph()
            # Each ProcessorGraph should start with one "entrypoint" Processor
            processor_data = self._config_data[ConfigTypes.graph.value][ConfigKeys.processors.value][0]
            # TODO: Recursively parse the config data to generate processors and add them to the
            self._parse_graph_processors(processor_data, processor_graph)
            return processor_graph

    def _parse_graph_processors(self, processor_data: Dict[str, any], processor_graph: ProcessorGraph,
                                processor_parent: Processor = None):
        # Use processor_data to generate a Processor and add it to the ProcessorGraph
        processor_name = list(processor_data.keys())[0]
        target_function = processor_data[processor_name][ConfigKeys.target_function.value]
        input_model = processor_data[processor_name][ConfigKeys.input_model.value]
        output_model = processor_data[processor_name][ConfigKeys.output_model.value]
        processor = self.generate_processor(processor_name, target_function, input_model, output_model)
        processor_graph.add_processor(processor, processor_parent)
        # If this processor has children, recursively parse them
        if ConfigKeys.processors.value in processor_data[processor_name].keys():
            for child_processor_data in processor_data['addition']['processors']:
                self._parse_graph_processors(child_processor_data, processor_graph, processor)

    def generate_processor(self, processor_name: str, target_function_import_path: str, input_model_import_path: str,
                           output_model_import_path: str) -> Processor:
        # Dynamically import the dependencies
        # Target function
        target_function_module_name, target_function_name = target_function_import_path.rsplit('.', 1)
        target_function_module = import_module(target_function_module_name)
        target_function = getattr(target_function_module, target_function_name)
        # Input model
        input_model_module_name, input_model_name = input_model_import_path.rsplit('.', 1)
        input_model_module = import_module(input_model_module_name)
        input_model = getattr(input_model_module, input_model_name)
        # Output model
        output_model_module_name, output_model_name = output_model_import_path.rsplit('.', 1)
        output_model_module = import_module(output_model_module_name)
        output_model = getattr(output_model_module, output_model_name)
        return Processor(
            name=processor_name,
            target_function=target_function,
            input_model=input_model,
            output_model=output_model
        )

    def _get_processor_container_type(self) -> ConfigTypes:
        processor_container_type = list(self._config_data.keys())[0]
        if processor_container_type == ConfigTypes.chain.value:
            return ConfigTypes.chain
        elif processor_container_type == ConfigTypes.graph.value:
            return ConfigTypes.graph
        else:
            raise ValueError(f"Invalid processor container type specified. "
                             f"Expected {ConfigTypes.chain.value} or {ConfigTypes.graph.value}")

    @staticmethod
    def _load_configuration_file(file_path: str) -> Dict[str, any]:
        with open(file_path) as f:
            config_data = yaml.safe_load(f)
        return config_data
