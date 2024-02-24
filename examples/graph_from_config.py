from bowline.utils.config import ProcessorConfig
from simple_graph import AddInputModel

if __name__ == '__main__':
    # Create process chain from config file
    config_file_path = "examples/graph-config.yml"
    config = ProcessorConfig(config_file_path)
    processor_graph = config.generate_processors()
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
