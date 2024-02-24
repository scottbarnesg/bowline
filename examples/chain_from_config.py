from bowline.utils.config import ProcessorConfig
from simple_chain import AddInputModel

if __name__ == '__main__':
    # Create process chain from config file
    config_file_path = "examples/chain-config.yml"
    config = ProcessorConfig(config_file_path)  # TODO: Refactor this so that you can just pass the config to a ProcessorChain()
    processor_chain = config.generate_processors()
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