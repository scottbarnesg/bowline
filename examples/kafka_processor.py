import time

from kafka import KafkaConsumer
from pydantic import BaseModel

from bowline import KafkaProcessor


class AddInputModel(BaseModel):
    x: int
    y: int


class AddOutputModel(BaseModel):
    result: int


def add_two_numbers(input: AddInputModel) -> AddOutputModel:
    result = input.x + input.y
    return AddOutputModel(result=result)


if __name__ == '__main__':
    try:
        # Create and start processor
        addition_processor = KafkaProcessor(target_function=add_two_numbers,
                                            name="add",
                                            input_model=AddInputModel,
                                            output_model=AddOutputModel,
                                            kafka_boostrap_server="localhost:9094")
        addition_processor.set_input_topic("bowline-add-test")
        addition_processor.start()
        # Create a kafka consumer
        kafka_consumer = KafkaConsumer(addition_processor.get_output_topic(), bootstrap_servers="localhost:9094")

        # Push data to the
        addition_processor.push_input(AddInputModel(x=2, y=2))
        addition_processor.push_input(AddInputModel(x=3, y=4))
        addition_processor.push_input(AddInputModel(x=123, y=456))
        # Get the results
        print("Results: ")
        result_counter = 0
        for message in kafka_consumer:
            print(message)
            result_counter += 1
            if result_counter == 3:
                break
    finally:
        # Stop the processor
        addition_processor.shutdown()
