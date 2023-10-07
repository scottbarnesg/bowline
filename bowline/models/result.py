from pydantic import BaseModel


class Output(BaseModel):
    processor: str
    output: BaseModel
