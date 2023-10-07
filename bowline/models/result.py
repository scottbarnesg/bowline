from pydantic import BaseModel


class Result(BaseModel):
    processor: str
    value: BaseModel
