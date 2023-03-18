from pydantic import BaseModel, Field, Extra
from datetime import datetime


class DataAssetRecord(BaseModel):
    id: str = Field(
        ...,
        alias="_id",
        title="Data Asset ID",
        description="The unique id of the data asset.",
    )
    name: str = Field(
        ...,
        alias="_name",
        description="Name of the data asset.",
        title="Data Asset Name",
    )
    created: datetime = Field(
        ...,
        alias="_created",
        title="Created",
        description="The data and time the data asset created.",
    )
    location: str = Field(
        ...,
        alias="_location",
        title="Location",
        description="Current location of the data asset.",
    )

    class Config:
        extra = Extra.allow

    @property
    def _id(self):
        return self.id

    @property
    def _name(self):
        return self.name

    @property
    def _location(self):
        return self.location

    @property
    def _created(self):
        return self.created
