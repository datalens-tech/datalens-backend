import marshmallow as ma


class BaseRequestSchema(ma.Schema):
    class Meta:
        unknown = ma.EXCLUDE
