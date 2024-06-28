import marshmallow as ma


class BaseRequestSchema(ma.Schema):
    class Meta:
        unknown = ma.EXCLUDE

    conn_type = ma.fields.String(required=True)
