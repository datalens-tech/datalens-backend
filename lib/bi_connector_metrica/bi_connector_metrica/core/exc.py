from bi_core.exc import DatabaseQueryError


class MetricaAPIDatabaseQueryError(DatabaseQueryError):
    err_code = DatabaseQueryError.err_code + ['METRICA']
    default_message = 'Metrica API error.'
