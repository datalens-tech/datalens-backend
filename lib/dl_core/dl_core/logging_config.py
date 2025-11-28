import dl_logging


def hook_configure_logging(app, *args, **kwargs):  # type: ignore  # TODO: fix
    """
    Try to configure logging in uwsgi `postfork` if possible,
    but ensure it is configured in `before_first_request` (flask app).
    """
    try:
        import uwsgidecorators  # type: ignore  # TODO: fix  # noqa
    except Exception:
        pass
    else:

        @uwsgidecorators.postfork
        def _init_logging_in_uwsgi_postfork():  # type: ignore  # TODO: fix
            dl_logging.configure_logging(*args, **kwargs)

    @app.before_first_request
    def _init_logging_in_before_first_request():  # type: ignore  # TODO: fix
        dl_logging.configure_logging(*args, **kwargs)
