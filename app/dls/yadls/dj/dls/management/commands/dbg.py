""" ... """

from __future__ import annotations

from django.core.management.base import BaseCommand


class Command(BaseCommand):

    def handle(self, *args, **options):
        # from pprint import pprint
        from pprint import pformat
        import yadls.dj.dls.models as models

        # results = {}

        def note_model(model_name, **kwargs):
            print(" ======= {} =======".format(model_name))

        def add_res(key, value):
            print(" ==== {}: \n{}".format(key, pformat(value)))

        for model_name in models.__all__:
            model = getattr(models, model_name)
            note_model(model_name, model=model)
            qs = model._default_manager.get_queryset()
            add_res('{}_cnt'.format(model_name),  qs.count())
            if getattr(model, 'id', None) is not None:
                add_res('{}_last'.format(model_name), qs.order_by('-id')[0].__dict__)

        # pprint(results)
