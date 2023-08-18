# Типовое использование

Открыть на машинке с bi-api ipython и запустить следующее:
```python
import asyncio
from bi_api_lib.maintenance.crawler_runner import run_crawler
from bi_core.maintenance.crawlers.crypto_keys_rotation import RotateCryptoKeyInConnection

crawler = RotateCryptoKeyInConnection(
    dry_run=True,  # Do not really save
    target_tenant=None,  # Crawl all tenants
)
# Will configure logging to send to it to Logbroker
asyncio.run(run_crawler(crawler, use_sr_factory=True))
```

`run_crawler` выдернет из окружения настройки для US manager'а и сконфигурит логирование.
Можно запустить только в IPython, делается проверка на это дело.
Если нужно запустить повторно, то лучше делать это с configure_logging=False

Краулер запустить можно только один раз. Если нужно запустить повторно - нужно после
запуска скопировать со сбросом USM:
```python
new_crawler = crawler.copy()
```
