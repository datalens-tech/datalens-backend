config_infra_files:
  file.managed:
    - user: root
    - group: root
    - mode: '0644'
    - makedirs: True
    - template: jinja
    - names:
      - /etc/api/configs/juggler-client/MANIFEST.json:
        - source: salt://infra/configs/MANIFEST.json
      - /etc/api/configs/juggler-client/juggler-client.conf:
        - source: salt://infra/configs/juggler-client.conf
      - /etc/api/configs/juggler-client/platform-http-check.json:
        - source: salt://infra/configs/platform-http-check.json
      - /etc/fluent/config.d/containers.input.conf:
        - source: salt://infra/configs/containers.input.conf
      - /etc/fluent/config.d/monitoring.conf:
        - source: salt://infra/configs/monitoring.conf
      - /etc/fluent/config.d/output.conf:
        - source: salt://infra/configs/output.conf
      - /etc/fluent/config.d/system.input.conf:
        - source: salt://infra/configs/system.input.conf
      - /etc/fluent/fluent.conf:
        - source: salt://infra/configs/fluent.conf
      - /etc/metricsagent/metricsagent.yaml:
        - source: salt://infra/configs/metricsagent.yaml
      - /etc/jaeger-agent/jaeger-agent-config.yaml:
        - source: salt://infra/configs/jaeger-agent.yaml
      {% if 'TVM_APP_ID' in pillar or 'PUSH_CLIENT_TVM_ID' in pillar %}
      - /etc/yandex/statbox-push-client/push-client.yaml:
        - source: salt://infra/configs/push-client.yaml
      {% endif %}
