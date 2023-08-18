application:
    file.managed:
        - makedirs: True
        - user: root
        - group: root
        - names:
            - /etc/kubelet.d/app.yaml:
                - source: salt://application/pod.yaml
                - template: jinja
                - mode: '0644'
            {% if 'TVM_APP_ID' in pillar %}
            - /etc/tvm/tvm.json:
                - source: salt://application/tvm/tvm.json
                - template: jinja
            {% endif %}
ip6tables.module:
    file.append:
        - name: /etc/modules
        - text: ip6_tables
