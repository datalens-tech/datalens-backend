kubelet.readOnlyRort:
  file.append:
    - name: /var/lib/kubelet/config.yaml
    - text: 'readOnlyPort: 10255'
