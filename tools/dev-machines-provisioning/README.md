## Quick start
Создайте в QYP (https://qyp.yandex-team.ru/create-vm) машинку со следующими параметрами:
 * OS Image: bionic или focal
 * Сеть: `_DL_BACKEND_DEV_NETS_`
 * Internet access: NAT64
 * RAM 32/SSD 100Gb both or more

### Дальнейшие действия выполняются на локальной машине (ноуте)
Для установки нужной версии ansible выполните `pip install -r requirements.txt`. В virtualenv он живет нормально.

Сделайте в папке `inventories` файлик по образцу `inventories/sample.yml`.
 Если не нужен OpenVPN через SSH-туннель, поставьте `dm_setup_ovpn` в `no`.
 Остальные настройки с префиксом `ovpn` можно не указывать.
 Если хотите, чтобы докер-демон слушал на всех IPv6 адресах машинки, поставьте `dm_docker_expose_socket` в `yes`.
  Убедитесь, что к этому порту на dev-машинке нет доступа ни у кого, кроме вас!

Запустите следующую команду:
```shell script
ansible-playbook -i inventories/%{MY_INVENTORY_FILE} site.yml
```

Импортируйте в Tunnelblick (Drag'n'Drop vpn-конфиг в Tunnelblick -> VPN Details... -> Configurations). vpn-конфиг тут:
```
dev-machines/{dev_machine_fqdn}/ovpn/{dev_machine_fqdn}.ovpn
```

### Дальнейшие действия выполняются на удаленной машинке
После настройки надо будет настроить NAT для ipv6 в `docker0`:
```shell script
sudo ip6tables -t nat -A POSTROUTING -s ${DOCKER_0_IPV6_NET} -j MASQUERADE
```
${DOCKER_0_IPV6_NET} - см значение в roles/docker_host/defaults/main.yml переменная `docker_doker0_addr`


### При использовании Skotty

Для ssh-авторизации через skotty необходимо в `~/.ssh/config` добавить:

```text
Host {DEV_MACHINE_IPV4}
  ForwardAgent /home/{USER}/.skotty/sock/default.sock
```

### При запуске openvpn-клиент на Ubuntu

Вероятнее всего встроенный NetworkManager не потянет установить туннель, потому стоит запускать его через openvpn-cli:

```shell
openvpn {PATH_TO_OPENVPN_CONFIG}.ovpn
```
