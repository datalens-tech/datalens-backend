#!/bin/bash -x

# set -Eeuo pipefail
set -Eeu

export DEBIAN_FRONTEND=noninteractive
# update-locale LANG=C.UTF-8
# export LC_ALL="C.UTF-8"
# export LANG="C.UTF-8"


printf ' ======= apt keys =======\n' >&2

cat > /etc/apt/trusted.gpg.d/yandex.asc <<\_EOF
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v1

mQGiBEtUdt4RBACjI2l5GgpfeZdgM6tz76IUulWTz9mpraW47x+5CJKMAnYZiHim
3UjgK5RzNGTf0p1zE0CcSpKEt46n5tmCyzRrLp/XIUksRgoD0x85WXqTZ1FcfzGP
DHduTUTg/qtmRZmQBZ7Qx9b3PQHXtfTaG14Etjg58zhG4s6upg0IvJp5FwCggxA4
7gNGsoNfJmRgEuITw7Lz4bsEAJo1actFQxllmCr27cj46m9qzw0m2+343ttoiy9n
tnc6dObAQjjcuNTAekaFyf5vkVfHu2u3weeRbO29TO/jzAg5WQjS/A/co3fEQvJW
Vx+PLs8ayTVuOna9zSf2nhmkvHSPJ8m2zOBef+tLVnY7Ilh5VFD8UB9l/l+fU+ZC
6P8iBACYCYX86yH/fJB7dtqHI59XotEL0DbhVxq5v3suDhAa15lDGjSINf4NoR6N
+0k2td35dVTPiVj4EA8kPuOHbd8KfOzEB+TrvMonNRexrxJJWwXbDWDCR0OXsjYk
ZoYsg5eK5Wg/3ZPGmHkamQdlUMllyFhoaZAsEurxv/fpAKinR7Q9WWFuZGV4IFB1
YmxpYyBSZXBvc2l0b3J5IFNpZ24gS2V5IDxvcGVuc291cmNlQHlhbmRleC10ZWFt
LnJ1PohgBBMRAgAgBQJLVHbeAhsDBgsJCAcDAgQVAggDBBYCAwECHgECF4AACgkQ
f80RGGBQzRr+LQCePI23TIYmaI+EKl2Xaj9+re8rZ78AnAuizGbQsxK+ekkbAaf1
6ayZDXgquQQMBEtUdzQQEACPupsR/uPOiSodPtMPEZclpMR2nHJCoK68Rm+NsYYy
ZXEdh6xA/c9YvbK7zV4GvKaV6sqc6HFVVvS2kfMtr4qrq+QUjTho61jbjPGNkW1L
nXwcDVu16C4lD3AHpjw1DI0hDmuPBudax9a5kdu9o55F9ryTB5MUjlWV6lc910UL
fk8wA9TXMd21EDUep347xbPeXEYxT1VajOJSg7AnIDbHC2piHXqlyEBS3VN1446m
uon1o4gXcLMyqkBoZzNHn7oHz4zRvwv9GdP31lrkwvb9UEmLx344WBLWV5n05Wyi
w73Ovq0fgrk4GLYGC0TPwk6VGdHMs4z8gOEHSs49N3xvE8eFQgTQjyrRtHYUvhY6
zj79bMmRncPQlQQkocUuWHNdV7pW3d6O23u2uOzbzfOKfm9qQeMYsbR7QGKZUdVB
NPsZGJ2UOo4LG1t4dJyEQ3ne8GJV56E1F5se2xB5MfNVk7OUoGyboesH83deyeV5
C4QFARBN4oEpwk5eObe2iJ/sXc3vH/GzWQC8RsVaTiY7owhRq7xP4f617J5LS8f0
dPPOnM9EDSnEz4H1aeN2qGqKWmLnXQakXrzJPetwBeFeUXp1BdnDPdCdQxZ3hfCz
CHf0TTsM+O1F3R7ReQBQC5FDGNB8Gmz6R9Nb1+5pfGtzxQUVAt7uGVrZqEf16A5e
+wADBQ/41byo8Gkff3u4YO+fLblyKU9hNJDmc1kvOCdyEYyrKVV7hqzqr+wvKG/2
2cJ0UwcDGwd/YvCwD3l2f2R8Q//1RMM2/GQ22Fn5l8llqNwdBnPL6VajWWFBkatW
iox6gq264lneYRUBDX6QGRbW+7K4VTycf9G+o2oY8h2FHP+FZp/loAd8kp100DQM
EJay4Vh5274DJs4nCyXjIHr0OYtiBk2ftkSBobisd0CsE/dsaxpL72S+od6wxtUa
fimDAJxxufVGYPbpHOEVEH7VUxTkr+zhAQBRAaTrYDJdCzKB2UwsacizxW3Bf40Y
1VJv/efrnxkv03wn7mTHkdDtnqlXrDvVGmpJNhWK0XMJig80QLw/ON7JMntcvaRJ
ZyhTUAt6+fFRxRP8Ty9eHnMUJ9bGAyKwoLMdcnAi+0h8wop0iPxm+Mh82sYjPzik
t6xNJmxT9+ROONH3xRsWjAJT2xm6buugzGrfRIcfxNtu067Z/EWNuQl3ikUiGiUa
nijX2yKTJtv4l2sOB2aG/KVQCbIB3dPkr/zExSTBE1BxCfmh5kWrtDn/4JthnAAi
CcfAI2wj0J+qOriiacs1Jqyf8P1a75xJBjvoLVOqsJBcWyBI8+i6NfILCBZNnnhD
Pj5XfC/OkFt0XXJQxAzKEUiG641gVefNz357qJfuWMV/WsWdaYhJBBgRAgAJBQJL
VHc0AhsMAAoJEH/NERhgUM0aENcAn2ZISZNc+5D3uferkgdG8Ek1h4N9AJsGG+TZ
zNicZHfu680ocsUdJKoQqA==
=m3IH
-----END PGP PUBLIC KEY BLOCK-----
_EOF
cat > /etc/apt/sources.list.d/yandex.list <<\_EOF
deb http://common.dist.yandex.ru/common stable/all/
deb http://common.dist.yandex.ru/common stable/$(ARCH)/
_EOF


cat > /etc/apt/trusted.gpg.d/docker.asc <<\_EOF
-----BEGIN PGP PUBLIC KEY BLOCK-----

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
YT90qFF93M3v01BbxP+EIY2/9tiIPbrd
=0YYh
-----END PGP PUBLIC KEY BLOCK-----
_EOF
cat > /etc/apt/sources.list.d/docker.list <<\_EOF
deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable
# deb-src [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable
_EOF


if [ ! -f '/usr/lib/apt/methods/https' ]; then
    printf ' ======= apt-transport-https =======\n' >&2
    apt-get -y update
    apt-get -y install apt-transport-https
fi


printf ' ======= docker config and iptables =======\n' >&2

mkdir -p /etc/iptables
cat > /etc/iptables/rules.v6 <<\_EOF
*filter
:INPUT ACCEPT [176:22921]
:FORWARD ACCEPT [0:0]
:OUTPUT ACCEPT [29:5356]
COMMIT
*nat
:PREROUTING ACCEPT [101:8396]
:INPUT ACCEPT [10:720]
:OUTPUT ACCEPT [80:6923]
:POSTROUTING ACCEPT [80:6923]
-A POSTROUTING -s fd00::/8 -j MASQUERADE
COMMIT
_EOF

mkdir -p /etc/docker
cat > /etc/docker/daemon.json <<\_EOF
{
    "ipv6": true,
    "fixed-cidr-v6": "fd00:dead:beef::/48",
    "ip-forward": true
}
_EOF
sudo ip6tables -t nat -D POSTROUTING -s fd00::/8 -j MASQUERADE || true
sudo ip6tables -t nat -A POSTROUTING -s fd00::/8 -j MASQUERADE


printf ' ======= apt packages =======\n' >&2

apt-get -y update
# # apt pre-fix:
# apt-get install yandex-internal-root-ca  # /usr/share/yandex-internal-root-ca/YandexInternalRootCA.crt
# apt-get --yes purge yandex-search-common-apt || true

dpkg --configure -a
apt-get -y install \
        yandex-internal-root-ca \
        yandex-passport-vault-client \
        yandex-arc-launcher \
        curl software-properties-common \
        git subversion \
        iptables-persistent \
        python3.7 python3-pip python3-virtualenv python3.7-venv \
        gettext \
        xjobs zstd


printf ' ======= apt docker (+ hax-retry) =======\n' >&2

# HAX: install it in a separate step as an attempt to fix intermittent wait-for-startup-in-postinst errors.
apt-get -y install docker-ce || true
sleep 15
dpkg --configure -a
apt-get install --reinstall docker-ce


# printf ' ======= docker wait-for-up =======\n' >&2
# # Useuful if the docker needs to be reconfigured *after* the installation.
# sudo service docker restart
# # see also:
# # https://github.com/ClickHouse/ClickHouse/blob/master/docker/test/integration/runner/dockerd-entrypoint.sh
# max_attempts=50
# sleep_time=0.1
# for attempt in $(seq 1 "$max_attempts"); do
#     if ! docker info; then
#         if [ "$attempt" -eq "$max_attempts" ]; then
#             printf "Timed out while waiting for docker to come up (%s x %s seconds).\n" "$max_attempts" "$sleep_time" >&2
#             exit 1
#         fi
#         printf "Waiting for docker to come up (@%s x %ss)...\n" "$attempt" "$sleep_time" >&2
#         sleep "$sleep_time"
#         continue
#     fi
#     break
# done
# docker info  # just in case, again


printf ' ======= docker-load helper =======\n' >&2

cat > /usr/bin/docker-load.sh <<\_EOF
#!/bin/bash
set -e
for img in $@; do
    fname=$(basename -- "$img")
    sfx="${img##*.}"
    name="${fname%.*}"
    catcmd='cat'
    case "$sfx" in
    zst|zstd)
        catcmd='zstdcat'
        ;;
    xz)
        catcmd='xzcat'
        ;;
    tgz|gz)
        catcmd='zcat'
        ;;
    bz2|bz)
        catcmd='bzcat'
        ;;
    tar)
        catcmd='cat'
        ;;
    esac
    $catcmd $img | docker load
done
_EOF
chmod +x /usr/bin/docker-load.sh


printf ' ======= ya =======\n' >&2

if which ya >/dev/null; then
    ya --version
else
    curl --fail --silent --show-error --output /usr/local/bin/ya "https://s3.mds.yandex.net/ya-bucket/ya"
    ya --version
fi

printf ' ======= python packages =======\n' >&2

PIP3_INST="python3 -m pip install --index-url https://pypi.yandex-team.ru/simple/"
$PIP3_INST --upgrade pip setuptools
$PIP3_INST --upgrade pytest pytest-html pytest-xdist 'releaser-cli[all]'


printf ' ======= "bitst" user =======\n' >&2

if ! id bitst; then
    adduser --disabled-password --gecos "" bitst
    adduser bitst docker
fi


printf ' ======= configuration tuning =======\n' >&2

test -f /etc/ssl/certs/ca-certificates.crt
cat >> /etc/environment <<\_EOF
REQUESTS_CA_BUNDLE="/etc/ssl/certs/ca-certificates.crt"
_EOF

sed -i '/^user_allow_other/ d' /etc/fuse.conf
sed -i '$ s/$/\nuser_allow_other/' /etc/fuse.conf


printf ' ======= state debug =======\n' >&2

dpkg -l
pip3 list


printf ' ======= done =======\n' >&2
echo "."
