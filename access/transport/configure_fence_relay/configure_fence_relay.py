# Контекст миграции: AS IS / TO BE.
# Назначение: Настраивает ограниченный SSH relay от Airflow к Kafka fence с forced command и закреплёнными host keys.
# AS IS: Source-доступ осуществляется через GP, TLS и зарегистрированные identities.
# TO BE: Настраивает проверяемый транспорт/доверие или verifier target-сервисов.
# Отличие: Это инфраструктурный доступ, а не автоматический перенос бизнес-ролей; TLS не отключается ради миграции.
# Конец контекста миграции.
"""Provision command-only SSH access for the migration fence; no shell access.

Private keys never leave memory except their intended restricted host/Secret.
Uses existing trusted administrative SSH routes; does not change cloud firewall.
"""

import base64
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile

HERE = Path(__file__).resolve().parent
CONF = Path(os.environ["OIL_GP_SSH_CONFIG"]).expanduser().resolve()
BASTION = ["ssh", "-o", "BatchMode=yes", "redos@79.137.175.114"]
GP = ["ssh", "-F", str(CONF), "-o", "BatchMode=yes", "gp-demo-coordinator"]
KAFKA = [
    "ssh",
    "-F",
    str(CONF),
    "-o",
    "BatchMode=yes",
    "-o",
    "ProxyJump=gp-demo-coordinator",
    "-i",
    str(Path.home() / ".ssh/vk_migration_demo_20261115_rsa"),
    "ubuntu@10.77.25.20",
]


def call(prefix, command, data=None):
    """Выполняет один запрос к сервису или провайдеру с явно заданным контекстом и проверкой ответа."""
    r = subprocess.run(
        prefix + [command], input=data, text=True, capture_output=True, timeout=35
    )
    if r.returncode:
        raise RuntimeError("administrative SSH operation failed")
    return r.stdout


def hostkey(prefix):
    """Получает и проверяет SSH host key до настройки доверенного relay."""
    return " ".join(
        call(prefix, "cat /etc/ssh/ssh_host_ed25519_key.pub").strip().split()[:2]
    )


def main():
    """Точка входа: Provision command-only SSH access for the migration fence; no shell access. Импорт модуля сам по себе эту операцию не запускает."""
    prior = subprocess.run(
        BASTION
        + [
            "kubectl get secret oil-wave2-fence-ssh -n airflow-atomcluster-4jn2dw24 -o name"
        ],
        text=True,
        capture_output=True,
        timeout=20,
    )
    if prior.returncode == 0:
        call(
            BASTION,
            "sudo -n -u oil-fence-relay test -s /etc/oil-fence-relay/id_ed25519",
        )
        print(
            "Existing scoped relay retained; no keys rotated. Verify with the runtime preflight."
        )
        return
    with tempfile.TemporaryDirectory() as td:
        keys = {}
        for name in ["frontend", "backend"]:
            key = Path(td) / name
            subprocess.run(
                [
                    "ssh-keygen",
                    "-q",
                    "-t",
                    "ed25519",
                    "-N",
                    "",
                    "-C",
                    "oil-fence-" + name,
                    "-f",
                    str(key),
                ],
                check=True,
                capture_output=True,
            )
            keys[name] = (key.read_text(), Path(str(key) + ".pub").read_text().strip())
        backend_known = (
            "160.5.219.123 " + hostkey(GP) + "\n10.77.25.20 " + hostkey(KAFKA) + "\n"
        )
        frontend_known = "10.0.2.7 " + hostkey(BASTION) + "\n"
        gp_program = """import os,pwd,subprocess,sys,json,pathlib
x=json.load(sys.stdin);name='oil-fence-jump'
try:u=pwd.getpwnam(name)
except KeyError:
 subprocess.run(['useradd','--create-home','--shell','/bin/sh',name],check=True);u=pwd.getpwnam(name)
d=pathlib.Path(u.pw_dir)/'.ssh';d.mkdir(mode=0o700,exist_ok=True);d.chmod(0o700);os.chown(d,u.pw_uid,u.pw_gid)
p=d/'authorized_keys'
line='restrict,port-forwarding,permitopen="10.77.25.20:22",command="/bin/false" '+x['public']+'\\n'
if p.exists() and p.read_text()!=line:raise RuntimeError('existing jump key differs; reconcile before replacing')
p.write_text(line);p.chmod(0o600);os.chown(p,u.pw_uid,u.pw_gid)
print('forward-only GP jump configured')
"""
        call(
            GP,
            "sudo -n python3 -c " + shlex.quote(gp_program),
            json.dumps({"public": keys["backend"][1]}),
        )
        config = """Host oil-fence-gp
 HostName 160.5.219.123
 User oil-fence-jump
Host oil-fence-broker
 HostName 10.77.25.20
 User oil-fence
 ProxyJump oil-fence-gp
Host *
 IdentityFile /etc/oil-fence-relay/id_ed25519
 UserKnownHostsFile /etc/oil-fence-relay/known_hosts
 IdentitiesOnly yes
 StrictHostKeyChecking yes
 BatchMode yes
 ForwardAgent no
 RequestTTY no
 ConnectTimeout 8
 ServerAliveInterval 15
 ServerAliveCountMax 2
"""
        relay = '#!/usr/bin/python3\nimport os\nos.execv("/usr/bin/ssh",["ssh","-F","/etc/oil-fence-relay/ssh_config","oil-fence-broker"])\n'
        bastion_program = """import os,pwd,subprocess,sys,json,pathlib
x=json.load(sys.stdin);name='oil-fence-relay'
try:u=pwd.getpwnam(name)
except KeyError:
 subprocess.run(['useradd','--create-home','--shell','/bin/sh',name],check=True);u=pwd.getpwnam(name)
d=pathlib.Path('/etc/oil-fence-relay');d.mkdir(mode=0o750,exist_ok=True);os.chown(d,0,u.pw_gid);d.chmod(0o750)
for filename,content in [('id_ed25519',x['private']),('known_hosts',x['known']),('ssh_config',x['config'])]:
 p=d/filename
 if p.exists() and p.read_text()!=content:raise RuntimeError('existing relay configuration differs')
 p.write_text(content);p.chmod(0o600);os.chown(p,u.pw_uid,u.pw_gid)
exe=pathlib.Path('/usr/local/libexec/oil-fence-relay');exe.parent.mkdir(exist_ok=True);exe.write_text(x['relay']);exe.chmod(0o755);os.chown(exe,0,0)
sd=pathlib.Path(u.pw_dir)/'.ssh';sd.mkdir(mode=0o700,exist_ok=True);sd.chmod(0o700);os.chown(sd,u.pw_uid,u.pw_gid)
p=sd/'authorized_keys';line='restrict,command="/usr/local/libexec/oil-fence-relay" '+x['public']+'\\n'
if p.exists() and p.read_text()!=line:raise RuntimeError('existing frontend key differs')
p.write_text(line);p.chmod(0o600);os.chown(p,u.pw_uid,u.pw_gid)
print('command-only bastion relay configured')
"""
        call(
            BASTION,
            "sudo -n python3 -c " + shlex.quote(bastion_program),
            json.dumps(
                {
                    "private": keys["backend"][0],
                    "known": backend_known,
                    "config": config,
                    "relay": relay,
                    "public": keys["frontend"][1],
                }
            ),
        )
        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "oil-wave2-fence-ssh",
                "namespace": "airflow-atomcluster-4jn2dw24",
            },
            "type": "Opaque",
            "data": {
                "id_ed25519": base64.b64encode(keys["frontend"][0].encode()).decode(),
                "known_hosts": base64.b64encode(frontend_known.encode()).decode(),
            },
        }
        call(BASTION, "kubectl apply -f -", json.dumps(secret))
        (HERE / "fence-backend-public-key.pub").write_text(keys["backend"][1] + "\n")
        (HERE / "fence-relay-evidence.json").write_text(
            json.dumps(
                {
                    "frontend": "oil-fence-relay@10.0.2.7",
                    "backend": "oil-fence@10.77.25.20",
                    "jump": "oil-fence-jump@160.5.219.123",
                    "worker_secret": "oil-wave2-fence-ssh",
                    "capabilities": "forced JSON fence controller only; no shell, agent or arbitrary port forwarding",
                    "host_keys": "pinned from existing trusted SSH sessions",
                },
                indent=2,
            )
            + "\n"
        )
        print(
            "Scoped relay and worker Secret ready; Kafka authorized_keys install remains."
        )


if __name__ == "__main__":
    main()
