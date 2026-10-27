#!/usr/bin/env python3
"""Service-owned local tooling; no sibling checkout or deployment imports."""
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[1]
SERVICE = 'catalog-comparison'
ENV_FILE = os.environ.get('ENV_FILE', '.env')
COMPOSE = ['docker', 'compose', '--env-file', ENV_FILE, '-f', 'docker-compose.yml']


def execute(command, **kwargs):
    subprocess.run(command, check=True, **kwargs)


def resolve():
    result = subprocess.run(COMPOSE + ['--profile', '*', 'config', '--format', 'json'], capture_output=True, text=True)
    if result.returncode:
        sys.exit('Cannot resolve local configuration; check .env, required credentials, owner and dependency tags')
    model = json.loads(result.stdout)
    for name, service in model['services'].items():
        image = service.get('image', '')
        if image.startswith('ghcr.io/'):
            tag = image.rsplit('/', 1)[-1]
            if ':' not in tag or tag.rsplit(':', 1)[1] in ('', 'latest'):
                sys.exit(name + ': select a nonempty published version/commit tag')
    return model


def host_url(value, model):
    url = urlsplit(value)
    service = model['services'].get(url.hostname)
    if service is None:
        return value
    ports = [port for port in service.get('ports', []) if int(port['target']) == (url.port or 80)]
    if not ports:
        sys.exit('Required container connection has no configured host port')
    credentials = url.netloc.rsplit('@', 1)[0] + '@' if '@' in url.netloc else ''
    return urlunsplit((url.scheme, credentials + '127.0.0.1:' + str(ports[0]['published']), url.path, url.query, url.fragment))


def main():
    os.chdir(ROOT)
    action = sys.argv[1]
    model = resolve()
    if action == 'config':
        for name, service in model['services'].items():
            print(name + ': ' + service.get('image', 'build from current checkout'))
        return
    if action == 'down':
        execute(COMPOSE + ['--profile', '*', 'down'])
        return
    if action in ('deps.up', 'app.up'):
        flags = ['--profile', 'app'] if action == 'app.up' else []
        profiles = os.environ.get('WORKFLOW_PROFILES', '').split()
        for profile in profiles:
            flags += ['--profile', profile]
        command = COMPOSE + flags
        print('Dependency image availability will be checked; no image set has validated compatibility evidence.', flush=True)
        execute(command + ['pull', '--ignore-buildable'])
        execute(command + ['up', '-d', '--pull', 'never'] + (['--build'] if action == 'app.up' else []))
        return
    environment = os.environ.copy()
    environment.update({key: str(value) for key, value in model['services']['app']['environment'].items() if value is not None})
    if action == 'run':
        selected = subprocess.run(COMPOSE + ['--profile', '*', 'config', '--environment'], capture_output=True, text=True, check=True)
        values = dict(line.split('=', 1) for line in selected.stdout.splitlines() if '=' in line)
        for key in ('DATABASE_URL', 'RABBITMQ_URL', 'OIDC_JWKS_URL'):
            if key in environment and key not in values:
                environment[key] = host_url(environment[key], model)
        ports = model['services']['app'].get('ports', [])
        if ports and 'HTTP_ADDR' not in values:
            environment['HTTP_ADDR'] = '127.0.0.1:' + str(ports[0]['published'])
        if SERVICE == 'frontend':
            command = ['npm', 'run', 'dev', '--', '--host', '127.0.0.1', '--port', str(ports[0]['published']), '--strictPort']
        elif SERVICE == 'renderer-parser':
            command = ['npm', 'start']
        else:
            command = ['go', 'run', './cmd/' + SERVICE]
        os.execvpe(command[0], command, environment)
    sys.exit('Unknown local action: ' + action)


if __name__ == '__main__':
    try:
        main()
    except subprocess.CalledProcessError as error:
        sys.exit('Local command failed (exit ' + str(error.returncode) + '); no cached fallback or volume reset')
    except FileNotFoundError as error:
        sys.exit('Required local tool missing: ' + str(error.filename))
