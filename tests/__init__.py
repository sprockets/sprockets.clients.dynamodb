import os


def setup_module():
    try:
        with open('build/test-environment') as env_file:
            for line in env_file:
                if line.startswith('export '):
                    line = line[7:]
                name, sep, value = line.strip().partition('=')
                if value:
                    os.environ[name] = value
                else:
                    os.environ.pop(name, None)
    except IOError:
        pass
