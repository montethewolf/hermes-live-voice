"""Bounded repository evidence, shared by the gateway and research tools. No shell."""
import fnmatch
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import subprocess
import sys

MAX_FILE = 128_000
MAX_FILES = 10_000
DENIED = {'.git', '.hg', '.svn', '.ssh', '.aws', '.azure', '.gnupg', '.config', '.npmrc', '.netrc',
          '.pypirc', 'auth.json', 'credentials', 'credentials.json', 'credentials.yaml', 'secrets.json',
          'secrets.yaml', 'secrets.yml', 'id_rsa', 'id_ed25519', 'known_hosts', 'authorized_keys'}

def safe_name(name):
    parts = PurePosixPath(name).parts
    return bool(parts) and not name.startswith('/') and '\\' not in name and not any(
        p in ('..', '.') or p.lower() in DENIED or p.lower().startswith('.env') or
        any(fnmatch.fnmatch(p.lower(), pattern) for pattern in ('*.pem', '*.key', '*.p12', '*.pfx', '*credentials*', '*.keystore'))
        for p in parts)

class Repositories:
    def __init__(self, registry):
        data = json.loads(Path(registry).read_text())
        if data.get('version') != 1:
            raise ValueError('Unsupported repository registry')
        self.projects = {p['id']: p for p in data['projects']}

    def root(self, project):
        value = self.projects.get(project)
        if not value:
            raise ValueError('Project is not registered')
        path = Path(value['path'])
        if not path.is_absolute() or path.resolve() != path or not (path / '.git').exists():
            raise ValueError('Registered repository is unavailable')
        return path

    def git(self, project, *args, input=None):
        result = subprocess.run(['git', '-c', 'core.fsmonitor=false', '-c', 'core.hooksPath=/dev/null',
                                 '-C', str(self.root(project)), *args], input=input, capture_output=True, timeout=8,
                                env={'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'HOME': '/nonexistent',
                                     'GIT_CONFIG_NOSYSTEM': '1', 'GIT_TERMINAL_PROMPT': '0', 'GIT_OPTIONAL_LOCKS': '0'})
        if result.returncode not in (0, 1):
            raise ValueError('Repository evidence unavailable')
        if len(result.stdout) > 4_000_000:
            raise ValueError('Repository listing exceeds limit')
        return result.stdout

    def files(self, project):
        raw = self.git(project, 'ls-files', '-z', '--cached', '--others', '--exclude-standard')
        names = sorted(set(n for n in raw.decode('utf-8', 'replace').split('\0') if safe_name(n)))
        ignored = self.git(project, 'check-ignore', '--no-index', '-z', '--stdin', input=('\0'.join(names) + '\0').encode()) if names else b''
        excluded = set(ignored.decode('utf-8', 'replace').split('\0'))
        result = []
        for name in names:
            if name in excluded:
                continue
            try:
                fd = self.open_file(project, name)
                os.close(fd)
                result.append(name)
            except (OSError, ValueError):
                continue
            if len(result) >= MAX_FILES:
                break
        return result

    def open_file(self, project, name):
        if not safe_name(name):
            raise ValueError('Forbidden repository path')
        # Walk from an open root directory. O_NOFOLLOW closes symlink and rename races.
        fd = os.open(self.root(project), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            parts = PurePosixPath(name).parts
            for part in parts[:-1]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            leaf = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
            info = os.fstat(leaf)
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_FILE or info.st_nlink != 1:
                os.close(leaf)
                raise ValueError('File type or size is not allowed')
            return leaf
        finally:
            os.close(fd)

    def read(self, project, path, start=1, lines=120, allowed=None):
        if path not in (self.files(project) if allowed is None else allowed):
            raise ValueError('File is excluded or outside the repository')
        if not isinstance(start, int) or not isinstance(lines, int) or start < 1 or not 1 <= lines <= 200:
            raise ValueError('Invalid line bounds')
        with os.fdopen(self.open_file(project, path), 'rb') as stream:
            raw = stream.read(MAX_FILE + 1)
        if len(raw) > MAX_FILE or b'\0' in raw:
            raise ValueError('File is binary or exceeds limit')
        text = raw.decode('utf-8', 'strict')
        if '-----BEGIN ' in text and 'PRIVATE KEY-----' in text:
            raise ValueError('Credential content excluded')
        selected = text.splitlines()[start-1:start-1+lines]
        return '\n'.join(f'{i+start}: {line}' for i, line in enumerate(selected))[:12000]

    def dispatch(self, tool, args):
        project = args.get('project', '')
        if tool == 'repo_list':
            if not project:
                return {'projects': [{'id': p['id'], 'name': p['name']} for p in self.projects.values()]}
            names = self.files(project)
            return {'files': names[:300], 'truncated': len(names) > 300}
        if tool == 'repo_read':
            return {'path': args.get('path'), 'content': self.read(project, args.get('path', ''), args.get('start', 1), args.get('lines', 120))}
        if tool == 'repo_search':
            query = args.get('query', '')
            if not isinstance(query, str) or not 1 <= len(query) <= 200:
                raise ValueError('Search requires 1–200 characters of literal text')
            names = self.files(project)
            hits = []
            size = 0
            for name in names[:500]:
                try:
                    with os.fdopen(self.open_file(project, name), 'rb') as stream:
                        raw = stream.read(MAX_FILE + 1)
                    if b'\0' in raw or b'PRIVATE KEY-----' in raw:
                        continue
                    for i, line in enumerate(raw.decode('utf-8').splitlines(), 1):
                        if query.casefold() in line.casefold():
                            hit = {'path': name, 'line': i, 'text': line[:300]}
                            hits.append(hit)
                            size += len(json.dumps(hit))
                            if len(hits) >= 60 or size > 12000:
                                return {'matches': hits, 'truncated': True}
                except (OSError, ValueError, UnicodeError):
                    continue
            return {'matches': hits, 'truncated': len(names) > 500}
        raise ValueError('Tool is not allowed by the research profile')

    def brief(self, project):
        names = self.files(project)
        revision = self.git(project, 'rev-parse', '--verify', '--quiet', 'HEAD').decode().strip()
        status = self.git(project, 'status', '--porcelain=v1', '--untracked-files=all').decode()
        digest = hashlib.sha256((revision + status).encode())
        for name in names:
            try:
                info = os.stat(self.root(project) / name, follow_symlinks=False)
                digest.update(f'{name}:{info.st_mtime_ns}:{info.st_size}:{info.st_ino}'.encode())
            except OSError:
                continue
        preferred = sorted(names, key=lambda n: (0 if Path(n).name.lower().startswith('readme') else
            1 if n in ('package.json', 'pyproject.toml') else 2 if 'architect' in n.lower() else 3, len(n)))
        snippets = []
        for name in preferred[:6]:
            try:
                snippets.append(f'FILE {name}\n{self.read(project, name, 1, 45, names)[:1800]}')
            except (OSError, ValueError, UnicodeError):
                continue
        briefing = f'Project: {self.projects[project]["name"]}\nRevision: {revision or "unborn"}\nWorking tree: {"dirty" if status else "clean"}\nFiles (bounded):\n' + '\n'.join(names[:100]) + '\n\n' + '\n\n'.join(snippets)
        return {'briefing': briefing[:12000], 'stamp': digest.hexdigest()}

if __name__ == '__main__':
    if len(sys.argv) != 4 or sys.argv[1] != 'brief':
        raise SystemExit('Usage: repositories.py brief REGISTRY PROJECT_ID')
    print(json.dumps(Repositories(sys.argv[2]).brief(sys.argv[3])))
