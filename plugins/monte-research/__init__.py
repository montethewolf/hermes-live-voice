"""Standalone Hermes plugin. Install ONLY in the separate research profile."""
import importlib.util
import json
import os
from pathlib import Path

_spec = importlib.util.spec_from_file_location('monte_repositories', Path(__file__).with_name('repositories.py'))
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
Repositories = _module.Repositories
ALLOWLIST = frozenset({'repo_list', 'repo_read', 'repo_search'})

def enforce(tool_name='', **_):
    # No discovery, delegation, execution, writes, messaging, or arbitrary integrations.
    # Never raise here: Hermes treats unexpected hook errors as observer failures.
    if not isinstance(tool_name, str) or tool_name not in ALLOWLIST:
        return {'action': 'block', 'message': 'Research profile permits only repo_list, repo_read, and repo_search.'}
    return None

def handler(name, args, **_):
    try:
        return json.dumps(Repositories(os.environ['HERMES_LIVE_REPOSITORY_REGISTRY']).dispatch(name, args))
    except Exception as error:
        return json.dumps({'error': str(error)[:500]})

def register(ctx):
    ctx.register_hook('pre_tool_call', enforce)
    properties = {'project': {'type': 'string', 'description': 'Registered project id; list projects to resolve it.'},
                  'path': {'type': 'string'}, 'query': {'type': 'string'},
                  'start': {'type': 'integer', 'minimum': 1}, 'lines': {'type': 'integer', 'minimum': 1, 'maximum': 200}}
    for name in sorted(ALLOWLIST):
        fields = {'repo_list': ['project'], 'repo_read': ['project', 'path', 'start', 'lines'],
                  'repo_search': ['project', 'query']}[name]
        schema = {'name': name, 'description': 'Bounded read-only repository ' + name.removeprefix('repo_'),
                  'parameters': {'type': 'object', 'additionalProperties': False,
                                 'properties': {k: properties[k] for k in fields},
                                 'required': [] if name == 'repo_list' else fields[:2]}}
        ctx.register_tool(name=name, toolset='monte_research', schema=schema,
                          handler=lambda args, _name=name, **kw: handler(_name, args, **kw))
