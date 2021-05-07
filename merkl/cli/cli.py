import os
import sys
import argparse
from pathlib import Path

from merkl.cli.init import InitAPI
from merkl.cli.run import RunAPI
from merkl.cli.dot import DotAPI


class MerkLAPI:
    init = InitAPI()
    run = RunAPI()
    dot = DotAPI()


def main():
    cwd = Path.cwd()
    desc = 'MerkL CLI'

    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Verbose output')
    parser.add_argument(
        '-q', '--quiet', action='store_true', help='No printing to stdout')

    parser.set_defaults(command='help', subcommand='')
    subparsers = parser.add_subparsers()

    # ------------- HELP --------------
    help_parser = subparsers.add_parser('help', description='Display help')
    help_parser.set_defaults(command='help', subcommand='')

    version_parser = subparsers.add_parser('version', description='Display version')
    version_parser.set_defaults(command='version', subcommand='')

    # ------------- INIT --------------
    init_parser = subparsers.add_parser(
        'init', description='Initialize a .merkl cache for this project')
    init_parser.set_defaults(command='init', subcommand='init')

    # ------------- RUN --------------
    run_parser = subparsers.add_parser(
        'run', description='Run a task or pipeline function')
    run_parser.set_defaults(command='run', subcommand='run')
    run_parser.add_argument('--no-cache', action='store_true', help='Disable caching')
    run_parser.add_argument('module_function', help='Module function to run (<module>.<function>)')

    # ------------- DAG --------------
    dot_parser = subparsers.add_parser(
        'dot', description='Output the DAG of a task or pipeline as a dot file')
    dot_parser.set_defaults(command='dot', subcommand='dot')
    dot_parser.add_argument('--rankdir', choices=['TB', 'LR'], help='Value for the rankdir dot graph parameter')
    dot_parser.add_argument('--transparent_bg', action='store_true', help='Option to output transparent background')
    dot_parser.add_argument('--no-cache', action='store_true', help='Disable caching')
    dot_parser.add_argument('module_function', help='Module function to use (<module>.<function>)')

    args, unknown_args = parser.parse_known_args()
    kwargs = dict(args._get_kwargs())

    # Pop commands that should not go to the route
    kwargs.pop('command')
    kwargs.pop('subcommand')
    quiet = kwargs.pop('quiet')
    verbose = kwargs.pop('verbose')

    if args.command == 'help':
        print('Use the --help option for help')
        exit(0)
    elif args.command == 'version':
        #print(merkl.__version__)
        exit(0)

    api = MerkLAPI()
    api_route = getattr(api, args.command)
    api_route.unknown_args = unknown_args
    route = getattr(api_route, args.subcommand)
    route(**kwargs)
