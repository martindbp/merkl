import os
import sys
import logging
import argparse
import cProfile, pstats, io
from pstats import SortKey
from pathlib import Path

import merkl
from merkl.cli.init import InitAPI
from merkl.cli.run import RunAPI
from merkl.cli.dot import DotAPI
from merkl.cli.clear import ClearAPI
from merkl.logger import logger


class MerkLAPI:
    init = InitAPI()
    run = RunAPI()
    dot = DotAPI()
    clear = ClearAPI()


def main():
    cwd = Path.cwd()
    desc = 'MerkL CLI'

    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Verbose output')
    parser.add_argument(
        '-l', '--long', action='store_true', help='Print long hashes for verbose output')
    parser.add_argument(
        '-p', '--profile', action='store_true', help='Profile the command')

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
    run_parser.add_argument('-n', '--no-cache', action='store_true', help='Disable caching')
    run_parser.add_argument('-c', '--clear', action='store_true', help='Clear cache of any unrelated items before running')
    run_parser.add_argument('module_function', help='Module function to run (<module>.<function>)')

    # ------------- DOT --------------
    dot_parser = subparsers.add_parser(
        'dot', description='Output the DAG of a task or pipeline as a dot file')
    dot_parser.set_defaults(command='dot', subcommand='dot')
    dot_parser.add_argument('--rankdir', choices=['TB', 'LR'], help='Value for the rankdir dot graph parameter')
    dot_parser.add_argument('--transparent-bg', action='store_true', help='Option to output transparent background')
    dot_parser.add_argument('-n', '--no-cache', action='store_true', help='Disable caching')
    dot_parser.add_argument('module_function', help='Module function to use (<module>.<function>)')

    # ------------- CLEAR --------------
    clear_parser = subparsers.add_parser(
        'clear', description='Clears the cache optionially with respect to a pipeline or task')
    clear_parser.set_defaults(command='clear', subcommand='clear')
    clear_parser.add_argument('-k', '--keep', action='store_true', help='Keep reachable all values from <module_function>')
    clear_parser.add_argument('--keep-outs', action='store_true', help='Keep only final <module_function> outputs')
    clear_parser.add_argument('module_function', help='Module function to use (<module>.<function>)')


    args, unknown_args = parser.parse_known_args()
    kwargs = dict(args._get_kwargs())

    # Pop commands that should not go to the route
    kwargs.pop('command')
    kwargs.pop('subcommand')
    verbose = kwargs.pop('verbose')
    long_hashes = kwargs.pop('long')
    profile = kwargs.pop('profile')
    if verbose:
        logger.setLevel(logging.DEBUG)
        merkl.logger.LONG = long_hashes

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

    if profile:
        with cProfile.Profile() as pr:
            route(**kwargs)

        s = io.StringIO()
        sortby = SortKey.CUMULATIVE
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())
    else:
        route(**kwargs)
