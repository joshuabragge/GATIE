import argparse

from gaite import environment
from gaite import manager


def main():
    parser = argparse.ArgumentParser(description='Read google analytics and send results to recruiters')
    parser.add_argument('-e', '--env', choices=['test', 'prod'], help='test or prod', default='test')
    parser.add_argument('-v', '--verbosity', help="increase verbosity for troubleshooting", action='store_true')
    args = parser.parse_args()

    env = environment.Env()
    env._shared_state['env'] = args.env
    if args.verbosity:
        env._shared_state['verbosity'] = True

    runner = manager.ProcessManager()

    runner.run()


if __name__ == '__main__':
    main()
