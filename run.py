import argparse

from gaite import environment
from gaite import manager


def main():
    parser = argparse.ArgumentParser(description='Read google analytics and send results to recruiters')
    parser.add_argument('-e', '--env', choices=['test', 'prod'], help='test or prod', default='prod')
    parser.add_argument('-v', '--verbosity', help="increase verbosity for troubleshooting", action='store_true')
    parser.add_argument('-r', '--recruiter', help="specify a single recruiter to act upon")
    args = parser.parse_args()

    env = environment.Env()
    env.shared_state['env'] = args.env
    if args.verbosity:
        env.shared_state['verbosity'] = True
    if args.recruiter is not None:
        env.shared_state['recruiter'] = args.recruiter

    runner = manager.ProcessManager(env)

    runner.run()


if __name__ == '__main__':
    main()
