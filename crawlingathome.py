import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Crawling@Home Worker Script'
    )

    parser.add_argument('--name', '-n', type=str,
                        default='ARKseal', help='Your name')
    parser.add_argument('--url', '-u', type=str,
                        default='http://cah.io.community/', help='The Crawling Server')
    parser.add_argument('--debug', '-d', action='store_true',
                        help='Add additional prints to debug code')
    parser.add_argument('--notebook', '-b', action='store_true',
                        help='Use tqdm.notebook module for notebooks')
    parser.add_argument('--docker', '-k', action='store_true',
                        help='Check docker version for latest image')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--hybrid', '-y', action='store_true',
                       help='Run the hybrid worker (default)')
    group.add_argument('--cpu', '-c', action='store_true',
                       help='Run the cpu worker')
    group.add_argument('--gpu', '-g', action='store_true',
                       help='Run the gpu worker')

    args = parser.parse_args()

    if args.cpu:
        import cpu
        cpu.main(args.name, args.url, args.debug, args.notebook, args.docker)
    elif args.gpu:
        import gpu
        gpu.main(args.name, args.url, args.debug, args.notebook, args.docker)
    else:
        import hybrid
        hybrid.main(args.name, args.url, args.debug,
                    args.notebook, args.docker)
