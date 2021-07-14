import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Crawling@Home Worker Script'
    )

    parser.add_argument('--name', '-n', type=str, default="ARKseal", help='Your name')
    parser.add_argument('--url', '-u', type=str, default="http://cah.io.community/", help='The Crawling Server')
    parser.add_argument('--debug', '-d', action='store_true')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--cpu', '-c', action='store_true')
    group.add_argument('--gpu', '-g', action='store_true')

    args = parser.parse_args()

    if args.cpu:
        import cpu
        cpu.main(args.name, args.url, args.debug)
    elif args.gpu:
        import gpu
        gpu.main(args.name, args.url, args.debug)
    else:
        import hybrid
        hybrid.main(args.name, args.url, args.debug)
