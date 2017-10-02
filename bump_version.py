import sys

if __name__ == '__main__':
    new_version = sys.argv[1].strip()
    with open('diagnostic/version.py', 'wb') as f:
        f.write("__version__ = '{0}'".format(new_version))
