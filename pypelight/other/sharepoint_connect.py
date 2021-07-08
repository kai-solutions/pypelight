from subprocess import call
from getpass import getpass
import sys

if __name__ == '__main__':

    default_username = 'default'
    # Prompt for establishing connection with sp
    user = input('User[default:{default_username}]:') or default_username
    username = f'/user:{user}'
    pwd = getpass('Password:', stream=sys.stderr)
    print('Creating connection...')
    # Command parameters and string conversion
    command = ['net', 'use', 'm:',
               '\\\\INTRANET.CORP.COMPANY.COM@SSL@0000\DAVWWWROOT', username, pwd]
    command_str = ' '.join(command)
    # Command execution for cmd
    try:
        call(command_str, timeout=3, shell=True)
    except Exception as e:
        print(e)
