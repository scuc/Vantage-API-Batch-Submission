import os

if os.name == 'nt':
    os.system('pip install requests')
    os.system('pause')
else:
    os.system('pip3 install requests')