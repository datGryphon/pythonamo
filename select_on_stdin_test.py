import sys
import select
from time import sleep

print('++>',end='')
sys.stdout.flush()

readable = select.select([sys.stdin.fileno()],[],[],10)[0]

sleep(10)

if readable:
	text = input()
	print('Echo: %s'%(text,))
else:
	print('Nothing to read?')

