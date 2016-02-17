import sys
import json

from person_parser import get_person_info

if __name__ == "__main__":
	with open(sys.argv[1]) as fh:
		for line in fh:
			info = get_person_info(line)
			print info