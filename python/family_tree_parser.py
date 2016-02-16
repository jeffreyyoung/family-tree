import sys
import json

from person_parser import get_person_info

if __name__ == "__main__":
	with open(sys.argv[1]) as fh:
		for line in fh:
			person_id = line[:23]
			data = json.loads(line[24:])
			get_person_info(person_id, data)