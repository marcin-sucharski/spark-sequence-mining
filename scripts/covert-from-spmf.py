#!/bin/python

def convert(in_file, out_file):
    for i, line in enumerate(in_file):
        line = ' '.join(filter(lambda x: x != '-2\n', line.split(' ')))
        line_with_index = f"{i + 1} -1 {line}\n"
        out_file.write(line_with_index)

def main(argv):
    in_path, out_path = argv[1], argv[2]
    print(f"Reading from {in_path} and writing to {out_path}")
    with open(in_path, 'r') as in_file, open(out_path, 'w') as out_file:
        convert(in_file, out_file)

if __name__ == "__main__":
    from sys import argv
    main(argv)
