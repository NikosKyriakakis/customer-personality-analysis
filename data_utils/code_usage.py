from itertools import count
import os
import argparse


def count_lines(filename):
    with open(filename, 'r') as F:
        lines = 0
        comment_section = False
        for line in F:
            if "/*" in line:
                comment_section = True
            if "*/" in line:
                comment_section = False
                continue
            
            if not comment_section:
                line = line.strip()
                if line:
                    if not (
                        line.startswith("//") \
                        or line.startswith("--")
                    ):
                        lines += 1
            
        return lines


def code_size(path_to_dir, file_type):
    if not os.path.exists(path_to_dir):
        raise OSError
        
    os.chdir(path_to_dir)
    dir_contents = os.listdir(path_to_dir)
    file_type = file_type.lower()
    if file_type != "java" and file_type != "pig":
        print("[!!] Unsupported code extension provided --> Supported types are <java> and <pig> files")
        return 0

    extension_size = len(file_type)
    total_lines = 0
    for file_i in dir_contents:
        if file_i[-extension_size:] == file_type:
            total_lines += count_lines(file_i)
    return total_lines


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate code size in a specified folder.")
    parser.add_argument("--src", dest="src", type=str, required=True)
    parser.add_argument("--code_ext", dest="ext", type=str, required=True)
    args = parser.parse_args()
    total_lines = code_size(args.src, args.ext)
    print("Total code size: {}".format(total_lines))