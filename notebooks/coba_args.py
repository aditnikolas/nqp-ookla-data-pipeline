import argparse

parser = argparse.ArgumentParser(description="Latihan argparse")
parser.add_argument("--n-quarters", type=int, default=4)
parser.add_argument("--type", default="mobile")
parser.add_argument("--dry-run", action="store_true")

args = parser.parse_args()

print(args)
print("n_quarters =", args.n_quarters, type(args.n_quarters))
print("type       =", args.type)
print("dry_run    =", args.dry_run)