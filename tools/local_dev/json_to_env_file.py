import json
import sys

input_files = sys.argv[1].split(",")
output_file = sys.argv[2]
replace_map: dict[str, str] = json.loads(sys.argv[3])

print(f"Going to merge {input_files} into {output_file}")

secrets: dict[str, str] = {}

for filename in input_files:
    with open(filename, "r") as file:
        secrets.update(json.load(file))
        for k, v in replace_map.items():
            if k in secrets:
                print(f"Remapping {k} to {v}")
                secrets[v] = secrets.pop(k)

with open(output_file, "w") as file:
    for k, v in secrets.items():
        file.write(f"{k}={json.dumps(v)}\n")
