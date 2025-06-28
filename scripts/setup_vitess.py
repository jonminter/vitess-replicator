import subprocess
import json
import tempfile
import sys


TABLE_DDL = """
CREATE TABLE users (
  id BIGINT PRIMARY KEY,
  username VARCHAR(255),
  email VARCHAR(255)
);
"""

subprocess.check_call(
    ["./vtctldclient", "ApplySchema", f"--sql={TABLE_DDL}", "commerce"],
)



VSCHEMA = {
    "sharded": True,
      "vindexes": {
        "xxhash": {
          "type": "xxhash"
        }
      },
  "tables": {
    "users": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "xxhash"
        }
      ]
    }
  }
}

subprocess.check_call(
    ["./vtctldclient", "ApplyVSchema", "commerce", f"--vschema={json.dumps(VSCHEMA)}"]
)
