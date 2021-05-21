import random
import string
from tqdm import tqdm
from datasketch import MinHash, MinHashLSH

import config


def main() -> None:
    for _ in tqdm(range(1), desc="Create finding example:"):
        minhash = MinHash(num_perm=256)
        list_strings = []
        for _ in range(200):
            rand_string = ''.join(random.choice(string.ascii_lowercase) for i in range(5))
            list_strings.append(rand_string)
        minhash.update_batch([s.encode('utf-8') for s in list_strings])

    for _ in tqdm(range(1), desc="Connect to existing db:"):
        lsh = MinHashLSH(
            threshold=0.5, num_perm=256, storage_config={
                'type': 'cassandra',
                'basename': b'perftest',
                'cassandra': {
                    'seeds': ['127.0.0.1'],
                    'keyspace': config.KEY_SPACE,
                    'replication': {
                        'class': 'SimpleStrategy',
                        'replication_factor': '1',
                    },
                    'drop_keyspace': False,
                    'drop_tables': False,
                }
            }
        )

    try:
        for _ in tqdm(range(1), desc="Find minHash similarity:"):
            result = lsh.query(minhash)
        print("Approximate neighbours with Jaccard similarity > 0.5", result)
    except BaseException as e:
        print(str(e))
        print("Error")


if __name__ == "__main__":
    main()
