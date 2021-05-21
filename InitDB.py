import random
import string
import config
from tqdm import tqdm
from datasketch import MinHash, MinHashLSH


def main() -> None:
    minhashes = []
    files = []
    for iterator in tqdm(range(config.COUNT_UNQ_MHS), desc="Generate minHashes:"):
        minhash = MinHash(num_perm=256)
        file = []
        for _ in range(200):
            rand_string = ''.join(random.choice(string.ascii_lowercase) for _ in range(5))
            file.append(rand_string)
        files[iterator] = file
        minhash.update_batch([s.encode('utf-8') for s in file])
        minhashes.append(("key" + str(iterator), minhash))

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

    for _ in tqdm(range(1), desc="Insert 100 minHashes:"):
        with lsh.insertion_session(buffer_size=100) as session:
            for key, minhash in minhashes:
                session.insert(key, minhash)

    f_disc_mhs = open('minhashes.txt', 'w+')
    for minhash in tqdm(minhashes, desc="Log minHashes:"):
        log(f_disc_mhs, minhash[0], minhash[1].digest())
    f_disc_mhs.close()

    f_disc_files = open('files.txt', 'w+')
    for iterator in tqdm(range(len(files)), desc="Log files:"):
        log(f_disc_files, minhashes[iterator][0], files[iterator])
    f_disc_mhs.close()


def log(f_disc, key: str, signature: list) -> None:
    mh_info = '# ' + key + ' SIG: ' + str(signature) + '\n'
    f_disc.write(mh_info)


if __name__ == "__main__":
    main()
