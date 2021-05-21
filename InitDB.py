# Эксперимент:
# Заполняем 100k minHash-ей в Cassandra
# Смотрим время заполнения и время поиска
import random
import string
from tqdm import tqdm
from datasketch import MinHash, MinHashLSH


def main():
    # Генерация list-а минхешей, где ["Ключ МинХэша", ОбъектMinHash]
    minhashes = []
    files = []
    for iterator in tqdm(range(100), desc="Generate minHashes:"):
        minhash = MinHash(num_perm=256)
        file = []
        for _ in range(200):
            rand_string = ''.join(random.choice(string.ascii_lowercase) for _ in range(5))
            file.append(rand_string)
        minhash.update_batch([s.encode('utf-8') for s in file])
        minhashes.append(("key" + str(iterator), minhash))
    # Подключаем бд Кассандру
    lsh = MinHashLSH(
        threshold=0.5, num_perm=256, storage_config={
            'type': 'cassandra',
            'basename': b'perftest',
            'cassandra': {
                'seeds': ['127.0.0.1'],
                'keyspace': 'lsh_test_100_mln',
                'replication': {
                    'class': 'SimpleStrategy',
                    'replication_factor': '1',
                },
                'drop_keyspace': False,
                'drop_tables': False,
            }
        }
    )
    # Вставляем 100k минхэшей в базу
    for _ in tqdm(range(1), desc="Insert 100 minHashes:"):
        with lsh.insertion_session(buffer_size=100) as session:
            for key, minhash in minhashes:
                session.insert(key, minhash)

    # Логируем созданные минхеши
    f_disc_mhs = open('log/minhashes.txt', 'w+')
    for minhash in tqdm(minhashes, desc="Log minHashes:"):
        log(f_disc_mhs, minhash[0], minhash[1].digest())
    f_disc_mhs.close()

    # Логируем созданные файлы
    f_disc_files = open('log/files.txt', 'w+')
    for iterator in tqdm(range(len(files)), desc="Log files:"):
        log(f_disc_files, minhashes[iterator][0], files[iterator])
    f_disc_mhs.close()


def log(f_disc, key: str, signature: tuple):
    mh_info = '# ' + key + ' SIG: ' + str(signature)
    f_disc.write(mh_info)


if __name__ == "__main__":
    main()