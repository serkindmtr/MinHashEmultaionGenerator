import cassandra
import base64
import csv
from cassandra.cluster import Cluster, Session, ResultSet
from tqdm import tqdm

import config

BANDS = [
    "lsh_" + config.BASE_NAME + "_bucket_0000",
    "lsh_" + config.BASE_NAME + "_bucket_0001",
    "lsh_" + config.BASE_NAME + "_bucket_0002",
    "lsh_" + config.BASE_NAME + "_bucket_0003",
    "lsh_" + config.BASE_NAME + "_bucket_0004",
    "lsh_" + config.BASE_NAME + "_bucket_0005",
    "lsh_" + config.BASE_NAME + "_bucket_0006",
    "lsh_" + config.BASE_NAME + "_bucket_0007",
    "lsh_" + config.BASE_NAME + "_bucket_0008",
    "lsh_" + config.BASE_NAME + "_bucket_0009",
    "lsh_" + config.BASE_NAME + "_bucket_000a",
    "lsh_" + config.BASE_NAME + "_bucket_000b",
    "lsh_" + config.BASE_NAME + "_bucket_000c",
    "lsh_" + config.BASE_NAME + "_bucket_000d",
    "lsh_" + config.BASE_NAME + "_bucket_000e",
    "lsh_" + config.BASE_NAME + "_bucket_000f",
    "lsh_" + config.BASE_NAME + "_bucket_0010",
    "lsh_" + config.BASE_NAME + "_bucket_0011",
    "lsh_" + config.BASE_NAME + "_bucket_0012",
    "lsh_" + config.BASE_NAME + "_bucket_0013",
    "lsh_" + config.BASE_NAME + "_bucket_0014",
    "lsh_" + config.BASE_NAME + "_bucket_0015",
    "lsh_" + config.BASE_NAME + "_bucket_0016",
    "lsh_" + config.BASE_NAME + "_bucket_0017",
    "lsh_" + config.BASE_NAME + "_bucket_0018",
    "lsh_" + config.BASE_NAME + "_bucket_0019",
    "lsh_" + config.BASE_NAME + "_bucket_001a",
    "lsh_" + config.BASE_NAME + "_bucket_001b",
    "lsh_" + config.BASE_NAME + "_bucket_001c",
    "lsh_" + config.BASE_NAME + "_bucket_001d",
    "lsh_" + config.BASE_NAME + "_bucket_001e",
    "lsh_" + config.BASE_NAME + "_bucket_001f",
    "lsh_" + config.BASE_NAME + "_bucket_0020",
    "lsh_" + config.BASE_NAME + "_bucket_0021",
    "lsh_" + config.BASE_NAME + "_bucket_0022",
    "lsh_" + config.BASE_NAME + "_bucket_0023",
    "lsh_" + config.BASE_NAME + "_bucket_0024",
    "lsh_" + config.BASE_NAME + "_bucket_0025",
    "lsh_" + config.BASE_NAME + "_bucket_0026",
    "lsh_" + config.BASE_NAME + "_bucket_0027",
    "lsh_" + config.BASE_NAME + "_bucket_0028",
    "lsh_" + config.BASE_NAME + "_bucket_0029"
]


def main() -> None:
    cluster = Cluster()
    session = cluster.connect(config.KEY_SPACE)
    # Number of bands 42
    for band_name in tqdm(BANDS, desc="Generating bands:"):
        print("-------- BAND: " + band_name + " --------")
        result = get_values_from_band(session, band_name)
        create_csv_band(band_name, result.current_rows)


def get_values_from_band(session: Session, band: str) -> ResultSet:
    return session.execute("select * from " + config.KEY_SPACE + "." + band)


def create_csv_band(band_name: str, rows: tuple) -> None:
    path = config.KEY_SPACE + "." + band_name + ".csv"
    data = ["key,value,ts".split(",")]
    band_f_disc = open(path, "w+", newline='')
    writer = csv.writer(band_f_disc, delimiter=',')
    writer.writerows(data)
    del data
    # Count of rows see config.COUNT_UNQ_MHS
    count_row = 1
    for row in tqdm(rows, desc="Copy minhashing:"):
        print("-------- MINHASH #" + str(count_row) + " --------")
        variable = row.value.decode()
        key = base64.b64encode(row.key)
        data = []
        # Count of iteration 100 thousands
        for i in range(0, 100000):
            value_string = variable + "_" + str(i).zfill(5)
            value = base64.b64encode(value_string.encode())
            data.append([key.decode(), value.decode(), row.ts])

            # Writing to csv batch size of 10000
            if (i + 1) % 10000 == 0:
                writer.writerows(data)
                data = []
        count_row += 1
    band_f_disc.close()


if __name__ == "__main__":
    main()
