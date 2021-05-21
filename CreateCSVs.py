import cassandra
import base64
import csv
from cassandra.cluster import Cluster, Session, ResultSet
from tqdm import tqdm

import config

BANDS = [
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0000",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0001",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0002",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0003",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0004",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0005",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0006",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0007",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0008",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0009",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_000a",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_000b",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_000c",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_000d",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_000e",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_000f",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0010",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0011",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0012",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0013",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0014",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0015",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0016",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0017",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0018",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0019",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_001a",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_001b",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_001c",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_001d",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_001e",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_001f",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0020",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0021",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0022",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0023",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0024",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0025",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0026",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0027",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0028",
    config.KEY_SPACE + ".lsh_" + config.BASE_NAME + "_bucket_0029"]


def main():
    cluster = Cluster()
    session = cluster.connect(config.KEY_SPACE)
    # Number of bands 42
    for band_name in tqdm(BANDS, desc="Generating bands:"):
        result = get_values_from_band(session, band_name)
        create_csv_band(band_name, result.current_rows)
    #
    # result = session.execute("select * fromconfig.KEY_SPACE +  .lsh_" + BASE_NAME + "_bucket_0000")
    # # result.current_rows.0
    # data = ["first_name,last_name,city".split(","),
    #         "Tyrese,Hirthe,Strackeport".split(","),
    #         "Jules,Dicki,Lake Nickolasville".split(","),
    #         "Dedric,Medhurst,Stiedemannberg".split(",")
    #         ]
    # path = "output.csv"
    # # csv_writer(["key,value,ts".split(",")], path)
    # data = ["key,value,ts".split(",")]
    # for row in result.current_rows:
    #     for i in range(0, 100):
    #         variable = str(i) + "_" + row.value.decode()
    #         key = base64.b64encode(row.key)
    #         value = base64.b64encode(variable.encode())
    #         data.append([key.decode(), value.decode(), row.ts])
    # csv_writer(data, path)


def get_values_from_band(session: Session, band: str) -> ResultSet:
    return session.execute("select * from " + band)


def create_csv_band(band_name: str, rows: tuple):
    path = band_name + ".csv"
    data = ["key,value,ts".split(",")]
    band_f_disc = open(path, "w+", newline='')
    writer = csv.writer(band_f_disc, delimiter=',')
    writer.writerows(data)
    del data
    # Count of rows see config.COUNT_UNQ_MHS
    for row in tqdm(rows, desc="Copy minhashing:"):
        variable = row.value.decode()
        key = base64.b64encode(row.key)
        data = []
        # Count of iteration 100 millions
        for i in tqdm(range(0, 100), desc="Copy minhash:"):
            value_string = variable + "_" + str(i).zfill(5)
            value = base64.b64encode(value_string.encode())
            data.append([key.decode(), value.decode(), row.ts])

            if (i + 1) % 10 == 0:
                writer.writerows(data)
                data = []
    band_f_disc.close()


# def init_csv(band_name: str) -> None:
#     path = band_name + ".csv"
#     data = ["key,value,ts".split(",")]
#     csv_writer(data, path)
#
#
# def csv_writer(data, path) -> None:
#     """
#     Write data to a CSV file path
#     """
#     with open(path, "w+", newline='') as csv_file:
#         writer = csv.writer(csv_file, delimiter=',')
#         writer.writerows(data)


if __name__ == "__main__":
    main()
