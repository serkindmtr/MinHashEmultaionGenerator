import cassandra
import base64
import csv
from cassandra.cluster import Cluster, Session, ResultSet
import logging

logging.getLogger("cassandra").setLevel(logging.ERROR)

BANDS = [
    "lsh_test_100_mln.lsh_howto_bucket_0001",
    "lsh_test_100_mln.lsh_howto_bucket_0000",
    "lsh_test_100_mln.lsh_howto_bucket_0002",
    "lsh_test_100_mln.lsh_howto_bucket_0003",
    "lsh_test_100_mln.lsh_howto_bucket_0004",
    "lsh_test_100_mln.lsh_howto_bucket_0005",
    "lsh_test_100_mln.lsh_howto_bucket_0006",
    "lsh_test_100_mln.lsh_howto_bucket_0007",
    "lsh_test_100_mln.lsh_howto_bucket_0008",
    "lsh_test_100_mln.lsh_howto_bucket_0009",
    "lsh_test_100_mln.lsh_howto_bucket_000a",
    "lsh_test_100_mln.lsh_howto_bucket_000b",
    "lsh_test_100_mln.lsh_howto_bucket_000c",
    "lsh_test_100_mln.lsh_howto_bucket_000d",
    "lsh_test_100_mln.lsh_howto_bucket_000e",
    "lsh_test_100_mln.lsh_howto_bucket_000f",
    "lsh_test_100_mln.lsh_howto_bucket_0010",
    "lsh_test_100_mln.lsh_howto_bucket_0011",
    "lsh_test_100_mln.lsh_howto_bucket_0012",
    "lsh_test_100_mln.lsh_howto_bucket_0013",
    "lsh_test_100_mln.lsh_howto_bucket_0014",
    "lsh_test_100_mln.lsh_howto_bucket_0015",
    "lsh_test_100_mln.lsh_howto_bucket_0016",
    "lsh_test_100_mln.lsh_howto_bucket_0017",
    "lsh_test_100_mln.lsh_howto_bucket_0018",
    "lsh_test_100_mln.lsh_howto_bucket_0019",
    "lsh_test_100_mln.lsh_howto_bucket_001a",
    "lsh_test_100_mln.lsh_howto_bucket_001b",
    "lsh_test_100_mln.lsh_howto_bucket_001c",
    "lsh_test_100_mln.lsh_howto_bucket_001d",
    "lsh_test_100_mln.lsh_howto_bucket_001e",
    "lsh_test_100_mln.lsh_howto_bucket_001f",
    "lsh_test_100_mln.lsh_howto_bucket_0020",
    "lsh_test_100_mln.lsh_howto_bucket_0021",
    "lsh_test_100_mln.lsh_howto_bucket_0022",
    "lsh_test_100_mln.lsh_howto_bucket_0023",
    "lsh_test_100_mln.lsh_howto_bucket_0024",
    "lsh_test_100_mln.lsh_howto_bucket_0025",
    "lsh_test_100_mln.lsh_howto_bucket_0026",
    "lsh_test_100_mln.lsh_howto_bucket_0027",
    "lsh_test_100_mln.lsh_howto_bucket_0028",
    "lsh_test_100_mln.lsh_howto_bucket_0029"]


def main():
    cluster = Cluster()
    session = cluster.connect('lsh_test_100_mln')
    for band_name in BANDS:
        result = get_values_from_band(session, band_name)
        create_csv_band(band_name, result.current_rows)
    #
    # result = session.execute("select * from lsh_test_100_mln.lsh_howto_bucket_0000")
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
    for row in rows:
        for i in range(0, 100000):
            variable = row.value.decode() + "_" + str(i).zfill(5)
            key = base64.b64encode(row.key)
            value = base64.b64encode(variable.encode())
            data.append([key.decode(), value.decode(), row.ts])
    csv_writer(data, path)


def csv_writer(data, path):
    """
    Write data to a CSV file path
    """
    with open(path, "w+", newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerows(data)


if __name__ == "__main__":
    main()
