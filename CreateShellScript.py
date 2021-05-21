import CreateCSVs
import config


def main():
    f_disc = open('dsbulk_script', 'w+')
    f_disc.writelines(['#!/bin/bash'])
    data = []
    for band_name in CreateCSVs.BANDS:
        line = "dsbulk load -url " + config.KEY_SPACE + "." + band_name + ".csv -k " + config.KEY_SPACE + " -t " + band_name
        data.append(line)
    f_disc.writelines(data)
    f_disc.close()


if __name__ == "__main__":
    main()
