import sys
import pandas as pd

def merge_segments_create_audience(all_segments_csv, filename):
    """ Concatenate given dataframes to create one audience and write to file
    """

    all_dataframes = [pd.read_csv(x, encoding='utf_8_sig') for x in all_segments_csv]
    bigdataframe = pd.concat(all_dataframes)
    bigdataframe.to_csv(filename, index=False)

    return

input_file = sys.argv[1]
write_to_csv = sys.argv[2]
segments_to_be_merged = [x.strip() for x in open(input_file).readlines()]
print(segments_to_be_merged)
merge_segments_create_audience(segments_to_be_merged, write_to_csv)
