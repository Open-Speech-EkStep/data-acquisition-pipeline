import pandas as pd
import re
import glob
import os
import wget

def extract_metadata(file):
    '''
    Reads scraped text files for each page and extracts the following fields:
        - file title
        - file type (audio/video etc)
        - downlaodable file link
    '''
    with open(file) as f:
        list_of_contents = f.readlines()
        
    if len(list_of_contents) != 3:
        print("{} //file has {} fields instead of 3".format(file, len(list_of_contents)))
 
    else:
        # each of the 3 elements present in "list of contents" represents one of the three fields
        titles = list_of_contents[0][:-1]
        file_types = list_of_contents[1][:-1]
        incomplete_links = list_of_contents[2]
        
        titles_list = titles.split(',')
        file_types_list = file_types.split(',')
        incomplete_links_list = incomplete_links.split(',')
        
        website_prefix = 'https://nroer.gov.in'
        complete_links_list = [website_prefix + i for i in incomplete_links_list]
        
        if len(titles_list) == len(file_types_list) == len(complete_links_list):
            print("{} == {} == {}".format(len(titles_list), len(file_types_list), len(complete_links_list)))
            metadata = dict({
                'title': titles_list,
                'file_type': file_types_list,
                'download_link': complete_links_list
            })
            df = pd.DataFrame(metadata, columns=['title', 'file_type', 'download_link'])
            # print("{} has shape {}".format(file, df.shape))
            return df
        else:
            print("file: {} does not have all values for 3 columns".format(file))



if __name__ == "__main__":
    path_to_scraped_audios = '../data/scraped/audios_sr_secondary/'
    path_to_save_final_csv = '../data/processed/nroer_audios_sr_secondary.csv'
    
    scraped_files = glob.glob(path_to_scraped_audios+'*.txt')
    
    print("Number of files in directory {} are = {}".format(path_to_scraped_audios, len(scraped_files)))
    print("---------- EXTRACTING METADATA ----------")
    
    dfs = []
    for file in scraped_files:
        try:
            dfs.append(extract_metadata(file))
        except:
            print("Error in file: {}".format(file))
            continue
    print("No. of files with usable data = {}".format(len(dfs)))
    final_df = pd.concat(dfs)
    final_df.reset_index(drop=True, inplace=True)
    print("Final file shape:", final_df.shape)
    print("---------- SAVING METADATA FILE: {}----------".format(path_to_save_final_csv))
    final_df.to_csv(path_to_save_final_csv, index=False)






    