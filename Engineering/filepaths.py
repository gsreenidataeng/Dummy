# import the necessary module
import json
import logging

# setting up logging configuration
logging.basicConfig(level=logging.ERROR)

path_info_file = '../filepath.json'


# read json file to get the input and output path
def get_input_file_paths(path_info):
    try:
        with open(path_info, 'r') as json_file:
            data = json.load(json_file)
        if 'input_file_path' not in data or 'output_file_path' not in data:
            raise KeyError('we found keys missing from the file - Data')
        return data['input_file_path'], data['output_file_path']

    except FileNotFoundError:
        logging.error(f'File not found error {path_info}')
    except json.JSONDecodeError:
        logging.error(f'failed to parse from json {path_info}')
    except KeyError as b:
        logging.error('Key error is: ', str(b))
    except Exception as e:
        print('Issue with file is:', str(e))
    return None, None


input_path, output_path = get_input_file_paths(path_info_file)
