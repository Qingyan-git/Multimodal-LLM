import os
import dotenv
import gdown

dotenv.load_dotenv()
dataset_url = os.getenv('dataset_path')
dataset_output = os.getenv('dataset_download_path')


try:
    gdown.download_folder(
        url = dataset_url,
        output = dataset_output,
        quiet = True,
        use_cookies = True
    )

    if os.path.exists(dataset_output) and os.listdir(dataset_output):
        print(f'Download successful. {len(os.listdir(dataset_output))} files uploaded.') 
    else:
        raise Exception('Please double check the path to the dataset dump folder')

except Exception as e:
    print(f'An error occured : {e}')
