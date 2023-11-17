
import os, io, json, copy
from azure.core.exceptions import AzureError
from azure.storage.blob import BlobClient
from urllib.parse import urlparse

def blob_name_and_url_dict(blob_service_client, container_name) -> dict:
    """
    This function takes in a blob service client and a container name and returns
    a dictionary containing the blob details indexed by the container name, file type, and then file name or url.
    :param blob_service_client: BlobServiceClient object
    :param container_name: Name of the container to be accessed
    :return: Dictionary containing the blob details indexed by the container name, file type, and then file name or url
    """
    # Create an empty dictionary to store the blob details
    blob_name_and_url_dict = {}

    try:
        # Create the BlobContainerClien object so we can connect to the blob storage container based on the container name
        container_client = blob_service_client.get_container_client(container_name)
        print(f"The container {container_name} is being accessed.")
    except Exception as e:
        print(f"ERROR: Could not access the container {container_name}.")
        print(e)
    
    try:
        # List all blobs in the container
        blob_list = container_client.list_blobs()

        # If the container name is not in the dictionary, add it
        if container_name not in blob_name_and_url_dict:
            blob_name_and_url_dict[container_name] = {}

        # Loop through each blob in the container
        for blob in blob_list:
            blob_client = container_client.get_blob_client(blob)
            print(f"The file {blob.name} located at {blob_client.url} is being added to the blob list.")
            
            # Get the file extension
            _, file_extension = os.path.splitext(blob.name)

            # Now you can use file_extension in your dictionary
            blob_file_dict = {
                'file_name': blob.name,
                'blob_url': blob_client.url
            }

            # If the file type is not in the dictionary, add it
            if file_extension not in blob_name_and_url_dict[container_name]:
                blob_name_and_url_dict[container_name][file_extension] = {}

            # Add the dictionary to the dictionary associated with the file type
            blob_name_and_url_dict[container_name][file_extension][blob.name] = blob_file_dict

        return blob_name_and_url_dict
    except Exception as e:
        print(f"ERROR: Could not list the blobs in the container {container_name}.")
        print(e)


def doc_intel_pdf(document_analysis_client, doc_intel_model, file_name, blob_url, poller_result_or_dict_flag) -> dict:
    '''
    Function to analyze a PDF document using the Document Intelligence service based
    on the url of the blob containing the document - this is the raw document.
    The url must contain a SAS token with read permissions.

    :param document_analysis_client: DocumentAnalysisClient object
    :param doc_intel_model: Document Intelligence model to use
    :param file_name: Name of the file to be analyzed
    :param blob_url: URL of the blob to be analyzed
    :param poller_result_or_dict_flag: Flag to return the poller result ('result')  or the dictionary ('dict) or both ('both)
    :return: Document Intelligence results as the result object, a dictionary, or both
    '''
    try:        
        # Extract the blob name from the blob URL
        blob_name = urlparse(blob_url).path.split('/')[-1]
        
        _, file_extension = os.path.splitext(blob_name)    
        if file_extension == '.pdf':
            # Analyze the document using the Document Intelligence service
            doc_intel_pdf_poller = document_analysis_client.begin_analyze_document_from_url(
                                            doc_intel_model, 
                                            document_url=blob_url
                                            )
            doc_intel_pdf_result = doc_intel_pdf_poller.result()
            doc_intel_pdf_result_dict = doc_intel_pdf_result.to_dict()

            print(f'File {file_name} was analyzed using the Document Intelligence service.')
            
            # Return the result based on the flag poller_result_or_dict_flag
            if poller_result_or_dict_flag == 'result':
                return doc_intel_pdf_result
            if poller_result_or_dict_flag == 'dict':
                return doc_intel_pdf_result_dict
            if poller_result_or_dict_flag == 'both':
                return doc_intel_pdf_result, doc_intel_pdf_result_dict

        else:
            print(f'File {file_name} is not a PDF according to the file extension.')
            return None

    except Exception as e:
        print(f'Error: {e}')
        return None


def local_file_write(data, text_or_json_flag=str, file_path=str, file_name_with_extension=str) -> None:
    '''
    :param raw_result: the raw result from the document intelligence service
    :param result_dict: the dictionary of results from the document intelligence service
    :param text_or_json_flag: a flag to determine if the file should be written as a text file or a json file
    :return: None
    '''
    try:
        if text_or_json_flag == 'text':
            with open(file_path + '/' + file_name_with_extension, 'w', encoding='utf-8') as f:
                f.write(str(data))
                print('File successfully written to ' + file_path + '/' + file_name_with_extension)
        elif text_or_json_flag == 'json':
            with open(file_path + '/' + file_name_with_extension, 'w') as f:
                json.dump(data, f)
                print('File successfully written to ' + file_path + '/' + file_name_with_extension)
        else:
            print('''Please specify if you want to write a text or json file by setting the text_or_json_flag parameter to 'json' or 'text'.''')

    except Exception as e:
        print(e)
    
    return None


def local_file_read(file_path: str, text_or_json_flag: str):
    '''
    :param file_path: the path to the file to read
    :param text_or_json_flag: a flag to determine if the file is a text file or a json file
    :return: the contents of the file as either a dictionary or raw text
    '''
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            if text_or_json_flag == 'text':
                return f.read()
            elif text_or_json_flag == 'json':
                return json.load(f)
            else:
                print('''Please specify if you want to read a text or json file by setting the text_or_json_flag parameter to 'json' or 'text'.''')
                return None
    except Exception as e:
        print(e)
        return None


def write_to_blob(object_to_upload, blob_service_client, container_name=str, virtual_directory_name=str, file_name=str, json_dump_flag=bool):
    '''
    This function will write a file to a blob storage container; uses json.dumps to convert to json if json_dump_flag is True.
    :param object_to_upload: The object to upload to the blob storage container
    :param blob_service_client: The blob service client object; raw_blob_service_client, processed_blob_service_client, or final_blob_service_client in this exercise
    :param container_name: The name of the container to upload to; raw_container_name, processed_container_name, or final_container_name in this exercise
    :param virtual_directory_name: The name of the virtual directory to upload to; raw_results or dictionaries in this exercise
    :param file_name: The name of the file to upload, string format
    :param json_dump_flag: A boolean flag to indicate if the object should be converted to json before uploading
    '''
    blob_name = str(container_name + '/' + virtual_directory_name + '/' + file_name)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    print(blob_client) # Only needed for debugging
    if json_dump_flag == True:
        try:
            json_object_to_upload = json.dumps(object_to_upload)
            blob_client.upload_blob(json_object_to_upload, blob_type="BlockBlob")
            print(f'Uploaded {file_name} to {blob_name}')
        except Exception as e:
            print(f'Error uploading {file_name} to {blob_name} with the following Error: {e}')
    
    else:
        try:
            str_object_to_upload = str(object_to_upload)
            blob_client.upload_blob(str_object_to_upload, blob_type="BlockBlob")
            print(f'Uploaded {file_name} to {blob_name}')
        except Exception as e:
            print(f'Error uploading {file_name} to {blob_name} with the following Error: {e}')


def load_blobs_iterator(blob_dict, file_type=None, file_name=None):
    """
    Generator function to load blobs from Azure Blob Storage.

    Parameters:
    blob_dict (dict): A dictionary with blob names as keys and blob URLs as values.
    file_type (str, optional): The file type to filter blobs by. Only blobs with this file type will be loaded. Defaults to None.
    file_name (str, optional): The file name to filter blobs by. Only the blob with this file name will be loaded. Defaults to None.

    Yields:
    str: The content of a blob.

    Example usage:
    dictionary_of_processed_blobs = fn.blob_name_and_url_dict()

    for blob_content in load_blobs(dictionary_of_processed_blobs):
        # Now blob_content contains the actual content of a blob
        # You can process it here
        print(blob_content)
    """
    
    # Iterate over all blobs in the dictionary
    for blob_name, blob_url in blob_dict.items():
        # If a file type is specified, skip blobs that don't match the file type
        if file_type and not blob_name.endswith(file_type):
            continue
        # If a file name is specified, skip blobs that don't match the file name
        if file_name and blob_name != file_name:
            continue
        
        try:
            # Create a blob client for the current blob
            blob_client = BlobClient.from_blob_url(blob_url)
            # Download the blob's content
            blob_content = blob_client.download_blob().readall()
            
            # Yield the blob's content
            yield blob_content
        except AzureError as e:
            print(f"Failed to download blob: {blob_name}. Error: {e}")


def load_blob(blob_dict, container_name=None, file_type=None, file_name=None):
    """
    Load a single blob from a nested dictionary of blob names and URLs.

    Parameters:
    blob_dict (dict): A nested dictionary where keys are container names, file types, and blob names, and values are blob details.
    file_type (str, optional): The file type to filter blobs by. Blobs not of this file type will be ignored.
    file_name (str, optional): A specific blob name to load. If provided, only this blob will be loaded.
    container_name (str, optional): A specific container name to load blobs from. If provided, only blobs from this container will be loaded.

    Returns:
    bytes: The content of the blob.

    Example usage:
    blob_dict = {"container1": {".txt": {"blob1": {"file_name": "blob1", "blob_url": "url1"}}}}
    load_blob(blob_dict, file_type=".txt", file_name="blob1", container_name="container1")

    .txt
    blob_content_string = blob_content.decode('utf-8')

    .json
    
    """

    # If container_name is provided and it does not exist in the dictionary, return None
    if container_name and container_name not in blob_dict:
        print(f'container_name:{container_name} not found in blob_dict')
        return None
    # If container_name is provided, iterate over just this container, otherwise over all containers in the dictionary
    for container_name, container_dict in ([(container_name, blob_dict[container_name])] if container_name else blob_dict.items()):
        # If file_type is provided and it does not exist in the current container's dictionary, skip this container
        if file_type and file_type not in container_dict:
            print(f'file_type:{file_type} not found in container_dict')
            continue
        # If file_type is provided, iterate over all blobs in the file_type dictionary, otherwise in the container's dictionary
        for blob_name, blob_details in (container_dict[file_type] if file_type else container_dict).items():
            # If file_name is provided and blob_name is not equal to it, skip this blob
            if file_name and blob_name != file_name:
                print(f'file_name:{file_name} not found in blob_dict {blob_dict}')
                continue

            try:
                # Create a BlobClient for the blob
                print(f'blob_details:{blob_details}')
                blob_client = BlobClient.from_blob_url(blob_details['blob_url'])
                # Download the blob and read all its content
                blob_content = blob_client.download_blob().readall()

                if file_type == '.json':
                    # Convert the bytes to a file-like object
                    blob_file = io.BytesIO(blob_content)
                    # Load the JSON document from the file
                    blob_content_obj = json.load(blob_file)
                    # Return the Python object
                    return blob_content_obj

                if file_type == '.txt':
                    # Convert the bytes to a string
                    blob_content_string = blob_content.decode('utf-8')
                    # Return the string
                    return blob_content_string

                # Return the blob content
                return blob_content
            except AzureError as e:
                print(f"Failed to download blob: {e}")
                return None

    # If no blob was found that matches the criteria, return None
    return None


def print_keys(data, parent_key='', printed_keys=set()):
    """
    Recursively prints all keys in a nested dictionary or list.

    Args:
        data (dict or list): The data to print keys from.
        parent_key (str, optional): The parent key to prepend to each key. Defaults to ''.
        printed_keys (set, optional): The set of already printed keys. Defaults to set().

    Returns:
        None
    """
    # if data is a dictionary
    if isinstance(data, dict):
        # iterate over each key-value pair in the dictionary
        for k, v in data.items():
            # create the current key by appending the parent key and the current key
            current_key = f"{parent_key}.{k}" if parent_key else k
            # if the current key has not been printed before, print it and add it to the printed keys set
            if current_key not in printed_keys:
                print(current_key)
                printed_keys.add(current_key)
            # recursively call the function with the value of the current key and the current key as the parent key
            print_keys(v, current_key, printed_keys)
    # if data is a list
    elif isinstance(data, list):
        # iterate over each element in the list
        for v in data:
            # recursively call the function with the current element and the parent key
            print_keys(v, parent_key, printed_keys)


def select_keys(original_dict, keys_to_select, current_path=''):
    """
    This function selects specific keys from a dictionary, including nested dictionaries and lists of dictionaries.
    It also keeps track of the path of nested keys in the format 'key1.key2.key3'.

    Parameters:
    original_dict (dict): The original dictionary from which keys are to be selected.
    keys_to_select (list): A list of keys that need to be selected from the original dictionary.
    current_path (str): The current path of nested keys. Default is an empty string.

    Returns:
    new_dict (dict): A new dictionary containing only the selected keys from the original dictionary.
    """

    # Initialize a new dictionary to store the selected keys
    new_dict = {}

    # Iterate over each key-value pair in the original dictionary
    for key, value in original_dict.items():

        # Construct the new key path
        new_key_path = current_path + '.' + key if current_path else key

        # If the key or the new key path is in the list of keys to select, add it to the new dictionary
        if key in keys_to_select or new_key_path in keys_to_select:
            new_dict[key] = value

        # If the value is a dictionary, recursively call the function for nested keys
        if isinstance(value, dict):
            new_dict[key] = select_keys(value, keys_to_select, new_key_path)

        # If the value is a list, iterate over each item in the list
        elif isinstance(value, list):
            new_list = []

            # If the item is a dictionary, recursively call the function for nested keys
            for item in value:
                if isinstance(item, dict):
                    new_list.append(select_keys(item, keys_to_select, new_key_path))

            # If the new list is not empty, add it to the new dictionary
            if new_list:
                new_dict[key] = new_list

    # Return the new dictionary with the selected keys
    return new_dict


def process_data(data, keys_to_remove):
    """
    This function processes a data structure (dictionary or list) and removes specified keys.
    It also moves 'page_number' from 'bounding_regions' to the parent level for 'tables', 'paragraphs', and 'cells'.

    Parameters:
    data (dict or list): The original data structure from which keys are to be removed.
    keys_to_remove (list): A list of keys that need to be removed from the data structure.

    Returns:
    data (dict or list): The processed data structure with the specified keys removed.
    """

    # If the data is a dictionary, iterate over each key-value pair
    if isinstance(data, dict):
        for key, value in list(data.items()):

            # If the key is in the list of keys to remove, delete it from the dictionary
            if key in keys_to_remove:
                del data[key]
                continue

            # If the key is 'tables', 'paragraphs', or 'cells', process the 'bounding_regions'
            if key in ['tables', 'paragraphs', 'cells']:
                for item in value:
                    if 'bounding_regions' in item:
                        for region in item['bounding_regions']:
                            if 'page_number' in region:
                                # Move 'page_number' from 'bounding_regions' to the parent level
                                item['page_number'] = region['page_number']
                        # Delete 'bounding_regions' from the item
                        del item['bounding_regions']

            # Recursively call the function for nested data structures
            data[key] = process_data(value, keys_to_remove)

    # If the data is a list, iterate over each item in the list
    elif isinstance(data, list):
        for i in range(len(data)):
            # Recursively call the function for each item in the list
            data[i] = process_data(data[i], keys_to_remove)

    # Return the processed data structure
    return data

